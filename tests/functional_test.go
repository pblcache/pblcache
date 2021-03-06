//
// Copyright (c) 2014 The pblcache Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
package tests

import (
	"fmt"
	"github.com/lpabon/bufferio"
	"github.com/lpabon/goioworkload/zipf"
	"github.com/lpabon/tm"
	"github.com/pblcache/pblcache/cache"
	"github.com/pblcache/pblcache/message"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func response_handler(t *testing.T,
	wg *sync.WaitGroup,
	m chan *message.Message) {

	var (
		gethits, getmisses, puts int
		errors                   int
		tgh, tgm, tp             tm.TimeDuration
	)

	defer wg.Done()

	// Check incoming channels
	for msg := range m {
		// Collect stats
		switch msg.Type {
		case message.MsgGet:
			if msg.Err == nil {
				tgh.Add(msg.TimeElapsed())
				gethits++

				// Check correctness.  The value
				// of the offset should have been
				// saved in the buffer
				iopkt := msg.IoPkt()
				bio := bufferio.NewBufferIO(iopkt.Buffer)
				var offset_in_buffer uint64
				bio.ReadDataLE(&offset_in_buffer)

				if offset_in_buffer != iopkt.Address {
					errors++
				}

			} else {
				tgm.Add(msg.TimeElapsed())
				getmisses++
			}
		case message.MsgPut:
			puts++
			tp.Add(msg.TimeElapsed())
		}
	}

	fmt.Printf("ERRORS: %d\nGet H:%d M:%d, Puts:%d\n"+
		"Get Hit Rate: %.2f\n"+
		"Mean times in usecs:\n"+
		"Get H:%.2f M:%.2f, Puts:%.2f\n",
		errors, gethits, getmisses, puts,
		float64(gethits)/float64(gethits+getmisses),
		tgh.MeanTimeUsecs(), tgm.MeanTimeUsecs(),
		tp.MeanTimeUsecs())
	Assert(t, errors == 0)
}

func cacheio(t *testing.T, c *cache.CacheMap, log *cache.Log,
	actual_blocks, blocksize uint32) {
	var wgIo, wgRet sync.WaitGroup

	// Start up response server
	returnch := make(chan *message.Message, 100)
	wgRet.Add(1)
	go response_handler(t, &wgRet, returnch)

	// Create a parent message for all messages to notify
	// when they have been completed.
	messages := &message.Message{}
	messages_done := make(chan *message.Message)
	messages.RetChan = messages_done

	// Create 100 clients
	for i := 0; i < 100; i++ {
		wgIo.Add(1)
		go func() {
			defer wgIo.Done()
			z := zipf.NewZipfWorkload(uint64(actual_blocks)*10, 60)
			r := rand.New(rand.NewSource(time.Now().UnixNano()))

			// Each client to send 5k IOs
			for io := 0; io < 5000; io++ {
				var msg *message.Message
				offset, isread := z.ZipfGenerate()

				if isread {
					msg = message.NewMsgGet()
				} else {
					// On a write the client would first
					// invalidate the block, write the data to the
					// storage device, then place it in the cache
					iopkt := &message.IoPkt{
						Address: offset,
						Blocks:  1,
					}
					c.Invalidate(iopkt)

					// Simulate waiting for storage device to write data
					time.Sleep(time.Microsecond * time.Duration((r.Intn(100))))

					// Now, we can do a put
					msg = message.NewMsgPut()
				}

				messages.Add(msg)
				iopkt := msg.IoPkt()
				iopkt.Buffer = make([]byte, blocksize)
				iopkt.Address = offset
				msg.RetChan = returnch

				msg.TimeStart()

				// Write the offset into the buffer so that we can
				// check it on reads.
				if !isread {
					bio := bufferio.NewBufferIO(iopkt.Buffer)
					bio.WriteDataLE(offset)
					c.Put(msg)
				} else {
					_, err := c.Get(msg)
					if err != nil {
						msg.Err = err
						msg.Done()
					}
				}

				// Maximum "disk" size is 10 times bigger than cache

				// Send request

				// Simulate waiting for more work by sleeping
				time.Sleep(time.Microsecond * time.Duration((r.Intn(100))))
			}
		}()

	}

	// Wait for all clients to finish
	wgIo.Wait()

	// Wait for all messages to finish
	messages.Done()
	<-messages_done

	// Print stats
	fmt.Print(c)
	fmt.Print(log)

	// Close cache and log
	c.Close()
	log.Close()

	stats := log.Stats()
	Assert(t, stats.Seg_skipped == 0)

	// Send receiver a message that all clients have shut down
	close(returnch)

	// Wait for receiver to finish emptying its channel
	wgRet.Wait()

}

func TestSimpleCache(t *testing.T) {
	logsize := uint32(100 * cache.MB)
	blocksize := uint32(4 * cache.KB)
	blocks := uint32(logsize / blocksize)
	blocks_per_segment := uint32(32)
	bcsize := uint32(1 * cache.MB)

	logfile := Tempfile()
	err := CreateFile(logfile, int64(blocks*blocksize))
	Assert(t, err == nil)

	log, actual_blocks, err := cache.NewLog(logfile,
		blocksize,
		blocks_per_segment,
		bcsize,
		false)
	Assert(t, err == nil)
	c := cache.NewCacheMap(actual_blocks, blocksize, log.Msgchan)
	defer os.Remove(logfile)
	log.Start()

	// Start test
	fmt.Println("----> Initial test")
	cacheio(t, c, log, actual_blocks, blocksize)

	// Save cache metadata
	fmt.Println("----> Saving metadata")
	save := Tempfile()
	defer os.Remove(save)
	err = c.Save(save, log)
	Assert(t, err == nil)

	// Run it again but now load the cache.
	log, actual_blocks, err = cache.NewLog(logfile,
		blocksize,
		blocks_per_segment,
		bcsize,
		false)
	Assert(t, err == nil)
	c = cache.NewCacheMap(actual_blocks, blocksize, log.Msgchan)
	c.Load(save, log)
	log.Start()

	// Start test
	fmt.Println("----> Second pass")
	cacheio(t, c, log, actual_blocks, blocksize)
}
