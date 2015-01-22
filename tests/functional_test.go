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

				if offset_in_buffer != iopkt.Offset {
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

func TestSimpleCache(t *testing.T) {
	logsize := uint64(100 * cache.MB)
	blocksize := uint64(4 * cache.KB)
	blocks := uint64(logsize / blocksize)
	blocks_per_segment := uint64(32)
	bcsize := uint64(1 * cache.MB)
	logfile := Tempfile()

	log, actual_blocks := cache.NewLog(logfile,
		blocks,
		blocksize,
		blocks_per_segment,
		bcsize)
	c := cache.NewCache(actual_blocks, blocksize, log.Msgchan)
	defer os.Remove(logfile)

	var wgIo, wgRet sync.WaitGroup

	// Start up response server
	returnch := make(chan *message.Message, 100)
	wgRet.Add(1)
	go response_handler(t, &wgRet, returnch)

	// Create 100 clients
	for i := 0; i < 100; i++ {
		wgIo.Add(1)
		go func() {
			defer wgIo.Done()
			z := zipf.NewZipfWorkload(actual_blocks*10, 60)
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
						Offset:  offset,
						Nblocks: 1,
					}
					c.Invalidate(iopkt)

					// Simulate waiting for storage device to write data
					// anywhere from 100usecs to 10ms
					time.Sleep(time.Microsecond * time.Duration((r.Intn(10000) + 100)))

					// Now, we can do a put
					msg = message.NewMsgPut()
				}

				iopkt := msg.IoPkt()
				iopkt.Buffer = make([]byte, blocksize)
				iopkt.Offset = offset
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
						returnch <- msg
					}
				}

				// Maximum "disk" size is 10 times bigger than cache

				// Send request

				// Simulate waiting for more work by sleeping
				// anywhere from 100usecs to 10ms
				time.Sleep(time.Microsecond * time.Duration((r.Intn(10000) + 100)))
			}
		}()

	}

	// Wait for all clients to finish
	wgIo.Wait()

	// Print stats
	fmt.Print(c)
	fmt.Print(log)

	// Close cache and log
	c.Close()
	log.Close()

	// Send receiver a message that all clients have shut down
	close(returnch)

	// Wait for receiver to finish emptying its channel
	wgRet.Wait()

}
