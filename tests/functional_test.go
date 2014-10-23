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
	"github.com/lpabon/tm"
	zipf "github.com/lpabon/zipfworkload"
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
	quit chan struct{},
	m chan *message.Message) {

	var (
		gethits, getmisses, puts         int
		invalidatehits, invalidatemisses int
		errors                           int
		tgh, tgm, tp, tih, tim           tm.TimeDuration
	)

	defer wg.Done()

	emptychan := false
	for {

		// Check if we have been signaled through <-quit
		// If we have, we now know that as soon as the
		// message channel is empty, we can quit.
		if emptychan {
			if len(m) == 0 {
				break
			}
		}

		// Check incoming channels
		select {
		case msg := <-m:
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
			case message.MsgInvalidate:
				if msg.Err == nil {
					invalidatehits++
					tih.Add(msg.TimeElapsed())
				} else {
					invalidatemisses++
					tim.Add(msg.TimeElapsed())
				}
			case message.MsgPut:
				puts++
				tp.Add(msg.TimeElapsed())
			}

		case <-quit:
			emptychan = true
		}
	}
	fmt.Printf("ERRORS: %d\nGet H:%d M:%d, Puts:%d, Invalidates H:%d M:%d\n"+
		"Get Hit Rate: %.2f Invalidate Hit Rate: %.2f\n"+
		"Mean times in usecs:\n"+
		"Get H:%.2f M:%.2f, Puts:%.2f, Inv H:%.2f M:%.2f\n",
		errors, gethits, getmisses, puts, invalidatehits, invalidatemisses,
		float64(gethits)/float64(gethits+getmisses),
		float64(invalidatehits)/float64(invalidatehits+invalidatemisses),
		tgh.MeanTimeUsecs(), tgm.MeanTimeUsecs(),
		tp.MeanTimeUsecs(),
		tih.MeanTimeUsecs(), tim.MeanTimeUsecs())
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
	cache := cache.NewCache(actual_blocks, log.Msgchan)

	var wgIo, wgRet sync.WaitGroup

	// Start up response server
	returnch := make(chan *message.Message, 100)
	quit := make(chan struct{})
	wgRet.Add(1)
	go response_handler(t, &wgRet, quit, returnch)

	// Create 100 clients
	for i := 0; i < 100; i++ {
		wgIo.Add(1)
		go func() {
			defer wgIo.Done()
			z := zipf.NewZipfWorkload(actual_blocks*10, 60 /* read % */)
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			here := make(chan *message.Message, 1)

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
					msg = message.NewMsgInvalidate()
					iopkt := msg.IoPkt()
					iopkt.Offset = offset
					msg.RetChan = here
					msg.TimeStart()
					cache.Msgchan <- msg
					<-here

					// Send it to returnch so that it can track
					// the stats
					returnch <- msg

					// Simulate waiting for storage device to write data
					// anywhere from 100usecs to 10ms
					time.Sleep(time.Microsecond * time.Duration((r.Intn(10000) + 100)))

					// Now, we can do a put
					msg = message.NewMsgPut()
				}

				iopkt := msg.IoPkt()
				iopkt.Buffer = make([]byte, blocksize)

				// Write the offset into the buffer so that we can
				// check it on reads.
				if !isread {
					bio := bufferio.NewBufferIO(iopkt.Buffer)
					bio.WriteDataLE(offset)
				}

				// Maximum "disk" size is 10 times bigger than cache
				iopkt.Offset = offset
				msg.RetChan = returnch

				// Send request
				msg.TimeStart()
				cache.Msgchan <- msg

				// Simulate waiting for more work by sleeping
				// anywhere from 100usecs to 10ms
				time.Sleep(time.Microsecond * time.Duration((r.Intn(10000) + 100)))
			}
		}()

	}

	// Wait for all clients to finish
	wgIo.Wait()

	// Print stats
	fmt.Print(cache)
	fmt.Print(log)

	// Close cache and log
	cache.Close()
	log.Close()

	// Send receiver a message that all clients have shut down
	close(quit)

	// Wait for receiver to finish emptying its channel
	wgRet.Wait()

	// Cleanup
	os.Remove(logfile)
}
