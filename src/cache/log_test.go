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

package cache

import (
	"fmt"
	"github.com/lpabon/tm"
	"github.com/pblcache/pblcache/src/message"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	testcachefile = "/tmp/l"
)

func TestNewLog(t *testing.T) {

	// Simple log
	l, blocks := NewLog(testcachefile, 16, 4096, 4, 4096*2)
	assert(t, l != nil)
	assert(t, blocks == 16)
	l.Close()

	// Check the log correctly return maximum number of
	// blocks that are aligned to the segments.
	// 17 blocks are not aligned to a segment with 4 blocks
	// per segment
	l, blocks = NewLog(testcachefile, 17, 4096, 4, 4096*2)
	assert(t, l != nil)
	assert(t, blocks == 16)
	l.Close()

	// Cleanup
	os.Remove(testcachefile)
}

// Should wrap four times
func TestWrapPut(t *testing.T) {
	// Simple log
	blocks := uint64(16)
	l, logblocks := NewLog(testcachefile, blocks, 4096, 2, 4096*2)
	assert(t, l != nil)
	assert(t, blocks == logblocks)

	here := make(chan *message.Message)
	wraps := uint64(4)
	for io := uint8(0); io < uint8(blocks*wraps); io++ {
		buf := make([]byte, 4096)
		buf[0] = byte(io)

		msg := message.NewMsgPut()
		msg.RetChan = here

		iopkt := msg.IoPkt()
		iopkt.Buffer = buf
		iopkt.BlockNum = uint64(io % uint8(blocks))

		l.Msgchan <- msg
		<-here
	}

	// Cleanup
	l.Close()
	assert(t, l.stats.wraps == wraps)
	os.Remove(testcachefile)
}

func TestReadCorrectness(t *testing.T) {
	// Simple log
	blocks := uint64(240)
	bs := uint64(4096)
	blocks_per_segment := uint64(2)
	buffercache := uint64(4096 * 10)
	l, logblocks := NewLog(testcachefile,
		blocks,
		bs,
		blocks_per_segment,
		buffercache)
	assert(t, l != nil)
	assert(t, blocks == logblocks)

	here := make(chan *message.Message)
	for io := uint8(0); io < uint8(blocks); io++ {
		buf := make([]byte, 4096)
		buf[0] = byte(io)

		msg := message.NewMsgPut()
		msg.RetChan = here

		iopkt := msg.IoPkt()
		iopkt.Buffer = buf
		iopkt.BlockNum = uint64(io)

		l.Msgchan <- msg
		<-here
	}
	buf := make([]byte, 4096)
	msg := message.NewMsgGet()
	msg.RetChan = here

	iopkt := msg.IoPkt()
	iopkt.Buffer = buf
	iopkt.BlockNum = blocks - 1

	l.Msgchan <- msg
	<-here

	assert(t, buf[0] == uint8(blocks-1))

	for io := uint8(0); io < uint8(blocks); io++ {
		buf := make([]byte, 4096)
		msg := message.NewMsgGet()
		msg.RetChan = here

		iopkt := msg.IoPkt()
		iopkt.Buffer = buf
		iopkt.BlockNum = uint64(io)

		l.Msgchan <- msg
		<-here

		assert(t, buf[0] == uint8(io))
	}

	l.Close()
	os.Remove(testcachefile)
}

func logtest_response_handler(
	t *testing.T,
	wg *sync.WaitGroup,
	quit chan struct{},
	m chan *message.Message) {

	var (
		gets, puts int
		tg, tp     tm.TimeDuration
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
				gets++
				tg.Add(msg.TimeElapsed())
			case message.MsgPut:
				puts++
				tp.Add(msg.TimeElapsed())
			}

		case <-quit:
			emptychan = true
		}
	}
	fmt.Printf("Gets:%d, Puts:%d\n"+
		"Mean times in usecs: Gets:%.2f, Puts:%.2f\n",
		gets, puts, tg.MeanTimeUsecs(), tp.MeanTimeUsecs())
}

func TestLogConcurrency(t *testing.T) {
	// Simple log
	blocks := uint64(240)
	bs := uint64(4096)
	blocks_per_segment := uint64(2)
	buffercache := uint64(4096 * 10)
	l, logblocks := NewLog(testcachefile,
		blocks,
		bs,
		blocks_per_segment,
		buffercache)
	assert(t, l != nil)
	assert(t, blocks == logblocks)

	here := make(chan *message.Message)

	// Fill the log
	for io := uint8(0); io < uint8(blocks); io++ {
		buf := make([]byte, 4096)
		buf[0] = byte(io)

		msg := message.NewMsgPut()
		msg.RetChan = here

		iopkt := msg.IoPkt()
		iopkt.Buffer = buf
		iopkt.BlockNum = uint64(io)

		l.Msgchan <- msg
		<-here
	}

	var wgIo, wgRet sync.WaitGroup

	// Start up response server
	returnch := make(chan *message.Message, 100)
	quit := make(chan struct{})
	wgRet.Add(1)
	go logtest_response_handler(t, &wgRet, quit, returnch)

	// Create 100 readers
	for i := 0; i < 100; i++ {
		wgIo.Add(1)
		go func() {
			defer wgIo.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano()))

			// Each client to send 1k IOs
			for io := 0; io < 1000; io++ {
				msg := message.NewMsgGet()
				iopkt := msg.IoPkt()
				iopkt.Buffer = make([]byte, bs)

				// Maximum "disk" size is 10 times bigger than cache
				iopkt.BlockNum = uint64(r.Int63n(int64(blocks)))
				msg.RetChan = returnch

				// Send request
				msg.TimeStart()
				l.Msgchan <- msg

				// Simulate waiting for more work by sleeping
				// anywhere from 100usecs to 10ms
				time.Sleep(time.Microsecond * time.Duration((r.Intn(10000) + 100)))
			}
		}()
	}

	// Fill the log
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for wrap := 0; wrap < 30; wrap++ {
		for io := uint8(0); io < uint8(blocks); io++ {
			buf := make([]byte, 4096)
			buf[0] = byte(io)

			msg := message.NewMsgPut()
			msg.RetChan = here

			iopkt := msg.IoPkt()
			iopkt.Buffer = buf
			iopkt.BlockNum = uint64(io)

			l.Msgchan <- msg
			<-here
			time.Sleep(time.Microsecond * time.Duration((r.Intn(1000) + 100)))
		}
	}

	// Wait for all clients to finish
	wgIo.Wait()

	// Send receiver a message that all clients have shut down
	close(quit)

	// Wait for receiver to finish emptying its channel
	wgRet.Wait()

	// Cleanup
	l.Close()
	os.Remove(testcachefile)

}
