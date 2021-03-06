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
	"github.com/pblcache/pblcache/message"
	"github.com/pblcache/pblcache/tests"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestNewLog(t *testing.T) {

	mockfile := tests.NewMockFile()
	seeklen := int64(16 * 4096)
	mockfile.MockSeek = func(offset int64, whence int) (int64, error) {
		return seeklen, nil
	}

	// Mock openFile
	defer tests.Patch(&openFile,
		func(name string, flag int, perm os.FileMode) (Filer, error) {
			return mockfile, nil
		}).Restore()

	// Simple log
	l, blocks, err := NewLog("file", 4096, 4, 4096*2, false)
	tests.Assert(t, err == nil)
	tests.Assert(t, l != nil)
	tests.Assert(t, blocks == 16)
	l.Close()

	// Check the log correctly return maximum number of
	// blocks that are aligned to the segments.
	// 17 blocks are not aligned to a segment with 4 blocks
	// per segment
	seeklen = 17 * 4096
	l, blocks, err = NewLog("file", 4096, 4, 4096*2, false)
	tests.Assert(t, err == nil)
	tests.Assert(t, l != nil)
	tests.Assert(t, blocks == 16)
	l.Close()
}

func TestLogMultiBlock(t *testing.T) {

	// 256 blocks available in the log
	seeklen := int64(256 * 4096)

	// Setup Mockfile
	mockfile := tests.NewMockFile()
	mockfile.MockSeek = func(offset int64, whence int) (int64, error) {
		return seeklen, nil
	}

	mock_byteswritten := 0
	mock_written := 0
	mock_off_written := int64(0)
	continue_test := make(chan bool, 1)
	mockfile.MockWriteAt = func(p []byte, off int64) (n int, err error) {
		mock_written++
		mock_off_written = off
		mock_byteswritten += len(p)
		continue_test <- true
		return len(p), nil
	}

	mock_bytesread := 0
	mock_read := 0
	mock_off_read := int64(0)
	mockfile.MockReadAt = func(p []byte, off int64) (n int, err error) {
		mock_read++
		mock_off_read = off
		mock_bytesread += len(p)
		continue_test <- true
		return len(p), nil
	}

	// Mock openFile
	defer tests.Patch(&openFile,
		func(name string, flag int, perm os.FileMode) (Filer, error) {
			return mockfile, nil
		}).Restore()

	// Simple log
	l, blocks, err := NewLog("file", 4096, 4, 0, false)
	tests.Assert(t, err == nil)
	tests.Assert(t, l != nil)
	tests.Assert(t, blocks == 256)
	l.Start()

	// Send 8 blocks
	here := make(chan *message.Message)
	m := message.NewMsgPut()
	iopkt := m.IoPkt()
	m.RetChan = here
	iopkt.Buffer = make([]byte, 8*4096)
	iopkt.Blocks = 8

	for block := uint32(0); block < iopkt.Blocks; block++ {
		child := message.NewMsgPut()
		m.Add(child)

		child_io := child.IoPkt()
		child_io.Address = iopkt.Address + uint64(block)
		child_io.Buffer = SubBlockBuffer(iopkt.Buffer, 4096, block, 1)
		child_io.LogBlock = block
		child_io.Blocks = 1

		l.Msgchan <- child
	}

	m.Done()
	<-here
	<-continue_test

	tests.Assert(t, mock_byteswritten == 4*4096)
	tests.Assert(t, mock_written == 1)
	tests.Assert(t, mock_off_written == 0)
	tests.Assert(t, mock_read == 0)
	tests.Assert(t, len(continue_test) == 0)

	// At this point we have 4 blocks written to the log storage
	// and 4 blocks in the current segment.

	// Read log blocks 0-3
	m = message.NewMsgGet()
	iopkt = m.IoPkt()
	m.RetChan = here
	iopkt.Blocks = 4
	iopkt.Buffer = make([]byte, 4*4096)
	iopkt.LogBlock = 0

	mock_written = 0
	mock_byteswritten = 0
	l.Msgchan <- m
	<-here
	<-continue_test

	tests.Assert(t, mock_byteswritten == 0)
	tests.Assert(t, mock_written == 0)
	tests.Assert(t, mock_read == 1)
	tests.Assert(t, mock_bytesread == 4*4096)
	tests.Assert(t, mock_off_read == 0)
	tests.Assert(t, len(continue_test) == 0)

	// Now read log blocks 1,2,3,4,5.  Blocks 1,2,3 will be on the storage
	// device, and blocks 4,5 will be in ram
	m = message.NewMsgGet()
	iopkt = m.IoPkt()
	m.RetChan = here
	iopkt.Blocks = 5
	iopkt.Buffer = make([]byte, 5*4096)
	iopkt.LogBlock = 1

	mock_written = 0
	mock_byteswritten = 0
	mock_bytesread = 0
	mock_read = 0
	l.Msgchan <- m
	<-here
	<-continue_test

	tests.Assert(t, mock_byteswritten == 0)
	tests.Assert(t, mock_written == 0)
	tests.Assert(t, mock_read == 1)
	tests.Assert(t, mock_bytesread == 3*4096)
	tests.Assert(t, mock_off_read == 1*4096)
	tests.Assert(t, len(continue_test) == 0)

	// Cleanup
	l.Close()
}

// Should wrap four times
func TestWrapPut(t *testing.T) {
	// Simple log
	blocks := uint32(16)

	testcachefile := tests.Tempfile()
	err := tests.CreateFile(testcachefile, 16*4096)
	tests.Assert(t, nil == err)
	defer os.Remove(testcachefile)

	l, logblocks, err := NewLog(testcachefile, 4096, 2, 4096*2, false)
	tests.Assert(t, err == nil)
	tests.Assert(t, l != nil)
	tests.Assert(t, blocks == logblocks)
	l.Start()

	here := make(chan *message.Message)
	wraps := uint32(4)

	// Write enough blocks to wrap around the log
	// as many times as determined by the value in 'wraps'
	for io := uint32(0); io < (blocks * wraps); io++ {
		buf := make([]byte, 4096)
		buf[0] = byte(io)

		msg := message.NewMsgPut()
		msg.RetChan = here

		iopkt := msg.IoPkt()
		iopkt.Buffer = buf
		iopkt.LogBlock = io % blocks

		l.Msgchan <- msg
		<-here
	}

	// Close will also empty all the channels
	l.Close()

	// Check that we have wrapped the correct number of times
	tests.Assert(t, l.Stats().Wraps == uint64(wraps))
}

func TestReadCorrectness(t *testing.T) {
	// Simple log
	blocks := uint32(240)
	bs := uint32(4096)
	blocks_per_segment := uint32(2)
	buffercache := uint32(4096 * 10)
	testcachefile := tests.Tempfile()
	tests.Assert(t, nil == tests.CreateFile(testcachefile, int64(blocks*4096)))
	defer os.Remove(testcachefile)
	l, logblocks, err := NewLog(testcachefile,
		bs,
		blocks_per_segment,
		buffercache,
		false)
	tests.Assert(t, err == nil)
	tests.Assert(t, l != nil)
	tests.Assert(t, blocks == logblocks)
	l.Start()

	here := make(chan *message.Message)

	// Write enough blocks in the log to reach
	// the end.
	for io := uint32(0); io < blocks; io++ {
		buf := make([]byte, 4096)

		// Save the block number in the buffer
		// so that we can check it later.  For simplicity
		// we have made sure the block number is only
		// one byte.
		buf[0] = byte(io)

		msg := message.NewMsgPut()
		msg.RetChan = here

		iopkt := msg.IoPkt()
		iopkt.Buffer = buf
		iopkt.LogBlock = io

		l.Msgchan <- msg
		<-here
	}
	buf := make([]byte, 4096)
	msg := message.NewMsgGet()
	msg.RetChan = here

	iopkt := msg.IoPkt()
	iopkt.Buffer = buf
	iopkt.LogBlock = blocks - 1

	l.Msgchan <- msg
	<-here

	tests.Assert(t, buf[0] == uint8(blocks-1))

	for io := uint32(0); io < blocks; io++ {
		buf := make([]byte, 4096)
		msg := message.NewMsgGet()
		msg.RetChan = here

		iopkt := msg.IoPkt()
		iopkt.Buffer = buf
		iopkt.LogBlock = io
		l.Msgchan <- msg

		// Wait here for the response
		<-here

		// Check the block number is correct
		tests.Assert(t, buf[0] == uint8(io))
	}

	l.Close()
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
	blocks := uint32(240)
	bs := uint32(4096)
	blocks_per_segment := uint32(2)
	buffercache := uint32(4096 * 24)
	testcachefile := tests.Tempfile()
	tests.Assert(t, nil == tests.CreateFile(testcachefile, int64(blocks*4096)))
	defer os.Remove(testcachefile)
	l, logblocks, err := NewLog(testcachefile,
		bs,
		blocks_per_segment,
		buffercache,
		false)
	tests.Assert(t, err == nil)
	tests.Assert(t, l != nil)
	tests.Assert(t, blocks == logblocks)
	l.Start()

	here := make(chan *message.Message)

	// Fill the log
	for io := uint32(0); io < blocks; io++ {
		buf := make([]byte, 4096)
		buf[0] = byte(io)

		msg := message.NewMsgPut()
		msg.RetChan = here

		iopkt := msg.IoPkt()
		iopkt.Buffer = buf
		iopkt.LogBlock = io

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
				iopkt.LogBlock = uint32(r.Int31n(int32(blocks)))
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

	// Write to the log while the readers are reading
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for wrap := 0; wrap < 30; wrap++ {
		for io := uint32(0); io < blocks; io++ {
			buf := make([]byte, 4096)
			buf[0] = byte(io)

			msg := message.NewMsgPut()
			msg.RetChan = returnch

			iopkt := msg.IoPkt()
			iopkt.Buffer = buf
			iopkt.LogBlock = io

			msg.TimeStart()
			l.Msgchan <- msg
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
	fmt.Print(l)
	l.Close()
	os.Remove(testcachefile)

}
