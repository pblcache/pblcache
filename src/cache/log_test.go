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
	"github.com/pblcache/pblcache/src/message"
	"os"
	"testing"
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
	blocks := uint64(4)
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
