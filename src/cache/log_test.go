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
	"os"
	"testing"
)

const (
	testcachefile = "/tmp/l"
)

func TestNewLog(t *testing.T) {

	// Simple log
	l, blocks := NewLog(testcachefile, 16, 4096, 4096*4, 4096*2)
	assert(t, l != nil)
	assert(t, blocks == 16)
	l.Close()

	// Check the log correctly return maximum number of
	// blocks that are aligned to the segments.
	// 17 blocks are not aligned to a segment with 4 blocks
	// per segment
	l, blocks = NewLog(testcachefile, 17, 4096, 4096*4, 4096*2)
	assert(t, l != nil)
	assert(t, blocks == 16)
	l.Close()

	// Cleanup
	os.Remove(testcachefile)
}
