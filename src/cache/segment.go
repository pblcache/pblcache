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
	"github.com/lpabon/godbc"
	"sync"
)

type BlockDescriptor struct {
	address uint64
	mru     bool
	used    bool
}

func (b *BlockDescriptor) free() {
	b.mru = false
	b.used = false
}

type Segment struct {
	bds  []BlockDescriptor
	lock sync.RWMutex
}

func NewSegment(blocks int) *Segment {
	s := Segment{}

	InitSegment(&s, blocks)

	return &s
}

func InitSegment(s *Segment, blocks int) {
	s.bds = make([]BlockDescriptor, blocks)
}

func (s *Segment) Evict(addressmap *AddressMap) int {

	// This lock assures there is no one reading or writing
	// to this segment on the storage device
	s.lock.Lock()
	defer s.lock.Unlock()

	// Will be used to return the number of evictions.
	// This will help the caller, because if we return
	// that we have evicted 0 blocks, then they should
	// immediately try the next segment
	evictions := 0

	// Go through the segment evicting all unused
	// blocks
	for i := 0; i < len(s.bds); i++ {
		bd := &s.bds[i]

		if bd.mru {
			bd.mru = false
		} else {
			if bd.used {
				addressmap.Delete(bd.address)
				bd.free()
			}
			evictions++
		}
	}

	return evictions
}

func (s *Segment) Delete(addressmap AddressMap, block int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.bds[block].free()
	addressmap.Delete(s.bds[block].address)

}

func (s *Segment) Used(block int) {
	s.Reserve()
	defer s.Release()

	godbc.Require(s.bds[block].used == true)

	s.bds[block].mru = true

}

func (s *Segment) Alloc(address uint64, block int) (newindex int, allocated bool) {
	s.Reserve()

	// Go through the segment evicting all unused
	// blocks
	for i := block; i < len(s.bds); i++ {
		if !s.bds[i].used {

			s.bds[i].used = true
			s.bds[i].mru = false
			s.bds[i].address = address

			// Return without releasing.
			// The caller should release this
			// segment once they are done
			newindex = i
			allocated = true
			return
		}
	}

	// No free slots, so we can release the segment
	s.Release()
	allocated = false

	return
}

func (s *Segment) Get(addresmap *AddressMap, address uint64) (index uint64, found bool) {
	s.Reserve()
	if index, found = addresmap.Get(address); found {
		// Address is available.  Do not release segment
		// until the reader has read the data.  They are responsible
		// for releasing the segment
		return
	}
	s.Release()
	return
}

func (s *Segment) Reserve() {
	s.lock.RLock()
}

func (s *Segment) Release() {
	s.lock.RUnlock()
}
