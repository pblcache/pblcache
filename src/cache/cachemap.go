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
)

const (
	INVALID_KEY = ^uint64(0)
)

type BlockDescriptor struct {
	key  uint64
	mru  bool
	used bool
}

type CacheMap struct {
	bds   []BlockDescriptor
	size  uint64
	index uint64
}

func NewCacheMap(blocks uint64) *CacheMap {

	godbc.Require(blocks > 0)

	c := &CacheMap{}

	c.size = blocks
	c.bds = make([]BlockDescriptor, blocks)

	return c
}

func (c *CacheMap) Insert(key uint64) (newindex, evictkey uint64, evict bool) {
	for {

		// Use the current index to check the current entry
		for ; c.index < c.size; c.index++ {
			entry := &c.bds[c.index]

			// CLOCK: If it has been used recently, then do not evict
			if entry.mru {
				entry.mru = false
			} else {

				// If it is in use, then we need to evict the older key
				if entry.used {
					evictkey = entry.key
					evict = true
				} else {
					evictkey = INVALID_KEY
					evict = false
				}

				// Set return values
				newindex = c.index

				// Setup current cachemap entry
				entry.key = key
				entry.mru = false
				entry.used = true

				// Set index to next cachemap entry
				c.index++

				return
			}
		}
		c.index = 0
	}
}

func (c *CacheMap) Using(index uint64) {
	c.bds[index].mru = true
}

func (c *CacheMap) Free(index uint64) {
	c.bds[index].mru = false
	c.bds[index].used = false
	c.bds[index].key = INVALID_KEY
}

/*
func (c *CacheMap) segment_from_index(index uint64) (segment uint64, entry int) {
	segment = index / uint64(c.blocks_per_segment)
	block = int(index % uint64(c.blocks_per_segment))
	return
}

func (c *CacheMap) index_from_segment(segment uint64, block int) uint64 {
	return (segment * uint64(c.blocks_per_segment)) + uint64(block)
}

func (c *CacheMap) init_next_segment(segment uint64) {
	for {
		for c.segment = segment; c.segment < uint64(len(c.segments)); c.segment++ {
			// Evict any blocks no longer needed, and return
			// the number of evicted blocks
			if 0 != c.segments[c.segment].Evict(&c.addressmap) {
				return
			}
		}
		c.segment = 0
	}
}

func (c *CacheMap) Alloc(address uint64) (newindex uint64) {
	for {

		// Start at the current block
		for ; c.index < c.size; c.index++ {

			// Get which segment the current block is
			segment, block := c.segment_from_index(c.index)

			// Check if we need to move to the next available
			// segment.
			if segment > c.segment {
				c.init_next_segment(segment)
			} else {

				// Ask the current segment for the next available block
				if newblock, ok := c.segments[c.segment].Alloc(address, block); ok {
					newindex = c.index_from_segment(c.segment, newblock)
					return
				}
			}
		}
		c.index = 0
	}
}

func (c *CacheMap) Using(index uint64) {
	segment, block := c.segment_from_index(index)
	c.segments[segment].Used(block)
}

func (c *CacheMap) Release(index uint64) {
	segment, _ := c.segment_from_index(index)
	c.segments[segment].Release()
}

func (c *CacheMap) Free(index uint64) {
	segment, block := c.segment_from_index(index)
	c.segments[segment].Delete(&c.addressmap, block)
}

func (c *CacheMap) Get(address uint64) (index uint64, found bool) {
	segment, _ := c.segment_from_index(index)
	return c.segments[segment].Get(&c.addressmap, address)
}

func (c *CacheMap) Set(address, index uint64) {
	c.addressmap.Set(address, index)
}

*/
