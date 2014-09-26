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

type CacheMap struct {
	segments           []Segment
	addressmap         AddressMap
	size               uint64
	index              uint64
	segment            uint64
	blocks_per_segment int
}

func NewCacheMap(blocks uint64, blocks_per_segment int) *CacheMap {

	if 0 == blocks {
		return nil
	}

	b := &CacheMap{}

	b.size = blocks
	b.blocks_per_segment = blocks_per_segment

	// Initialize addressmap
	InitAddressmap(&b.addressmap)

	// Initialize segments
	b.segments = make([]Segment, blocks)
	for s := 0; s < len(b.segments); s++ {
		InitSegment(&b.segments[s], blocks_per_segment)
	}

	return b
}

func (c *CacheMap) segment_from_index(index uint64) (segment uint64, block int) {
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
	c.segments[segment].Delete(c.addressmap, block)
}

func (c *CacheMap) Get(address uint64) (index uint64, found bool) {
	segment, _ := c.segment_from_index(index)
	return c.segments[segment].Get(&c.addressmap, address)
}

func (c *CacheMap) Set(address, index uint64) {
	c.addressmap.Set(address, index)
}
