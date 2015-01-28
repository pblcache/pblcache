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
	Key  uint64
	Mru  bool
	Used bool
}

type CacheMapSave struct {
	Bds   []BlockDescriptor
	Size  uint64
	Index uint64
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
			if entry.Mru {
				entry.Mru = false
			} else {

				// If it is in use, then we need to evict the older key
				if entry.Used {
					evictkey = entry.Key
					evict = true
				} else {
					evictkey = INVALID_KEY
					evict = false
				}

				// Set return values
				newindex = c.index

				// Setup current cachemap entry
				entry.Key = key
				entry.Mru = false
				entry.Used = true

				// Set index to next cachemap entry
				c.index++

				return
			}
		}
		c.index = 0
	}
}

func (c *CacheMap) Using(index uint64) {
	c.bds[index].Mru = true
}

func (c *CacheMap) Free(index uint64) {
	c.bds[index].Mru = false
	c.bds[index].Used = false
	c.bds[index].Key = INVALID_KEY
}

func (c *CacheMap) Save() *CacheMapSave {
	cs := &CacheMapSave{}
	cs.Bds = c.bds
	cs.Index = c.index
	cs.Size = c.size

	return cs
}

func (c *CacheMap) Load(cs *CacheMapSave) {
	c.bds = cs.Bds
	c.index = cs.Index
	c.size = cs.Size
}
