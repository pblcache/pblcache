//
// Copyright (c) 2014 The Cache Authors
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
	"github.com/lpabon/godbc"
	_ "github.com/pblcache/pblcache/src/message"
)

type Cache struct {
	stats        *CacheStats
	cachemap     *CacheMap
	addressmap   map[uint64]uint64
	blocksize    uint64
	cachesize    uint64
	blocks       uint64
	writethrough bool
}

// cachesize is in bytes
// blocksize is in bytes
func NewCache(cachesize uint64, writethrough bool, blocksize uint64) *Cache {

	godbc.Require(cachesize > 0)
	godbc.Require(blocksize > 0)

	cache := &Cache{}
	cache.blocks = cachesize / blocksize
	cache.cachesize = cachesize
	cache.blocksize = blocksize
	cache.writethrough = writethrough

	cache.stats = NewCacheStats()
	cache.cachemap = NewCacheMap(cache.blocks)
	cache.addressmap = make(map[uint64]uint64)

	godbc.Ensure(cache.blocks > 0)
	godbc.Ensure(cache.cachemap != nil)
	godbc.Ensure(cache.addressmap != nil)
	godbc.Ensure(cache.stats != nil)

	return cache
}

func (c *Cache) Close() {
}

func (c *Cache) invalidate(key uint64) {
	if index, ok := c.addressmap[key]; ok {
		c.stats.invalidations++

		c.cachemap.Free(index)
		delete(c.addressmap, key)
	}
}

func (c *Cache) set(key uint64) (index uint64) {

	var (
		evictkey uint64
		evict    bool
	)

	c.stats.insertions++

	if index, evictkey, evict = c.cachemap.Insert(key); evict {
		delete(c.addressmap, evictkey)
	}

	return
}

func (c *Cache) get(key uint64) (index uint64, ok bool) {

	c.stats.reads++

	if index, ok = c.addressmap[key]; ok {
		c.stats.readhits++
	}

	return
}

func (c *Cache) String() string {
	return fmt.Sprintf(
		"Cache Utilization: 0 \n" +
			c.stats.String())
}

func (c *Cache) Stats() *CacheStats {
	return c.stats.Copy()
}

func (c *Cache) StatsClear() {
	c.stats = NewCacheStats()
}
