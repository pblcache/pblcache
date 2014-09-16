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
	"github.com/lpabon/godbc"
)

type PblCache struct {
	stats        *CacheStats
	cachemap     *CacheMap
	blocksize    uint32
	cachesize    uint64
	blocks       uint64
	writethrough bool
}

// cachesize is in bytes
// blocksize is in bytes
func NewPblCache(cachesize uint64, writethrough bool, blocksize uint32) *PblCache {

	godbc.Require(cachesize > 0)

	cache := &PblCache{}
	cache.blocks = cachesize / uint64(blocksize)
	cache.cachesize = cachesize
	cache.blocksize = blocksize
	cache.writethrough = writethrough

	cache.stats = NewCacheStats()
	cache.cachemap = NewCacheMap(cache.blocks)

	//cache.db = NewKVIoDB("cache.iodb", cachesize, bcsize, blocksize)
	//godbc.Check(cache.db != nil)

	godbc.Ensure(cache.blocks > 0)
	godbc.Ensure(cache.cachemap != nil)
	godbc.Ensure(cache.stats != nil)

	return cache
}

func (c *PblCache) Close() {
}

func (c *PblCache) Invalidate(key AddressMapKey) {
	if c.cachemap.HasAddressMapKey(key) {
		c.stats.writehits++
		c.stats.invalidations++
		c.cachemap.FreeAddress(key)

		/*
			start := time.Now()
			c.db.Delete([]byte(key), index)
			end := time.Now()
			c.stats.tdeletions.Add(end.Sub(start))
		*/
	}
}

func (c *PblCache) Insert(key AddressMapKey) {
	c.stats.insertions++

	index, _ := c.cachemap.Alloc(key)

	// Insert new key in cache map
	c.cachemap.SetAddressMapKey(key, index)

}

func (c *PblCache) Write(obj, block uint64) {
	c.stats.writes++

	key := AddressMapKey{obj, block}

	// Invalidate
	c.Invalidate(key)

	// We would do back end IO here

	// Insert
	if c.writethrough {
		c.Insert(key)
	}
}

func (c *PblCache) Read(obj, block uint64) bool {
	c.stats.reads++

	if _, ok := c.cachemap.Get(obj, block); ok {
		// Read Hit
		c.stats.readhits++
		return true

	} else {
		// Read miss
		c.Insert(AddressMapKey{obj, block})
		return false
	}
}

func (c *PblCache) String() string {
	return fmt.Sprintf(
		"Cache Utilization: 0 \n" +
			c.stats.String())
}

func (c *PblCache) Stats() *CacheStats {
	return c.stats.Copy()
}

func (c *PblCache) StatsClear() {
	c.stats = NewCacheStats()
}
