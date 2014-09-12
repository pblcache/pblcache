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

/*
import (
	"fmt"
	"github.com/lpabon/bufferio"
	"github.com/lpabon/godbc"
	"time"
)

type Kvdb interface {
	Close()
	Put(key, val []byte, index uint64) error
	Get(key, val []byte, index uint64) error
	Delete(key []byte, index uint64) error
	String() string
}

var buf []byte

type IoCacheKvDB struct {
	stats        *CacheStats
	cachemap     map[string]uint64
	chunksize    uint32
	cachesize    uint64
	writethrough bool
	cacheblocks  *IoCacheBlocks
	db           Kvdb
}

func NewIoCacheKvDB(cachesize, bcsize uint64, writethrough bool, chunksize uint32) *IoCacheKvDB {

	godbc.Require(cachesize > 0)

	cache := &IoCacheKvDB{}
	cache.stats = NewCacheStats()
	cache.cacheblocks = NewIoCacheBlocks(cachesize)
	cache.cachemap = make(map[string]uint64)
	cache.cachesize = cachesize
	cache.chunksize = chunksize
	cache.writethrough = writethrough

	buf = make([]byte, chunksize)

	cache.db = NewKVIoDB("cache.iodb", cachesize, bcsize, chunksize)

	godbc.Check(cache.db != nil)
	godbc.Ensure(cache.cachesize > 0)

	return cache
}

func (c *IoCacheKvDB) Close() {
	c.db.Close()
}

func (c *IoCacheKvDB) Invalidate(key string) {
	if index, ok := c.cachemap[key]; ok {
		c.stats.writehits++
		c.stats.invalidations++
		delete(c.cachemap, key)
		c.cacheblocks.Free(index)

		start := time.Now()
		c.db.Delete([]byte(key), index)
		end := time.Now()
		c.stats.tdeletions.Add(end.Sub(start))
	}
}

func (c *IoCacheKvDB) Insert(key string) {
	c.stats.insertions++

	evictkey, index, _ := c.cacheblocks.Insert(key)

	// Check for evictions
	if evictkey != "" {
		c.stats.evictions++
		delete(c.cachemap, evictkey)

		start := time.Now()
		c.db.Delete([]byte(evictkey), index)
		end := time.Now()
		c.stats.tdeletions.Add(end.Sub(start))
	}

	// Insert new key in cache map
	c.cachemap[key] = index

	b := bufferio.NewBufferIO(buf)
	b.Write([]byte(key))
	b.WriteDataLE(index)

	start := time.Now()
	c.db.Put([]byte(key), buf, index)
	end := time.Now()
	c.stats.twrites.Add(end.Sub(start))
}

func (c *IoCacheKvDB) Write(obj string, chunk string) {
	c.stats.writes++

	key := obj + chunk

	// Invalidate
	c.Invalidate(key)

	// We would do back end IO here

	// Insert
	if c.writethrough {
		c.Insert(key)
	}
}

func (c *IoCacheKvDB) Read(obj, chunk string) bool {
	c.stats.reads++

	key := obj + chunk

	if index, ok := c.cachemap[key]; ok {
		// Read Hit
		c.stats.readhits++

		// Allocate buffer
		val := make([]byte, c.chunksize)

		// Clock Algorithm: Set that we looked
		// at it
		c.cacheblocks.Using(index)
		start := time.Now()
		err := c.db.Get([]byte(key), val, index)
		end := time.Now()
		c.stats.treads.Add(end.Sub(start))
		godbc.Check(err == nil)

		// Check Data returned.
		var indexcheck uint64
		keycheck := make([]byte, len(key))
		b := bufferio.NewBufferIO(val)
		b.Read(keycheck)
		b.ReadDataLE(&indexcheck)
		godbc.Check(indexcheck == index, fmt.Sprintf("index[%v] != %v", index, indexcheck))
		godbc.Check(key == string(keycheck), fmt.Sprintf("key[%s] != %s", key, keycheck))

		return true

	} else {
		// Read miss
		// We would do IO here
		c.Insert(key)
		return false
	}
}

func (c *IoCacheKvDB) Delete(obj string) {
	// Not supported
}

func (c *IoCacheKvDB) String() string {
	return fmt.Sprintf(
		"Cache Utilization: %.2f %%\n",
		float64(len(c.cachemap))/float64(c.cachesize)*100.0) +
		c.stats.String() +
		c.db.String()
}

func (c *IoCacheKvDB) Stats() *CacheStats {
	return c.stats.Copy()
}

func (c *IoCacheKvDB) StatsClear() {
	c.stats = NewCacheStats()
}
*/
