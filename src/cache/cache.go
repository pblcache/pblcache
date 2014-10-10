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
	"errors"
	"fmt"
	"github.com/lpabon/godbc"
	"github.com/pblcache/pblcache/src/message"
	"sync"
)

type Cache struct {
	stats        *CacheStats
	cachemap     *CacheMap
	addressmap   map[uint64]uint64
	blocksize    uint64
	cachesize    uint64
	blocks       uint64
	writethrough bool
	Iochan       chan *message.MsgIo
	Msgchan      chan *message.Message
	quitchan     chan struct{}
	wg           sync.WaitGroup
}

var (
	ErrNotFound = errors.New("Block not found")
)

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

	cache.Iochan = make(chan *message.MsgIo, 128)
	cache.Msgchan = make(chan *message.Message, 128)
	cache.quitchan = make(chan struct{})

	godbc.Ensure(cache.blocks > 0)
	godbc.Ensure(cache.cachemap != nil)
	godbc.Ensure(cache.addressmap != nil)
	godbc.Ensure(cache.stats != nil)

	// Start goroutine
	cache.server()

	return cache
}

func (c *Cache) Close() {
	close(c.quitchan)
	c.wg.Wait()
}

func (c *Cache) server() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case iomsg := <-c.Iochan:
				// Do io channel
				// EMPTY ON QUIT!
				switch iomsg.Type {
				case message.MsgPut:
					// PUT
					iomsg.BlockNum = c.set(iomsg.Offset)
				case message.MsgGet:
					// Get
					if index, ok := c.get(iomsg.Offset); ok {
						iomsg.BlockNum = index
						iomsg.Err = nil
					} else {
						iomsg.Err = ErrNotFound
					}

				case message.MsgInvalidate:
					c.invalidate(iomsg.Offset)

					// case Release
				}

				iomsg.Done()
			case <-c.Msgchan:
				// Do simple command
			case <-c.quitchan:
				// :TODO: Ok for now, but we cannot just quit
				// We need to empty the Iochan
				return
			}
		}
	}()
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

	c.addressmap[key] = index

	return
}

func (c *Cache) get(key uint64) (index uint64, ok bool) {

	c.stats.reads++

	if index, ok = c.addressmap[key]; ok {
		c.stats.readhits++
		c.cachemap.Using(index)
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
