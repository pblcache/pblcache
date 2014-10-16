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
	"errors"
	"fmt"
	"github.com/lpabon/godbc"
	"github.com/pblcache/pblcache/src/message"
	"sync"
)

type Cache struct {
	stats      *CacheStats
	cachemap   *CacheMap
	addressmap map[uint64]uint64
	blocks     uint64
	Msgchan    chan *message.Message
	quitchan   chan struct{}
	wg         sync.WaitGroup
}

var (
	ErrNotFound = errors.New("Block not found")
)

func NewCache(blocks uint64) *Cache {

	godbc.Require(blocks > 0)

	cache := &Cache{}
	cache.blocks = blocks

	cache.stats = NewCacheStats()
	cache.cachemap = NewCacheMap(cache.blocks)
	cache.addressmap = make(map[uint64]uint64)

	cache.Msgchan = make(chan *message.Message, 32)
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

		emptychan := false
		for {

			// Check if we have been signaled through <-quit
			// If we have, we now know that as soon as the
			// message channel is empty, we can quit.
			if emptychan {
				if len(c.Msgchan) == 0 {
					break
				}
			}

			select {
			case msg := <-c.Msgchan:
				msg.Err = nil
				io := msg.IoPkt()
				switch msg.Type {
				case message.MsgPut:
					// PUT
					io.BlockNum = c.set(io.Offset)
				case message.MsgGet:
					// Get
					if index, ok := c.get(io.Offset); ok {
						io.BlockNum = index
					} else {
						msg.Err = ErrNotFound
					}

				case message.MsgInvalidate:
					if ok := c.invalidate(io.Offset); !ok {
						msg.Err = ErrNotFound
					}

					// case Release
				}

				msg.Done()
			case <-c.quitchan:
				// :TODO: Ok for now, but we cannot just quit
				// We need to empty the Iochan
				emptychan = true
			}
		}
	}()
}

func (c *Cache) invalidate(key uint64) bool {
	if index, ok := c.addressmap[key]; ok {
		c.stats.invalidations++

		c.cachemap.Free(index)
		delete(c.addressmap, key)

		return true
	}

	return false
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
