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
	"github.com/lpabon/godbc"
	"github.com/pblcache/pblcache/message"
	"sync"
)

type Cache struct {
	stats             *cachestats
	cachemap          *CacheMap
	addressmap        map[uint64]uint64
	blocks, blocksize uint64
	Msgchan           chan *message.Message
	pipeline          chan *message.Message
	quitchan          chan struct{}
	wg                sync.WaitGroup
}

var (
	ErrNotFound  = errors.New("None of the blocks where found")
	ErrSomeFound = errors.New("Only some of the blocks where found")
	ErrPending   = errors.New("New messages where created and are pending")
)

func NewCache(blocks, blocksize uint64, pipeline chan *message.Message) *Cache {

	godbc.Require(blocks > 0)
	godbc.Require(pipeline != nil)

	cache := &Cache{}
	cache.blocks = blocks
	cache.pipeline = pipeline
	cache.blocksize = blocksize

	cache.stats = &cachestats{}
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
					if io.Nblocks > 1 {
						for block := 0; block < io.Nblocks; block++ {
							m := message.NewMsgPut()
							m.RetChan = msg.RetChan

							mio := m.IoPkt()
							mio.Offset = io.Offset + uint64(block)*c.blocksize
							mio.Buffer = io.Buffer[uint64(block)*c.blocksize : uint64(block)*c.blocksize+c.blocksize]
							mio.BlockNum = c.put(io.Offset)
							mio.Nblocks = 1

							// Send to next one in line
							c.pipeline <- m
						}
					} else {
						io.BlockNum = c.put(io.Offset)
						c.pipeline <- msg
					}
				case message.MsgGet:
					hitmap := make([]bool, io.Nblocks)
					hits := 0

					// Create a message
					var m *message.Message
					var mblock int
					for block := 0; block < io.Nblocks; block++ {
						// Get
						current_offset := io.Offset + uint64(block)*c.blocksize
						if index, ok := c.get(current_offset); ok {
							hitmap[block] = true
							hits++
							if m == nil {
								m := message.NewMsgGet()
								m.RetChan = msg.RetChan
								m.Priv = msg.Priv
								mio := m.IoPkt()
								mio.Offset = current_offset
								mio.Buffer = io.Buffer[uint64(block)*c.blocksize : uint64(block+1)*c.blocksize]
								mio.BlockNum = index
								mblock = block
							} else {
								mio := m.IoPkt()
								numblocks := block - mblock
								if (mio.BlockNum+uint64(numblocks)) == index && hitmap[block-1] == true {
									// It is the next in both the cache and storage device
									mio.Buffer = io.Buffer[uint64(mblock)*c.blocksize : uint64(mblock+numblocks+1)*c.blocksize]
									mio.Nblocks++
								} else {
									// Send the previous one
									c.pipeline <- m

									// Now make a new one for the current block
									m := message.NewMsgGet()
									m.RetChan = msg.RetChan
									m.Priv = msg.Priv
									mio = m.IoPkt()
									mio.Offset = current_offset
									mio.Buffer = io.Buffer[uint64(block)*c.blocksize : uint64(block+1)*c.blocksize]
									mio.BlockNum = index
									mblock = block
								}

							}
						}
					}

					// Check if we have one more message
					if m != nil {
						c.pipeline <- m
					}

					if hits > 0 {
						// Some hit. Return hit map
						m := message.NewMsgHitmap(hitmap, hits, msg.RetChan)
						m.Done()
					} else {
						// None hit
						msg.Err = ErrNotFound
						msg.Done()
					}

				case message.MsgInvalidate:
					for block := 0; block < io.Nblocks; block++ {
						c.invalidate(io.Offset + uint64(block)*c.blocksize)
					}
					msg.Done()

					// case Release
				}

			case <-c.quitchan:
				// :TODO: Ok for now, but we cannot just quit
				// We need to empty the Iochan
				emptychan = true
			}
		}
	}()
}

func (c *Cache) invalidate(key uint64) bool {
	c.stats.invalidation()

	if index, ok := c.addressmap[key]; ok {
		c.stats.invalidateHit()

		c.cachemap.Free(index)
		delete(c.addressmap, key)

		return true
	}

	return false
}

func (c *Cache) put(key uint64) (index uint64) {

	var (
		evictkey uint64
		evict    bool
	)

	c.stats.insertion()

	if index, evictkey, evict = c.cachemap.Insert(key); evict {
		c.stats.eviction()
		delete(c.addressmap, evictkey)
	}

	c.addressmap[key] = index

	return
}

func (c *Cache) get(key uint64) (index uint64, ok bool) {

	c.stats.read()

	if index, ok = c.addressmap[key]; ok {
		c.stats.readHit()
		c.cachemap.Using(index)
	}

	return
}

func (c *Cache) String() string {
	return c.stats.stats().String()
}

func (c *Cache) Stats() *CacheStats {
	return c.stats.stats()
}

func (c *Cache) StatsClear() {
	c.stats.clear()
}
