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
	pipeline          chan *message.Message
	lock              sync.Mutex
}

type HitmapPkt struct {
	Hitmap []bool
	Hits   int
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

	godbc.Ensure(cache.blocks > 0)
	godbc.Ensure(cache.cachemap != nil)
	godbc.Ensure(cache.addressmap != nil)
	godbc.Ensure(cache.stats != nil)

	return cache
}

func (c *Cache) Close() {
}

func (c *Cache) Invalidate(io *message.IoPkt) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	for block := 0; block < io.Nblocks; block++ {
		c.invalidate(io.Offset + uint64(block)*c.blocksize)
	}

	return nil
}

func (c *Cache) Put(msg *message.Message) error {

	c.lock.Lock()
	defer c.lock.Unlock()
	defer msg.Done()

	io := msg.IoPkt()

	if io.Nblocks > 1 {
		//
		// It does not matter that we send small blocks to the Log, since
		// it will buffer them before sending them out to the cache device
		//
		// We do need to send each one sperately now so that the cache
		// policy hopefully aligns them one after the other.
		//
		for block := 0; block < io.Nblocks; block++ {
			m := message.NewMsgPut()
			m.Add(msg)

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

	return nil
}

func (c *Cache) Get(msg *message.Message) (*HitmapPkt, error) {

	c.lock.Lock()
	defer c.lock.Unlock()

	io := msg.IoPkt()
	hitmap := make([]bool, io.Nblocks)
	hits := 0

	// Create a message
	var m *message.Message
	var mblock uint64

	for block := uint64(0); block < uint64(io.Nblocks); block++ {
		// Get
		buffer_offset := block * c.blocksize
		current_offset := io.Offset + buffer_offset
		if index, ok := c.get(current_offset); ok {
			hitmap[block] = true
			hits++

			// Check if we already have a message ready
			if m == nil {

				// This is the first message, so let's set it up
				m = c.create_get_submsg(msg,
					current_offset,
					buffer_offset,
					index,
					io.Buffer[buffer_offset:(block+1)*c.blocksize])
				mblock = block
			} else {
				// Let's check what block we are using starting from the block
				// setup by the message
				numblocks := block - mblock

				// If the next block is available on the log after this block, then
				// we can optimize the read by reading a larger amount from the log.
				if m.IoPkt().BlockNum+numblocks == index && hitmap[block-1] == true {
					// It is the next in both the cache and storage device
					mio := m.IoPkt()
					mio.Buffer = io.Buffer[mblock*c.blocksize : (mblock+numblocks+1)*c.blocksize]
					mio.Nblocks++
				} else {
					// Send the previous one
					c.pipeline <- m

					// This is the first message, so let's set it up
					m = c.create_get_submsg(msg,
						current_offset,
						buffer_offset,
						index,
						io.Buffer[buffer_offset:(block+1)*c.blocksize])
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
		hitmappkt := &HitmapPkt{
			Hitmap: hitmap,
			Hits:   hits,
		}
		msg.Done()
		return hitmappkt, nil
	} else {
		return nil, ErrNotFound
	}
}

func (c *Cache) create_get_submsg(msg *message.Message,
	offset, buffer_offset, blocknum uint64,
	buffer []byte) *message.Message {

	m := message.NewMsgGet()
	m.Add(msg)

	// Set IoPkt
	mio := m.IoPkt()
	mio.Offset = offset
	mio.Buffer = buffer
	mio.BlockNum = blocknum

	return m
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
