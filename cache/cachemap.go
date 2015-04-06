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
	"encoding/gob"
	"errors"
	"github.com/lpabon/godbc"
	"github.com/pblcache/pblcache/message"
	"os"
	"sync"
)

type CacheMapSave struct {
	Bda               *BlockDescriptorArraySave
	Log               *LogSave
	Addressmap        map[uint64]uint32
	Blocks, Blocksize uint32
}

type CacheMap struct {
	stats             *cachestats
	bda               *BlockDescriptorArray
	addressmap        map[uint64]uint32
	blocks, blocksize uint32
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

func NewCacheMap(blocks, blocksize uint32, pipeline chan *message.Message) *CacheMap {

	godbc.Require(blocks > 0)
	godbc.Require(pipeline != nil)

	cache := &CacheMap{}
	cache.blocks = blocks
	cache.pipeline = pipeline
	cache.blocksize = blocksize

	cache.stats = &cachestats{}
	cache.bda = NewBlockDescriptorArray(cache.blocks)
	cache.addressmap = make(map[uint64]uint32)

	godbc.Ensure(cache.blocks > 0)
	godbc.Ensure(cache.bda != nil)
	godbc.Ensure(cache.addressmap != nil)
	godbc.Ensure(cache.stats != nil)

	return cache
}

func (c *CacheMap) Close() {
}

func (c *CacheMap) Invalidate(io *message.IoPkt) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	for block := uint32(0); block < io.Blocks; block++ {
		c.invalidate(io.BlockNum + block)
	}

	return nil
}

func (c *CacheMap) Put(msg *message.Message) error {

	err := msg.Check()
	if err != nil {
		return err
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	io := msg.IoPkt()

	if io.Blocks > 1 {
		// Have parent message wait for its children
		defer msg.Done()
		bio := NewBufferIo(io.Buffer, c.blocksize)

		//
		// It does not matter that we send small blocks to the Log, since
		// it will buffer them before sending them out to the cache device
		//
		// We do need to send each one sperately now so that the cache
		// policy hopefully aligns them one after the other.
		//
		for block := uint32(0); block < io.Blocks; block++ {
			child := message.NewMsgPut()
			msg.Add(child)

			child_io := child.IoPkt()
			child_io.BlockNum = io.BlockNum + block
			child_io.Buffer = bio.Block(block, 1)
			child_io.LogBlock = c.put(child_io.BlockNum)
			child_io.Blocks = 1

			// Send to next one in line
			c.pipeline <- child
		}
	} else {
		io.LogBlock = c.put(io.BlockNum)
		c.pipeline <- msg
	}

	return nil
}

func (c *CacheMap) Get(msg *message.Message) (*HitmapPkt, error) {

	err := msg.Check()
	if err != nil {
		return nil, err
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	io := msg.IoPkt()
	bio := NewBufferIo(io.Buffer, c.blocksize)
	hitmap := make([]bool, io.Blocks)
	hits := 0

	// Create a message
	var m *message.Message
	var mblock uint64

	for block := uint32(0); block < uint64(io.Blocks); block++ {
		// Get
		current_block := io.BlockNum + block
		if index, ok := c.get(current_block); ok {
			hitmap[block] = true
			hits++

			// Check if we already have a message ready
			if m == nil {

				// This is the first message, so let's set it up
				m = c.create_get_submsg(msg,
					current_block,
					index,
					bio.Block(block, 1))
				mblock = block
			} else {
				// Let's check what block we are using starting from the block
				// setup by the message
				numblocks := block - mblock

				// If the next block is available on the log after this block, then
				// we can optimize the read by reading a larger amount from the log.
				if m.IoPkt().LogBlock+numblocks == index && hitmap[block-1] == true {
					// It is the next in both the cache and storage device
					mio := m.IoPkt()
					mio.Buffer = bio.Block(mblock, numblocks)
					mio.Blocks++
				} else {
					// Send the previous one
					c.pipeline <- m

					// This is the first message, so let's set it up
					m = c.create_get_submsg(msg,
						current_block,
						index,
						bio.Block(block, 1))
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

func (c *CacheMap) create_get_submsg(msg *message.Message,
	block, logblock uint32,
	buffer []byte) *message.Message {

	m := message.NewMsgGet()
	msg.Add(m)

	// Set IoPkt
	mio := m.IoPkt()
	mio.BlockNum = block
	mio.Buffer = buffer
	mio.LogBlock = logblock

	return m
}

func (c *CacheMap) invalidate(key uint64) bool {
	c.stats.invalidation()

	if index, ok := c.addressmap[key]; ok {
		c.stats.invalidateHit()

		c.bda.Free(index)
		delete(c.addressmap, key)

		return true
	}

	return false
}

func (c *CacheMap) put(key uint64) (index uint32) {

	var (
		evictkey uint64
		evict    bool
	)

	c.stats.insertion()

	if index, evictkey, evict = c.bda.Insert(key); evict {
		c.stats.eviction()
		delete(c.addressmap, evictkey)
	}

	c.addressmap[key] = index

	return
}

func (c *CacheMap) get(key uint64) (index uint32, ok bool) {

	c.stats.read()

	if index, ok = c.addressmap[key]; ok {
		c.stats.readHit()
		c.bda.Using(index)
	}

	return
}

func (c *CacheMap) String() string {
	return c.stats.stats().String()
}

func (c *CacheMap) Stats() *CacheStats {
	return c.stats.stats()
}

func (c *CacheMap) StatsClear() {
	c.stats.clear()
}

func (c *CacheMap) Save(filename string, log *Log) error {

	c.lock.Lock()
	defer c.lock.Unlock()

	cs := &CacheMapSave{}
	cs.Addressmap = c.addressmap
	cs.Blocks = c.blocks
	cs.Blocksize = c.blocksize

	var err error
	cs.Bda, err = c.bda.Save()
	if err != nil {
		return err
	}

	if log != nil {
		cs.Log, err = log.Save()
		if err != nil {
			return err
		}
	}

	fi, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer fi.Close()

	encoder := gob.NewEncoder(fi)
	err = encoder.Encode(cs)
	if err != nil {
		return err
	}

	return nil
}

func (c *CacheMap) Load(filename string, log *Log) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	cs := &CacheMapSave{}

	fi, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fi.Close()

	decoder := gob.NewDecoder(fi)
	err = decoder.Decode(&cs)
	if err != nil {
		return err
	}

	err = c.bda.Load(cs.Bda, cs.Addressmap)
	if err != nil {
		return err
	}

	if cs.Log == nil && log != nil {
		return errors.New("No log metadata available")
	} else if cs.Log != nil && log == nil {
		return errors.New("Log unavaiable to apply loaded metadata")
	} else if cs.Log != nil && log != nil {
		err = log.Load(cs.Log, cs.Bda.Index)
		if err != nil {
			return err
		}
	}

	c.addressmap = cs.Addressmap
	c.blocks = cs.Blocks
	c.blocksize = cs.Blocksize

	return nil
}
