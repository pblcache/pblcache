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
package spc

import (
	"github.com/lpabon/godbc"
	"github.com/pblcache/pblcache/cache"
	"github.com/pblcache/pblcache/message"
	"io"
)

func readandstore(fp io.ReaderAt,
	c *cache.CacheMap,
	devid, offset, blocks uint32,
	buffer []byte,
	retchan chan *message.Message) {

	fp.ReadAt(buffer, int64(offset)*4*KB)

	m := message.NewMsgPut()
	m.RetChan = retchan
	io := m.IoPkt()
	io.Address = cache.Address64(cache.Address{Devid: devid, Block: offset})
	io.Buffer = buffer
	io.Blocks = blocks

	c.Put(m)
}

func read(fp io.ReaderAt,
	c *cache.CacheMap,
	devid, offset, blocks uint32,
	buffer []byte) {

	godbc.Require(len(buffer)%(4*KB) == 0)

	here := make(chan *message.Message, blocks)
	cacheoffset := cache.Address64(cache.Address{Devid: devid, Block: offset})
	msg := message.NewMsgGet()
	msg.RetChan = here
	iopkt := msg.IoPkt()
	iopkt.Buffer = buffer
	iopkt.Address = cacheoffset
	iopkt.Blocks = blocks

	msgs := 0
	hitpkt, err := c.Get(msg)
	if err != nil {
		//fmt.Printf("|blocks:%d::hits:0--", blocks)
		// None found
		// Read the whole thing from backend
		fp.ReadAt(buffer, int64(offset)*4*KB)

		m := message.NewMsgPut()
		m.RetChan = here

		io := m.IoPkt()
		io.Address = cacheoffset
		io.Buffer = buffer
		io.Blocks = blocks
		c.Put(m)
		msgs++

	} else if hitpkt.Hits != blocks {
		//fmt.Printf("|******blocks:%d::hits:%d--", blocks, hitpkt.Hits)
		// Read from storage the ones that did not have
		// in the hit map.
		var be_offset, be_block, be_blocks uint32
		var be_read_ready = false
		for block := uint32(0); block < blocks; block++ {
			if !hitpkt.Hitmap[int(block)] {
				if be_read_ready {
					be_blocks++
				} else {
					be_read_ready = true
					be_offset = offset + block
					be_block = block
					be_blocks++
				}
			} else {
				if be_read_ready {
					// Send read
					msgs++
					go readandstore(fp, c, devid,
						be_offset,
						be_blocks,
						cache.SubBlockBuffer(buffer, 4*KB, be_block, be_blocks),
						here)
					be_read_ready = false
					be_blocks = 0
					be_offset = 0
					be_block = 0
				}
			}
		}
		if be_read_ready {
			msgs++
			go readandstore(fp, c, devid,
				be_offset,
				be_blocks,
				cache.SubBlockBuffer(buffer, 4*KB, be_block, be_blocks),
				here)
		}

	} else {
		msgs = 1
	}

	// Wait for blocks to be returned
	for msg := range here {

		msgs--
		godbc.Check(msg.Err == nil, msg)
		godbc.Check(msgs >= 0, msgs)

		if msgs == 0 {
			return
		}
	}
}

func write(fp io.WriterAt,
	c *cache.CacheMap,
	devid, offset, blocks uint32,
	buffer []byte) {

	here := make(chan *message.Message, blocks)
	cacheoffset := cache.Address64(cache.Address{Devid: devid, Block: offset})

	// Send invalidates for each block
	iopkt := &message.IoPkt{
		Address: cacheoffset,
		Blocks:  blocks,
	}
	c.Invalidate(iopkt)

	// Write to storage back end
	// :TODO: check return status
	fp.WriteAt(buffer, int64(offset)*4*KB)

	// Now write to cache
	msg := message.NewMsgPut()
	msg.RetChan = here
	iopkt = msg.IoPkt()
	iopkt.Blocks = blocks
	iopkt.Address = cacheoffset
	iopkt.Buffer = buffer
	c.Put(msg)

	<-here
}
