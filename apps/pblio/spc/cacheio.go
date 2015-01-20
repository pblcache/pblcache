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
	c *cache.Cache,
	devid uint16,
	offset uint64,
	nblocks uint64,
	buffer []byte,
	retchan chan *message.Message) {

	fp.ReadAt(buffer, int64(offset))

	m := message.NewMsgPut()
	m.RetChan = retchan
	io := m.IoPkt()
	io.Offset = cache.Address64(cache.Address{Devid: devid, Lba: offset})
	io.Buffer = buffer
	io.Nblocks = int(nblocks)

	c.Put(m)
}

func read(fp io.ReaderAt,
	c *cache.Cache,
	devid uint16,
	offset, blocksize_bytes uint64,
	nblocks int,
	buffer []byte) {

	godbc.Require(len(buffer)%(4*KB) == 0)
	godbc.Require(blocksize_bytes%(4*KB) == 0)

	here := make(chan *message.Message, nblocks)
	cacheoffset := cache.Address64(cache.Address{Devid: devid, Lba: offset})
	msg := message.NewMsgGet()
	msg.RetChan = here
	iopkt := msg.IoPkt()
	iopkt.Buffer = buffer
	iopkt.Offset = cacheoffset
	iopkt.Nblocks = nblocks

	msgs := 0
	hitpkt, err := c.Get(msg)
	if err != nil {
		//fmt.Printf("|nblocks:%d::hits:0--", nblocks)
		// None found
		// Read the whole thing from backend
		fp.ReadAt(buffer, int64(offset))

		m := message.NewMsgPut()
		m.RetChan = here

		io := m.IoPkt()
		io.Offset = cacheoffset
		io.Buffer = buffer
		io.Nblocks = nblocks
		c.Put(m)
		msgs++

	} else if hitpkt.Hits != nblocks {
		//fmt.Printf("|******nblocks:%d::hits:%d--", nblocks, hitpkt.Hits)
		// Read from storage the ones that did not have
		// in the hit map.
		var be_offset, be_block, be_nblocks uint64
		var be_read_ready = false
		for block := uint64(0); block < uint64(nblocks); block++ {
			if !hitpkt.Hitmap[block] {
				if be_read_ready {
					be_nblocks++
				} else {
					be_read_ready = true
					be_offset = offset + (block * blocksize_bytes)
					be_block = block
					be_nblocks++
				}
			} else {
				if be_read_ready {
					// Send read
					buffer_offset := be_block * blocksize_bytes
					msgs++
					go readandstore(fp, c, devid, be_offset, be_nblocks,
						buffer[buffer_offset:(buffer_offset+blocksize_bytes)],
						here)
					be_read_ready = false
					be_nblocks = 0
					be_offset = 0
					be_block = 0
				}
			}
		}
		if be_read_ready {
			buffer_offset := be_block * blocksize_bytes
			msgs++
			go readandstore(fp, c, devid, be_offset, be_nblocks,
				buffer[buffer_offset:(buffer_offset+blocksize_bytes)],
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
	c *cache.Cache,
	devid uint16,
	offset, blocksize_bytes uint64,
	nblocks int,
	buffer []byte) {

	godbc.Require(len(buffer)%(4*KB) == 0)
	godbc.Require(blocksize_bytes%(4*KB) == 0)

	here := make(chan *message.Message, nblocks)
	cacheoffset := cache.Address64(cache.Address{Devid: devid, Lba: offset})

	// Send invalidates for each block
	iopkt := &message.IoPkt{
		Offset:  cacheoffset,
		Nblocks: nblocks,
	}
	c.Invalidate(iopkt)

	// Write to storage back end
	// :TODO: check return status
	fp.WriteAt(buffer, int64(offset))

	// Now write to cache
	msg := message.NewMsgPut()
	msg.RetChan = here
	iopkt = msg.IoPkt()
	iopkt.Nblocks = nblocks
	iopkt.Offset = cacheoffset
	iopkt.Buffer = buffer
	c.Put(msg)

	<-here
}
