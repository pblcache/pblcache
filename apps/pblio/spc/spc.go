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
	"github.com/lpabon/goioworkload/spc1"
	"github.com/pblcache/pblcache/cache"
	"github.com/pblcache/pblcache/message"
	"os"
	"sync"
	"time"
)

const (
	ASUs = 3
	ASU1 = 0
	ASU2 = 1
	ASU3 = 2
)

type SpcInfo struct {
	asus      []*Asu
	pblcache  *cache.Cache
	blocksize int
}

func NewSpcInfo(c *cache.Cache,
	usedirectio bool,
	blocksize int) *SpcInfo {

	s := &SpcInfo{
		pblcache:  c,
		asus:      make([]*Asu, ASUs),
		blocksize: blocksize,
	}

	s.asus[ASU1] = NewAsu(usedirectio)
	s.asus[ASU2] = NewAsu(usedirectio)
	s.asus[ASU3] = NewAsu(usedirectio)

	return s
}

func readandstore(fp *os.File,
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

func write(fp *os.File,
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

func read(fp *os.File,
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

func (s *SpcInfo) sendio(wg *sync.WaitGroup,
	iostream <-chan *spc1.Spc1Io,
	iotime chan<- time.Duration) {
	defer wg.Done()

	buffer := make([]byte, 4*KB*64)
	for io := range iostream {
		start := time.Now()

		// Make sure the io is correct
		io.Invariant()
		if io.Asu == 3 {
			s.asus[ASU3].fps.WriteAt(
				buffer[0:io.Blocks*4*KB],
				int64(io.Offset)*int64(4*KB))
		} else {
			// Send the io
			if io.Isread {
				if s.pblcache == nil {
					s.asus[io.Asu-1].fps.ReadAt(buffer[0:io.Blocks*4*KB],
						int64(io.Offset)*int64(4*KB))
				} else {
					read(s.asus[io.Asu-1].fps,
						s.pblcache,
						uint16(io.Asu),
						uint64(io.Offset)*uint64(4*KB),
						uint64(s.blocksize*KB),
						int(io.Blocks),
						buffer[0:io.Blocks*4*KB])
				}
			} else {
				if s.pblcache == nil {
					s.asus[io.Asu-1].fps.WriteAt(buffer[0:io.Blocks*4*KB],
						int64(io.Offset)*int64(4*KB))
				} else {
					write(s.asus[io.Asu-1].fps,
						s.pblcache,
						uint16(io.Asu),
						uint64(io.Offset)*uint64(4*KB),
						uint64(s.blocksize*KB),
						int(io.Blocks),
						buffer[0:io.Blocks*4*KB])
				}
			}
		}

		// Report back the latency
		end := time.Now()
		iotime <- end.Sub(start)
	}
}

func (s *SpcInfo) Open(asu int, filename string) error {
	godbc.Require(asu > 0 && asu < 4, asu)

	return s.asus[asu-1].Open(filename)
}

// Must be called after all the ASUs are opened
func (s *SpcInfo) Spc1Init(bsu, contexts int) {

	godbc.Require(s.asus[0].len != 0)
	godbc.Require(s.asus[1].len != 0)
	godbc.Require(s.asus[2].len != 0)

	// Initialize Spc1 workload
	spc1.Spc1Init(bsu,
		contexts,
		s.asus[0].len,
		s.asus[1].len,
		s.asus[2].len)
}

func (s *SpcInfo) Context(wg *sync.WaitGroup,
	iotime chan<- time.Duration,
	runlen, context int) {

	defer wg.Done()

	// Spc generator specifies that each context have
	// 8 io streams.  Spc generator will specify which
	// io stream to use.
	streams := 8
	iostreams := make([]chan *spc1.Spc1Io, streams)

	var iostreamwg sync.WaitGroup
	for stream := 0; stream < streams; stream++ {
		iostreamwg.Add(1)
		iostreams[stream] = make(chan *spc1.Spc1Io, 32)
		go s.sendio(&iostreamwg, iostreams[stream], iotime)
	}

	start := time.Now()
	lastiotime := start
	stop := time.After(time.Second * time.Duration(runlen))
	ioloop := true
	for ioloop {
		select {
		case <-stop:
			ioloop = false
		default:
			// Get the next io
			s := spc1.NewSpc1Io(context)

			// There is some type of bug, where s.Generate()
			// sometimes does not return anything.  So we loop
			// here until it returns the next IO
			for s.Asu == 0 {
				err := s.Generate()
				godbc.Check(err == nil)
			}
			godbc.Invariant(s)

			// Check how much time we should wait
			sleep_time := start.Add(s.When).Sub(lastiotime)
			if sleep_time > 0 {
				time.Sleep(sleep_time)
			}

			// Send io to io stream
			iostreams[s.Stream] <- s

			lastiotime = time.Now()

		}
	}

	// close the streams for this context
	for stream := 0; stream < streams; stream++ {
		close(iostreams[stream])
	}
	iostreamwg.Wait()
}
