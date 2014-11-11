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

package main

import (
	"flag"
	"fmt"
	"github.com/lpabon/godbc"
	zipf "github.com/lpabon/zipfworkload"
	"github.com/pblcache/pblcache/cache"
	"github.com/pblcache/pblcache/message"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
)

var (
	filename, cachefilename string
	runtime, cachesize      int
	blocksize, iogenerators int
	reads                   int
	usedirectio             bool

	// From spc1.c NetApp (BSD-lic)
	/*
		SMIX_LENGTHS = []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
			2, 2, 2, 2, 2, 2,
			4, 4, 4, 4, 4,
			8, 8,
			16, 16,
			32,
			64,
		}
	*/
	SMIX_LENGTHS = []int{16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
		8, 8, 8, 8, 8, 8,
		4, 4, 4, 4, 4,
		2, 2,
		1, 1,
		32, 32,
		64, 64,
		128,
		256,
	}
)

func init() {
	flag.StringVar(&filename, "filename", "", "\n\tStorage back end file to read and write")
	flag.StringVar(&cachefilename, "cache", "", "\n\tCache file name")
	flag.IntVar(&cachesize, "cachesize", 8, "\n\tCache size in GB")
	flag.IntVar(&runtime, "runtime", 300, "\n\tRuntime in seconds")
	flag.IntVar(&blocksize, "blocksize", 4, "\n\tCache block size in KB")
	flag.IntVar(&iogenerators, "iogenerators", 64, "\n\tNumber of io generators")
	flag.IntVar(&reads, "reads", 65, "\n\tRead percentage (0-100)")
	flag.BoolVar(&usedirectio, "directio", true, "\n\tUse O_DIRECT on filename")
}

func smix(r *rand.Rand) int {
	return SMIX_LENGTHS[r.Intn(len(SMIX_LENGTHS))]
}

func readandstore(fp *os.File,
	c *cache.Cache,
	offset uint64,
	nblocks uint64,
	buffer []byte,
	retchan chan *message.Message) {

	fp.ReadAt(buffer, int64(offset))

	m := message.NewMsgPut()
	m.RetChan = retchan
	io := m.IoPkt()
	io.Offset = offset
	io.Buffer = buffer
	io.Nblocks = int(nblocks)

	c.Msgchan <- m
}

func write2(fp *os.File,
	c *cache.Cache,
	offset, blocksize_bytes uint64,
	nblocks int,
	buffer []byte) {

	godbc.Require(len(buffer)%(4*KB) == 0)
	godbc.Require(blocksize_bytes%(4*KB) == 0)

	here := make(chan *message.Message, nblocks)

	// Send invalidates for each block
	msg := message.NewMsgInvalidate()
	msg.RetChan = here
	iopkt := msg.IoPkt()
	iopkt.Offset = offset
	iopkt.Nblocks = nblocks

	c.Msgchan <- msg

	// Write to storage back end
	// :TODO: check return status
	fp.WriteAt(buffer, int64(offset))

	// Invalidations should have been done
	// by now.  Let's make sure.
	<-here

	// Now write to cache
	msg = message.NewMsgPut()
	msg.RetChan = here
	iopkt = msg.IoPkt()
	iopkt.Nblocks = nblocks
	iopkt.Offset = offset
	iopkt.Buffer = buffer
	c.Msgchan <- msg

	msgs := nblocks
	for msg = range here {
		godbc.Check(msg.Type == message.MsgPut)

		if msg.Err == nil {
			iopkt := msg.IoPkt()
			msgs -= iopkt.Nblocks
		}

		if msgs == 0 {
			return
		}
	}
}

func write(fp *os.File,
	c *cache.Cache,
	offset, blocksize_bytes uint64,
	nblocks int,
	buffer []byte) {

	godbc.Require(len(buffer)%(4*KB) == 0)
	godbc.Require(blocksize_bytes%(4*KB) == 0)

	here := make(chan *message.Message, nblocks)

	// Send invalidates for each block
	msgs := nblocks
	for block := 0; block < nblocks; block++ {
		msg := message.NewMsgInvalidate()
		msg.RetChan = here

		buffer_offset := uint64(block) * blocksize_bytes
		current_offset := offset + buffer_offset
		iopkt := msg.IoPkt()
		iopkt.Offset = current_offset

		msg.TimeStart()
		c.Msgchan <- msg
	}

	for m := range here {
		if false {
			// Add stats
			m.TimeElapsed()
		}

		msgs--
		if msgs == 0 {
			break
		}
	}

	// Write to storage back end
	// :TODO: check return status
	fp.WriteAt(buffer, int64(offset))

	// Now write to cache
	msgs = nblocks
	for block := 0; block < nblocks; block++ {
		msg := message.NewMsgPut()
		msg.RetChan = here

		buffer_offset := uint64(block) * blocksize_bytes
		current_offset := offset + buffer_offset
		iopkt := msg.IoPkt()
		iopkt.Offset = current_offset
		iopkt.Buffer = buffer[buffer_offset:(buffer_offset + blocksize_bytes)]
		godbc.Check(len(iopkt.Buffer)%4*KB == 0)
		msg.Priv = block
		c.Msgchan <- msg
	}

	for m := range here {
		if false {
			// Add stats
			m.TimeElapsed()
		}

		msgs--
		if msgs == 0 {
			break
		}
	}

}

func read2(fp *os.File,
	c *cache.Cache,
	offset, blocksize_bytes uint64,
	nblocks int,
	buffer []byte) {

	godbc.Require(len(buffer)%(4*KB) == 0)
	godbc.Require(blocksize_bytes%(4*KB) == 0)

	here := make(chan *message.Message, nblocks)
	msg := message.NewMsgGet()
	msg.RetChan = here
	iopkt := msg.IoPkt()
	iopkt.Buffer = buffer
	iopkt.Offset = offset
	iopkt.Nblocks = nblocks

	hitpkt, err := c.Get(msg)
	if err != nil {
		// None found
		// Read the whole thing from backend
		fp.ReadAt(buffer, int64(offset))

		m := message.NewMsgPut()
		m.RetChan = here

		io := m.IoPkt()
		io.Offset = offset
		io.Buffer = buffer
		io.Nblocks = nblocks
		c.Msgchan <- m

	} else if hitpkt.Hits != nblocks {
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
					go readandstore(fp, c, be_offset, be_nblocks,
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
			go readandstore(fp, c, be_offset, be_nblocks,
				buffer[buffer_offset:(buffer_offset+blocksize_bytes)],
				here)

		}

	}

	// Wait for blocks to be returned
	msgs := nblocks
	for msg := range here {

		switch msg.Type {
		case message.MsgGet:
			if msg.Err == nil {
				io := msg.IoPkt()
				msgs -= io.Nblocks
			}
		case message.MsgPut:
			if msg.Err == nil {
				io := msg.IoPkt()
				msgs -= io.Nblocks
			}
		}

		if msgs == 0 {
			return
		}
	}
}

func read(fp *os.File,
	c *cache.Cache,
	offset, blocksize_bytes uint64,
	nblocks int,
	buffer []byte) {

	godbc.Require(len(buffer)%(4*KB) == 0)
	godbc.Require(blocksize_bytes%(4*KB) == 0)

	hitmap := make([]bool, nblocks)
	here := make(chan *message.Message, nblocks)

	msgs := nblocks
	for block := 0; block < nblocks; block++ {
		msg := message.NewMsgGet()
		msg.RetChan = here

		buffer_offset := uint64(block) * blocksize_bytes
		current_offset := offset + buffer_offset
		iopkt := msg.IoPkt()
		iopkt.Offset = current_offset
		iopkt.Buffer = buffer[buffer_offset:(buffer_offset + blocksize_bytes)]
		godbc.Check(len(iopkt.Buffer)%4*KB == 0)
		msg.Priv = block
		c.Msgchan <- msg
	}

	anyhit := false
	for m := range here {
		msgs--
		if m.Err == nil {
			anyhit = true
			block := m.Priv.(int)
			hitmap[block] = true
		}
		if msgs == 0 {
			break
		}
	}
	anyhit = false

	if anyhit == false {
		// Read the whole thing from backend
		fp.ReadAt(buffer, int64(offset))

		/*
			// Insert each one into cache
			msgs = nblocks
			for block := 0; block < nblocks; block++ {
				msg := message.NewMsgPut()
				msg.RetChan = here

				buffer_offset := uint64(block) * blocksize_bytes
				current_offset := offset + buffer_offset
				iopkt := msg.IoPkt()
				iopkt.Offset = current_offset
				iopkt.Buffer = buffer[buffer_offset:(buffer_offset + blocksize_bytes)]
				godbc.Check(len(iopkt.Buffer)%(4*KB) == 0)
				msg.Priv = block
				c.Msgchan <- msg
			}

			for m := range here {
				msgs--
				if msgs == 0 {
					break
				}
				if m.Err != nil {
					fmt.Printf("ERROR inserting to cache\n")
				}
			}
		*/

	} else {
		var wg sync.WaitGroup
		for block := 0; block < nblocks; block++ {
			if !hitmap[block] {

				wg.Add(1)
				go func(block int) {
					defer wg.Done()

					buffer_offset := uint64(block) * blocksize_bytes
					current_offset := offset + buffer_offset
					b := buffer[buffer_offset:(buffer_offset + blocksize_bytes)]
					godbc.Check(len(b)%(4*KB) == 0)
					fp.ReadAt(b, int64(current_offset))
					cache_sput(c, current_offset, b)
				}(block)

			}
		}
		wg.Wait()
	}

}

func cache_sput(c *cache.Cache,
	offset uint64,
	buffer []byte) error {

	if c != nil {
		here := make(chan *message.Message)
		msg := message.NewMsgPut()
		msg.RetChan = here
		iopkt := msg.IoPkt()
		iopkt.Offset = offset
		iopkt.Buffer = buffer
		c.Msgchan <- msg
		<-here

		//fmt.Print("p")
		return msg.Err
	} else {
		return nil
	}
}

func cache_sinval(c *cache.Cache,
	offset uint64) error {

	if c != nil {
		here := make(chan *message.Message)
		msg := message.NewMsgInvalidate()
		msg.RetChan = here
		iopkt := msg.IoPkt()
		iopkt.Offset = offset
		c.Msgchan <- msg
		<-here

		//fmt.Print("i")
		return msg.Err
	} else {
		return nil
	}
}

func cache_sget(c *cache.Cache, offset uint64, buffer []byte) error {

	if c != nil {
		here := make(chan *message.Message)
		msg := message.NewMsgGet()
		msg.RetChan = here
		iopkt := msg.IoPkt()
		iopkt.Offset = offset
		iopkt.Buffer = buffer
		c.Msgchan <- msg
		<-here

		//fmt.Print("g")
		return msg.Err
	} else {
		return cache.ErrNotFound
	}
}

func simluate(fp *os.File, c *cache.Cache) {

	// Get file size
	filestat, err := fp.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}
	filesize := uint64(filestat.Size())

	// Setup number of blocks
	blocksize_bytes := uint64(blocksize * KB)
	fileblocks := uint64(filesize / blocksize_bytes)
	mbs := make(chan int, iogenerators)

	// Start timer
	start := time.Now()

	// Start IO generators
	var wg sync.WaitGroup
	for gen := 0; gen < iogenerators; gen++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			z := zipf.NewZipfWorkload(fileblocks, reads /* read % */)
			stop := time.After(time.Second * time.Duration(runtime))
			buffer := make([]byte, blocksize_bytes*256 /*max in smix*/)

			for {
				select {
				case <-stop:
					return
				default:
					block, isread := z.ZipfGenerate()
					offset := block * blocksize_bytes
					nblocks := smix(r)

					if isread {
						if c != nil {
							read2(fp, c, offset,
								blocksize_bytes, nblocks,
								buffer[0:uint64(nblocks)*blocksize_bytes])
						} else {
							fp.ReadAt(buffer[0:uint64(nblocks)*blocksize_bytes], int64(offset))
						}
						mbs <- nblocks
					} else {
						if c != nil {
							write2(fp, c, offset,
								blocksize_bytes, nblocks,
								buffer[0:uint64(nblocks)*blocksize_bytes])
						} else {
							fp.WriteAt(buffer[0:uint64(nblocks)*blocksize_bytes], int64(offset))
						}
						mbs <- nblocks
					}
				}
			}
		}()
	}

	var mbswg sync.WaitGroup
	mbswg.Add(1)
	go func() {
		defer mbswg.Done()
		var num_blocks int

		for ioblocks := range mbs {
			num_blocks += ioblocks
		}
		total_duration := time.Now().Sub(start)

		fmt.Printf("Bandwidth: %.1f MB/s\n",
			((float64(blocksize_bytes)*
				float64(num_blocks))/(1024.0*1024.0))/
				float64(total_duration.Seconds()))
		fmt.Printf("Seconds: %.2f\n", total_duration.Seconds())
		fmt.Printf("IO blocks: %d\n", num_blocks)
	}()

	wg.Wait()
	close(mbs)
	mbswg.Wait()
}

func main() {
	flag.Parse()

	if filename == "" {
		fmt.Print("filename must be set\n")
		return
	}

	if (0 > reads) || (reads > 100) {
		fmt.Printf("Invalid value for reads")
		return
	}

	// Open file
	var fp *os.File
	var err error
	if usedirectio {
		fp, err = os.OpenFile(filename, syscall.O_DIRECT|os.O_RDWR|os.O_EXCL, os.ModePerm)
	} else {
		fp, err = os.OpenFile(filename, os.O_RDWR|os.O_EXCL, os.ModePerm)
	}
	if err != nil {
		fmt.Println(err)
		return
	}

	// Setup number of blocks
	blocksize_bytes := uint64(blocksize * KB)

	// Open cache
	var c *cache.Cache
	var log *cache.Log
	var logblocks uint64

	// Determine if we need to use the cache
	if cachefilename != "" {
		fmt.Printf("Using %s as the cache\n", cachefilename)

		// Create log
		log, logblocks = cache.NewLog(cachefilename,
			uint64(cachesize*GB)/blocksize_bytes,
			blocksize_bytes,
			(512*KB)/blocksize_bytes,
			0 /* buffer cache has been removed for now */)

		// Connect cache metadata with log
		c = cache.NewCache(logblocks, blocksize_bytes, log.Msgchan)
	} else {
		fmt.Println("No cache set")
	}

	if c != nil {
		fmt.Println("Cache warmup")
		simluate(fp, c)

		c.StatsClear()
		fmt.Println("Running simulation...")
		simluate(fp, c)
		c.Close()
		log.Close()
		fmt.Print(c)
		fmt.Print(log)
	} else {
		simluate(fp, nil)
	}
}
