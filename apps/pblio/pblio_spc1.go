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
	"github.com/pblcache/pblcache/cache"
	"github.com/pblcache/pblcache/message"
	"os"
	"runtime/pprof"
	//"sync"
	"syscall"
	//"time"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
)

var (
	asu1, asu2, asu3        string
	cachefilename           string
	runtime, cachesize      int
	blocksize, contexts     int
	usedirectio, cpuprofile bool
)

func init() {
	flag.StringVar(&asu1, "asu1", "", "\n\tASU1 - Data Store")
	flag.StringVar(&asu2, "asu2", "", "\n\tASU2 - User Store")
	flag.StringVar(&asu3, "asu3", "", "\n\tLog")
	flag.StringVar(&cachefilename, "cache", "", "\n\tCache file name")
	flag.IntVar(&cachesize, "cachesize", 8, "\n\tCache size in GB")
	flag.IntVar(&runtime, "runtime", 300, "\n\tRuntime in seconds")
	flag.IntVar(&blocksize, "blocksize", 4, "\n\tCache block size in KB")
	flag.IntVar(&contexts, "contexts", 1, "\n\tNumber of contexts.  Each context runs its own SPC1 generator"+
		"\n\tEach context also has 8 streams.  Four(4) streams for ASU1, three(3)"+
		"\n\tfor ASU2, and one for ASU3. Values are set in spc1.c:188")
	flag.BoolVar(&usedirectio, "directio", true, "\n\tUse O_DIRECT on ASU files")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "\n\tCreate a Go cpu profile for analysis")
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

	c.Put(m)
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
	iopkt := &message.IoPkt{
		Offset:  offset,
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
	iopkt.Offset = offset
	iopkt.Buffer = buffer
	c.Put(msg)

	<-here
}

func read(fp *os.File,
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
		io.Offset = offset
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
			msgs++
			go readandstore(fp, c, be_offset, be_nblocks,
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
		/*
			fmt.Printf("|msgs:%d-%s", msgs, msg)
				switch msg.Type {
				case message.MsgGet:
					if msg.Err == nil {
						io := msg.IoPkt()
						msgs -= io.Nblocks
					}
				case message.MsgPut:
					if msg.Err == nil {
						msgs -= msg.IoPkt().Nblocks
					}
				}
		*/

		godbc.Check(msgs >= 0, msgs)

		if msgs == 0 {
			return
		}
	}
}

/*

func simluate(fp *os.File, c *cache.Cache) {

	// Get file size
	filestat, err := fp.Stat()
	var filesize uint64
	if err != nil {
		fmt.Println(err)
		return
	}

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
			z := zipf.NewZipfWorkload(fileblocks, reads)
			stop := time.After(time.Second * time.Duration(runtime))
			buffer := make([]byte, blocksize_bytes*256)

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
							read(fp, c, offset,
								blocksize_bytes, nblocks,
								buffer[0:uint64(nblocks)*blocksize_bytes])
						} else {
							fp.ReadAt(buffer[0:uint64(nblocks)*blocksize_bytes], int64(offset))
						}
						mbs <- nblocks
					} else {
						if c != nil {
							write(fp, c, offset,
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
*/

type Asu struct {
	fps         []*os.File
	len         uint32
	usedirectio bool
}

type SpcInfo struct {
	asus []*Asu
}

func NewAsu(usedirectio bool) *Asu {
	return &Asu{
		fps:         make([]*os.File, 0),
		usedirectio: usedirectio,
	}
}

func (a *Asu) Open(filename string) error {
	flags := os.O_RDWR | os.O_EXCL
	if a.usedirectio {
		flags |= syscall.O_DIRECT
	}
	fp, err := os.OpenFile(filename, flags, os.ModePerm)
	if err != nil {
		return err
	}

	filestat, err := fp.Stat()
	if err != nil {
		return err
	}

	a.fps = append(a.fps, fp)
	a.len += uint32(filestat.Size() / int64(4*KB))

	return nil
}

func NewSpcInfo(usedirectio bool) *SpcInfo {
	s := &SpcInfo{
		asus: make([]*Asu, 3),
	}

	s.asus[0] = NewAsu(usedirectio)
	s.asus[1] = NewAsu(usedirectio)
	s.asus[2] = NewAsu(usedirectio)

	return s
}

func (s *SpcInfo) Open(asu int, filename string) error {
	return s.asus[asu-1].Open(filename)
}

func main() {
	flag.Parse()

	if asu1 == "" ||
		asu2 == "" ||
		asu3 == "" {
		fmt.Print("ASU files must be set\n")
		return
	}

	// Initialize spc1info
	spcinfo := NewSpcInfo(usedirectio)

	// Open asus
	spcinfo.Open(1, asu1)
	spcinfo.Open(2, asu2)
	spcinfo.Open(3, asu3)

	// Start cpu profiling
	if cpuprofile {
		f, _ := os.Create("cpuprofile")
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	/*
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
				0, // buffer cache has been removed for now
				)

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
	*/
}
