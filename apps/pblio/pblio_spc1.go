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
	"github.com/lpabon/goioworkload/spc1"
	"github.com/lpabon/tm"
	"github.com/pblcache/pblcache/cache"
	"github.com/pblcache/pblcache/message"
	"os"
	"runtime/pprof"
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
	asu1, asu2, asu3        string
	cachefilename           string
	runlen, cachesize       int
	blocksize, contexts     int
	bsu                     int
	usedirectio, cpuprofile bool
)

func init() {
	flag.StringVar(&asu1, "asu1", "", "\n\tASU1 - Data Store")
	flag.StringVar(&asu2, "asu2", "", "\n\tASU2 - User Store")
	flag.StringVar(&asu3, "asu3", "", "\n\tLog")
	flag.StringVar(&cachefilename, "cache", "", "\n\tCache file name")
	flag.IntVar(&bsu, "bsu", 50, "\n\tNumber of BSUs (Business Scaling Units)."+
		"\n\tEach BSU requires 50 IOPs from the back end storage")
	flag.IntVar(&cachesize, "cachesize", 8, "\n\tCache size in GB")
	flag.IntVar(&runlen, "runlen", 300, "\n\tBenchmark run time length in seconds")
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
		godbc.Check(msgs >= 0, msgs)

		if msgs == 0 {
			return
		}
	}
}

type Asu struct {
	fps         *os.File
	len         uint32
	usedirectio bool
}

type SpcInfo struct {
	asus     []*Asu
	pblcache *cache.Cache
}

func NewAsu(usedirectio bool) *Asu {
	return &Asu{
		usedirectio: usedirectio,
	}
}

func (a *Asu) Open(filename string) error {
	var err error

	godbc.Require(filename != "")

	flags := os.O_RDWR | os.O_EXCL
	if a.usedirectio {
		flags |= syscall.O_DIRECT
	}

	a.fps, err = os.OpenFile(filename, flags, os.ModePerm)
	if err != nil {
		return err
	}

	filestat, err := a.fps.Stat()
	if err != nil {
		return err
	}

	// Length in 4KB blocks
	a.len = uint32(filestat.Size() / int64(4*KB))

	godbc.Ensure(a.fps != nil, a.fps)
	godbc.Ensure(a.len > 0, a.len)

	return nil
}

func NewSpcInfo(c *cache.Cache, usedirectio bool) *SpcInfo {
	s := &SpcInfo{
		pblcache: c,
		asus:     make([]*Asu, 3),
	}

	s.asus[0] = NewAsu(usedirectio)
	s.asus[1] = NewAsu(usedirectio)
	s.asus[2] = NewAsu(usedirectio)

	return s
}

func (s *SpcInfo) Open(asu int, filename string) error {
	return s.asus[asu-1].Open(filename)
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

		// Send the io
		if io.Isread {
			if s.pblcache == nil {
				s.asus[io.Asu-1].fps.ReadAt(buffer[0:io.Blocks*4*KB],
					int64(io.Offset)*int64(4*KB))
			} else {
				read(s.asus[io.Asu-1].fps,
					s.pblcache,
					uint64(io.Offset)*uint64(4*KB),
					uint64(blocksize*KB),
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
					uint64(io.Offset)*uint64(4*KB),
					uint64(blocksize*KB),
					int(io.Blocks),
					buffer[0:io.Blocks*4*KB])
			}
		}

		// Report back the latency
		end := time.Now()
		iotime <- end.Sub(start)
	}
}

func (s *SpcInfo) Context(wg *sync.WaitGroup,
	iotime chan<- time.Duration,
	stop <-chan time.Time,
	context int) {

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

func main() {
	flag.Parse()

	if asu1 == "" ||
		asu2 == "" ||
		asu3 == "" {
		fmt.Print("ASU files must be set\n")
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
			0, // buffer cache has been removed for now
		)

		// Connect cache metadata with log
		c = cache.NewCache(logblocks, blocksize_bytes, log.Msgchan)
	} else {
		fmt.Println("No cache set")
	}

	// Initialize spc1info
	spcinfo := NewSpcInfo(c, usedirectio)

	// Open asus
	var err error
	err = spcinfo.Open(1, asu1)
	if err != nil {
		fmt.Print(err)
		return
	}
	err = spcinfo.Open(2, asu2)
	if err != nil {
		fmt.Print(err)
		return
	}
	err = spcinfo.Open(3, asu3)
	if err != nil {
		fmt.Print(err)
		return
	}

	// Start cpu profiling
	if cpuprofile {
		f, _ := os.Create("cpuprofile")
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Initialize Spc1 workload
	spc1.Spc1Init(bsu,
		contexts,
		spcinfo.asus[0].len,
		spcinfo.asus[1].len,
		spcinfo.asus[2].len)

	// This channel will be used for the io to return
	// the latency
	iotime := make(chan time.Duration, 64)

	// Spawn contexts coroutines
	var wg sync.WaitGroup
	stop := time.After(time.Second * time.Duration(runlen))
	for context := 1; context <= contexts; context++ {
		wg.Add(1)
		go spcinfo.Context(&wg, iotime, stop, context)
	}

	// This goroutine will be used to collect the data
	// from the io routines and print out to the console
	// every few seconds
	var outputwg sync.WaitGroup
	outputwg.Add(1)
	go func() {
		defer outputwg.Done()

		ios := uint64(0)
		start := time.Now()
		latency_mean := tm.TimeDuration{}
		print_iops := time.After(time.Second * 2)

		for latency := range iotime {

			// Save the number of ios being sent
			// and their latency
			ios++
			latency_mean.Add(latency)

			// Do this every few seconds
			select {
			case <-print_iops:
				end := time.Now()
				iops := float64(ios) / end.Sub(start).Seconds()
				fmt.Printf("ios:%v IOPS:%.2f Latency:%.4f ms"+
					"                                   \r",
					ios, iops, latency_mean.MeanTimeUsecs()/1000)

				// Clear the latency
				latency_mean = tm.TimeDuration{}

				// Set the timer for the next time
				print_iops = time.After(time.Second * 2)
			default:
			}
		}

		end := time.Now()
		iops := float64(ios) / end.Sub(start).Seconds()
		fmt.Printf("ios:%v IOPS:%.2f Latency:%.4f ms",
			ios, iops, latency_mean.MeanTimeUsecs()/1000)

		fmt.Print("\n")
	}()

	// Wait here for all the context goroutines to finish
	wg.Wait()

	// Now we can close the output goroutine
	close(iotime)
	outputwg.Wait()

	// Print cache stats
	if c != nil {
		c.Close()
		log.Close()
		fmt.Print(c)
		fmt.Print(log)
	} else {
		fmt.Println("No cache stats")
	}

}
