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
	"fmt"
	"github.com/lpabon/godbc"
	"github.com/lpabon/goioworkload/spc1"
	"github.com/pblcache/pblcache/cache"
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

func (s *SpcInfo) sendio(wg *sync.WaitGroup,
	iostream <-chan *IoStats,
	iotime chan<- *IoStats) {
	defer wg.Done()

	buffer := make([]byte, 4*KB*64)
	for iostat := range iostream {

		// Make sure the io is correct
		io := iostat.Io
		godbc.Invariant(io)
		if io.Asu == 3 {
			s.asus[ASU3].WriteAt(
				buffer[0:io.Blocks*4*KB],
				int64(io.Offset)*int64(4*KB))
		} else {
			// Send the io
			if io.Isread {
				if s.pblcache == nil {
					s.asus[io.Asu-1].ReadAt(buffer[0:io.Blocks*4*KB],
						int64(io.Offset)*int64(4*KB))
				} else {
					read(s.asus[io.Asu-1],
						s.pblcache,
						uint16(io.Asu),
						uint64(io.Offset)*uint64(4*KB),
						uint64(s.blocksize*KB),
						int(io.Blocks),
						buffer[0:io.Blocks*4*KB])
				}
			} else {
				if s.pblcache == nil {
					s.asus[io.Asu-1].WriteAt(buffer[0:io.Blocks*4*KB],
						int64(io.Offset)*int64(4*KB))
				} else {
					write(s.asus[io.Asu-1],
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
		iostat.Latency = time.Now().Sub(iostat.Start)
		iotime <- iostat
	}
}

// Add a file to the specified ASU and open for reading
// and writing.  Can be called multiple times to add many
// files to a specified ASU
func (s *SpcInfo) Open(asu int, filename string) error {
	godbc.Require(asu > 0 && asu < 4, asu)

	return s.asus[asu-1].Open(filename)
}

// ASU1 + ASU2 + ASU3 = X
// ASU1 is 45% of X
// ASU2 is 45% of X
// ASU3 is 10% of X
// Call this function after all ASUs are opened
func (s *SpcInfo) adjustAsuSizes() error {
	godbc.Require(s.asus[ASU1].len != 0)
	godbc.Require(s.asus[ASU2].len != 0)
	godbc.Require(s.asus[ASU3].len != 0)

	// lets start making user ASU1 and ASU2 are equal
	if s.asus[ASU1].len > s.asus[ASU2].len {
		s.asus[ASU1].len = s.asus[ASU2].len
	} else {
		s.asus[ASU2].len = s.asus[ASU1].len
	}

	// Now we need to adjust ASU3
	asu3_correct_size := uint32(float64(2*s.asus[ASU1].len) / 9)
	if asu3_correct_size > s.asus[ASU3].len {
		return fmt.Errorf("\nASU3 size is too small: %v KB.\n"+
			"It must be bigger than 1/9 of 2*ASU1,\n"+
			"or %v KB for this configuration\n",
			s.asus[ASU3].len*4, asu3_correct_size*4)
	} else {
		s.asus[ASU3].len = asu3_correct_size
	}

	godbc.Ensure(s.asus[ASU1].len != 0)
	godbc.Ensure(s.asus[ASU2].len != 0)
	godbc.Ensure(s.asus[ASU3].len != 0, asu3_correct_size)

	return nil
}

// Size in GB
func (s *SpcInfo) Size(asu int) float64 {
	godbc.Require(asu > 0 && asu < 4, asu)

	return s.asus[asu-1].Size()
}

// Must be called after all the ASUs are opened
func (s *SpcInfo) Spc1Init(bsu, contexts int) error {
	godbc.Require(s.asus[ASU1].len != 0)
	godbc.Require(s.asus[ASU2].len != 0)
	godbc.Require(s.asus[ASU3].len != 0)

	// Adjust sizes
	err := s.adjustAsuSizes()
	if err != nil {
		return err
	}

	// Initialize Spc1 workload
	spc1.Spc1Init(bsu,
		contexts,
		s.asus[ASU1].len,
		s.asus[ASU2].len,
		s.asus[ASU3].len)

	return nil
}

// Use as a goroutine to start the io workload
// Create one of these per context set on Spc1Init()
func (s *SpcInfo) Context(wg *sync.WaitGroup,
	iotime chan<- *IoStats,
	quit <-chan struct{},
	runlen, context int) {

	defer wg.Done()

	// Spc generator specifies that each context have
	// 8 io streams.  Spc generator will specify which
	// io stream to use.
	streams := 8
	iostreams := make([]chan *IoStats, streams)

	var iostreamwg sync.WaitGroup
	for stream := 0; stream < streams; stream++ {

		// Allow for queued requests
		iostreams[stream] = make(chan *IoStats, 64)

		// Create 32 io contexts per stream
		for i := 0; i < 32; i++ {
			iostreamwg.Add(1)
			go s.sendio(&iostreamwg, iostreams[stream], iotime)
		}
	}

	start := time.Now()
	lastiotime := start
	stop := time.After(time.Second * time.Duration(runlen))
	ioloop := true
	for ioloop {
		select {
		case <-quit:
			ioloop = false
		case <-stop:
			ioloop = false
		default:
			// Get the next io
			io := spc1.NewSpc1Io(context)

			err := io.Generate()
			godbc.Check(err == nil)
			godbc.Invariant(io)

			// Check how much time we should wait
			sleep_time := start.Add(io.When).Sub(lastiotime)
			if sleep_time > 0 {
				time.Sleep(sleep_time)
			}

			// Send io to io stream
			iostreams[io.Stream] <- &IoStats{
				Io:    io,
				Start: time.Now(),
			}

			lastiotime = time.Now()

		}
	}

	// close the streams for this context
	for stream := 0; stream < streams; stream++ {
		close(iostreams[stream])
	}
	iostreamwg.Wait()
}

// Close all spc files
func (s *SpcInfo) Close() {
	for _, asu := range s.asus {
		asu.Close()
	}
}
