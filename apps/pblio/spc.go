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
package pblio

type SpcInfo struct {
	asus     []*Asu
	pblcache *cache.Cache
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

func (s *SpcInfo) Open(asu int, filename string) error {
	return s.asus[asu-1].Open(filename)
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
