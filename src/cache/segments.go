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

/*
import (
	"flag"
	"fmt"
	"github.com/lpabon/buffercache"
	"github.com/lpabon/bufferio"
	"github.com/lpabon/foocsim/utils"
	"github.com/lpabon/godbc"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
	TB = 1024 * GB
)

// Command line
var fdirectio bool
var fsegmentbuffers int
var fsegmentsize int

func init() {
	// These values are set by the main program when it calls flag.Parse()
	flag.BoolVar(&fdirectio, "iodb_directio", false, "\n\tUse DIRECTIO in iodb")
	flag.IntVar(&fsegmentbuffers, "iodb_segmentbuffers", 32, "\n\tNumber of inflight buffers")
	flag.IntVar(&fsegmentsize, "iodb_segmentsize", 1024, "\n\tSegment size in KB")
}

type IoSegmentInfo struct {
	size         uint64
	metadatasize uint64
	datasize     uint64
}

type IoSegment struct {
	segmentbuf []byte
	data       *bufferio.BufferIO
	meta       *bufferio.BufferIO
	offset     uint64
	written    bool
	lock       sync.RWMutex
}

type IoStats struct {
	ramhits         uint64
	storagehits     uint64
	wraps           uint64
	seg_skipped     uint64
	bufferhits      uint64
	totalhits       uint64
	readtime        *utils.TimeDuration
	segmentreadtime *utils.TimeDuration
	writetime       *utils.TimeDuration
}

func NewIoStats() *IoStats {

	stats := &IoStats{}
	stats.readtime = &utils.TimeDuration{}
	stats.segmentreadtime = &utils.TimeDuration{}
	stats.writetime = &utils.TimeDuration{}

	return stats

}

func (s *IoStats) Close() {

}

func (s *IoStats) BufferHit() {
	s.bufferhits++
	s.totalhits++
}

func (s *IoStats) SegmentSkipped() {
	s.seg_skipped++
}

func (s *IoStats) RamHit() {
	s.ramhits++
	s.totalhits++
}

func (s *IoStats) StorageHit() {
	s.storagehits++
	s.totalhits++
}

func (s *IoStats) Wrapped() {
	s.wraps++
}

func (s *IoStats) ReadTimeRecord(d time.Duration) {
	s.readtime.Add(d)
}

func (s *IoStats) WriteTimeRecord(d time.Duration) {
	s.writetime.Add(d)
}

func (s *IoStats) SegmentReadTimeRecord(d time.Duration) {
	s.segmentreadtime.Add(d)
}

func (s *IoStats) RamHitRate() float64 {
	if 0 == s.totalhits {
		return 0.0
	} else {
		return float64(s.ramhits) / float64(s.totalhits)
	}
}

func (s *IoStats) BufferHitRate() float64 {
	if 0 == s.totalhits {
		return 0.0
	} else {
		return float64(s.bufferhits) / float64(s.totalhits)
	}
}

func (s *IoStats) String() string {
	return fmt.Sprintf("Ram Hit Rate: %.4f\n"+
		"Ram Hits: %v\n"+
		"Buffer Hit Rate: %.4f\n"+
		"Buffer Hits: %v\n"+
		"Storage Hits: %v\n"+
		"Wraps: %v\n"+
		"Segments Skipped: %v\n"+
		"Mean Read Latency: %.2f usec\n"+
		"Mean Segment Read Latency: %.2f usec\n"+
		"Mean Write Latency: %.2f usec\n",
		s.RamHitRate(),
		s.ramhits,
		s.BufferHitRate(),
		s.bufferhits,
		s.storagehits,
		s.wraps,
		s.seg_skipped,
		s.readtime.MeanTimeUsecs(),
		s.segmentreadtime.MeanTimeUsecs(),
		s.writetime.MeanTimeUsecs()) // + s.readtime.String() + s.writetime.String()
}

type KVIoDB struct {
	size           uint64
	blocksize      uint64
	segmentinfo    IoSegmentInfo
	segments       []IoSegment
	segment        *IoSegment
	chwriting      chan *IoSegment
	chreader       chan *IoSegment
	chavailable    chan *IoSegment
	wg             sync.WaitGroup
	segmentbuffers int
	current        uint64
	numsegments    uint64
	maxentries     uint64
	fp             *os.File
	wrapped        bool
	stats          *IoStats
	bc             buffercache.BufferCache
}

func NewKVIoDB(dbpath string, blocks, bcsize uint64, blocksize uint32) *KVIoDB {

	var err error

	db := &KVIoDB{}
	db.stats = NewIoStats()
	db.blocksize = uint64(blocksize)
	db.segmentinfo.metadatasize = 4 * KB
	db.segmentinfo.datasize = uint64(fsegmentsize) * KB
	db.segmentbuffers = fsegmentbuffers
	db.maxentries = db.segmentinfo.datasize / db.blocksize
	db.segmentinfo.size = db.segmentinfo.metadatasize + db.segmentinfo.datasize
	db.numsegments = blocks / db.maxentries
	db.size = db.numsegments * db.segmentinfo.size

	// Create buffer cache
	db.bc = buffercache.NewClockCache(bcsize, uint64(db.blocksize))

	// Segment channel state machine:
	// 		-> Client writes available segment
	// 		-> Segment written to storage
	// 		-> Segment read from storage
	// 		-> Segment available
	db.chwriting = make(chan *IoSegment, db.segmentbuffers)
	db.chavailable = make(chan *IoSegment, db.segmentbuffers)
	db.chreader = make(chan *IoSegment, db.segmentbuffers)

	// Set up each of the segments
	db.segments = make([]IoSegment, db.segmentbuffers)
	for i := 0; i < db.segmentbuffers; i++ {
		db.segments[i].segmentbuf = make([]byte, db.segmentinfo.size)
		db.segments[i].data = bufferio.NewBufferIO(db.segments[i].segmentbuf[:db.segmentinfo.datasize])
		db.segments[i].meta = bufferio.NewBufferIO(db.segments[i].segmentbuf[db.segmentinfo.datasize:])

		// Fill ch available with all the available buffers
		db.chavailable <- &db.segments[i]
	}

	// Set up the first available segment
	db.segment = <-db.chavailable

	// Open the storage device
	os.Remove(dbpath)

	// For DirectIO
	if fdirectio {
		db.fp, err = os.OpenFile(dbpath, syscall.O_DIRECT|os.O_CREATE|os.O_RDWR, os.ModePerm)
	} else {
		db.fp, err = os.OpenFile(dbpath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	}
	godbc.Check(err == nil)

	// Start goroutines
	db.writer()
	db.reader()

	godbc.Ensure(db.blocksize == uint64(blocksize))
	godbc.Ensure(db.chwriting != nil)
	godbc.Ensure(db.chavailable != nil)
	godbc.Ensure(db.chreader != nil)
	godbc.Ensure(db.segmentbuffers == len(db.segments))
	godbc.Ensure((db.segmentbuffers - 1) == len(db.chavailable))
	godbc.Ensure(0 == len(db.chreader))
	godbc.Ensure(0 == len(db.chwriting))
	godbc.Ensure(nil != db.segment)

	return db
}

func (c *KVIoDB) writer() {

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for s := range c.chwriting {
			if s.written {
				start := time.Now()
				n, err := c.fp.WriteAt(s.segmentbuf, int64(s.offset))
				end := time.Now()
				s.written = false

				c.stats.WriteTimeRecord(end.Sub(start))
				godbc.Check(n == len(s.segmentbuf))
				godbc.Check(err == nil)
			} else {
				c.stats.SegmentSkipped()
			}
			c.chreader <- s
		}
		close(c.chreader)
	}()

}

func (c *KVIoDB) reader() {

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for s := range c.chreader {
			s.lock.Lock()

			// Reset the bufferIO managers
			s.data.Reset()
			s.meta.Reset()

			// Move to the next offset
			c.current += c.segmentinfo.size
			c.current = c.current % c.size

			if 0 == c.current {
				c.stats.Wrapped()
				c.wrapped = true
			}
			s.offset = c.current

			if c.wrapped {
				start := time.Now()
				n, err := c.fp.ReadAt(s.segmentbuf, int64(s.offset))
				end := time.Now()
				c.stats.SegmentReadTimeRecord(end.Sub(start))
				godbc.Check(n == len(s.segmentbuf))
				godbc.Check(err == nil)
			}

			s.lock.Unlock()

			c.chavailable <- s
		}
	}()

}

func (c *KVIoDB) sync() {
	// Send to writer
	c.chwriting <- c.segment

	// Get a new available buffer
	c.segment = <-c.chavailable
}

func (c *KVIoDB) Close() {
	c.sync()
	close(c.chwriting)
	c.wg.Wait()
	c.fp.Close()
}

func (c *KVIoDB) offset(index uint64) uint64 {
	return (index*c.blocksize + (index/c.maxentries)*c.segmentinfo.metadatasize)
}

func (c *KVIoDB) inRange(index uint64, s *IoSegment) bool {
	offset := c.offset(index)

	return ((offset >= s.offset) &&
		(offset < (s.offset + c.segmentinfo.datasize)))
}

func (c *KVIoDB) Put(key, val []byte, index uint64) error {

	for !c.inRange(index, c.segment) {
		c.sync()
	}

	offset := c.offset(index)

	godbc.Require(c.inRange(index, c.segment),
		fmt.Sprintf("[%v - %v - %v]",
			c.segment.offset,
			offset,
			c.segment.offset+c.segmentinfo.datasize))

	// Buffer cache is a Read-miss cache
	c.bc.Invalidate(index)

	n, err := c.segment.data.WriteAt(val, int64(offset-c.segment.offset))
	godbc.Check(n == len(val))
	godbc.Check(err == nil)

	c.segment.written = true
	c.segment.meta.Write([]byte(key))

	return nil
}

func (c *KVIoDB) Get(key, val []byte, index uint64) error {

	var n int
	var err error

	offset := c.offset(index)

	err = c.bc.Get(index, val)
	if err == nil {
		c.stats.BufferHit()
		return nil
	}

	// Check if the data is in RAM.  Go through each buffered segment
	for i := 0; i < c.segmentbuffers; i++ {

		c.segments[i].lock.RLock()

		if (offset >= c.segments[i].offset) &&
			(offset < (c.segments[i].offset + c.segmentinfo.datasize)) {

			n, err = c.segments[i].data.ReadAt(val, int64(offset-c.segments[i].offset))

			godbc.Check(err == nil)
			godbc.Check(uint64(n) == c.blocksize,
				fmt.Sprintf("Read %v expected:%v from location:%v index:%v",
					n, c.blocksize, offset, index))
			c.stats.RamHit()

			c.segments[i].lock.RUnlock()

			// Save in buffer cache
			c.bc.Set(index, val)

			return nil
		}

		c.segments[i].lock.RUnlock()
	}

	// Read from storage
	start := time.Now()
	n, err = c.fp.ReadAt(val, int64(offset))
	end := time.Now()
	c.stats.ReadTimeRecord(end.Sub(start))

	godbc.Check(uint64(n) == c.blocksize,
		fmt.Sprintf("Read %v expected %v from location %v index %v",
			n, c.blocksize, offset, index))
	godbc.Check(err == nil)
	c.stats.StorageHit()

	// Save in buffer cache
	c.bc.Set(index, val)

	return nil
}

func (c *KVIoDB) Delete(key []byte, index uint64) error {
	// nothing to do
	return nil
}

func (c *KVIoDB) String() string {
	return fmt.Sprintf(
		"== IoDB Information ==\n") +
		c.stats.String()
}

*/
