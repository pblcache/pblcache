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
	"fmt"
	"github.com/lpabon/buffercache"
	"github.com/lpabon/bufferio"
	"github.com/lpabon/godbc"
	"github.com/lpabon/tm"
	"github.com/pblcache/pblcache/src/message"
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

	fdirectio       = false
	fsegmentbuffers = 32
	fsegmentsize    = 1024
)

/*
func init() {
	// These values are set by the main program when it calls flag.Parse()
	flag.BoolVar(&fdirectio, "iodb_directio", false, "\n\tUse DIRECTIO in iodb")
	flag.IntVar(&fsegmentbuffers, "iodb_segmentbuffers", 32, "\n\tNumber of inflight buffers")
	flag.IntVar(&fsegmentsize, "iodb_segmentsize", 1024, "\n\tSegment size in KB")
}
*/

type IoSegment struct {
	segmentbuf []byte
	data       *bufferio.BufferIO
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
	readtime        *tm.TimeDuration
	segmentreadtime *tm.TimeDuration
	writetime       *tm.TimeDuration
}

func NewIoStats() *IoStats {

	stats := &IoStats{}
	stats.readtime = &tm.TimeDuration{}
	stats.segmentreadtime = &tm.TimeDuration{}
	stats.writetime = &tm.TimeDuration{}

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

type Log struct {
	size           uint64
	blocksize      uint64
	segmentsize    uint64
	numsegments    uint64
	blocks         uint64
	segments       []IoSegment
	segment        *IoSegment
	segmentbuffers int
	chwriting      chan *IoSegment
	chreader       chan *IoSegment
	chavailable    chan *IoSegment
	wg             sync.WaitGroup
	current        uint64
	maxentries     uint64
	fp             *os.File
	wrapped        bool
	stats          *IoStats
	bc             buffercache.BufferCache
	Msgchan        chan *message.Message
	quitchan       chan struct{}
	logreaders     chan *message.Message
}

func NewLog(dbpath string, blocks, blocksize, blocks_per_segment, bcsize uint64) (*Log, uint64) {

	var err error

	db := &Log{}
	db.stats = NewIoStats()
	db.blocksize = blocksize
	db.segmentsize = blocks_per_segment * blocksize
	db.maxentries = db.segmentsize / db.blocksize

	// We have to make sure that the number of blocks requested
	// fit into the segments tracked by the log
	db.numsegments = blocks / db.maxentries
	db.blocks = db.numsegments * db.maxentries
	db.size = db.numsegments * db.segmentsize

	if db.numsegments < fsegmentbuffers {
		db.segmentbuffers = int(db.numsegments)
	} else {
		db.segmentbuffers = fsegmentbuffers
	}
	godbc.Check(db.numsegments != 0,
		fmt.Sprintf("bs:%v ssize:%v sbuffers:%v blocks:%v max:%v ns:%v size:%v\n",
			db.blocksize, db.segmentsize, db.segmentbuffers, db.blocks,
			db.maxentries, db.numsegments, db.size))

	// Create buffer cache
	db.bc = buffercache.NewClockCache(bcsize, uint64(db.blocksize))

	// Incoming message channel
	db.Msgchan = make(chan *message.Message, 32)
	db.quitchan = make(chan struct{})
	db.logreaders = make(chan *message.Message, 32)

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
		db.segments[i].segmentbuf = make([]byte, db.segmentsize)
		db.segments[i].data = bufferio.NewBufferIO(db.segments[i].segmentbuf)

		// Fill ch available with all the available buffers
		db.chreader <- &db.segments[i]
	}

	// Set up the first available segment
	db.segment = <-db.chreader

	// Open the storage device
	os.Remove(dbpath)

	// For DirectIO
	if fdirectio {
		db.fp, err = os.OpenFile(dbpath, syscall.O_DIRECT|os.O_CREATE|os.O_RDWR, os.ModePerm)
	} else {
		db.fp, err = os.OpenFile(dbpath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	}
	godbc.Check(err == nil)

	// Start reader goroutines
	for i := 0; i < 32; i++ {
		db.wg.Add(1)
		go db.logread()
	}

	// Start goroutines
	db.server()
	db.writer()
	db.reader()

	godbc.Ensure(db.size != 0)
	godbc.Ensure(db.blocksize == uint64(blocksize))
	godbc.Ensure(db.Msgchan != nil)
	godbc.Ensure(db.chwriting != nil)
	godbc.Ensure(db.chavailable != nil)
	godbc.Ensure(db.chreader != nil)
	godbc.Ensure(db.segmentbuffers == len(db.segments))
	godbc.Ensure(db.segmentbuffers-1 == len(db.chreader))
	godbc.Ensure(0 == len(db.chavailable))
	godbc.Ensure(0 == len(db.chwriting))
	godbc.Ensure(nil != db.segment)

	// Return the log object to the caller.
	// Also return the maximum number of blocks, which may
	// be different from what the caller asked.  The log
	// will make sure that the maximum number of blocks
	// are contained per segment
	return db, db.blocks
}

func (c *Log) logread() {
	defer c.wg.Done()
	for m := range c.logreaders {
		iopkt := m.IoPkt()
		offset := c.offset(iopkt.BlockNum)

		// Read from storage
		//start := time.Now()
		n, err := c.fp.ReadAt(iopkt.Buffer, int64(offset))
		//end := time.Now()
		//-- c.stats.ReadTimeRecord(end.Sub(start))

		godbc.Check(uint64(n) == c.blocksize,
			fmt.Sprintf("Read %v expected %v from location %v index %v",
				n, c.blocksize, offset, iopkt.BlockNum))
		godbc.Check(err == nil)
		//-- c.stats.StorageHit()

		// Save in buffer cache
		//c.bc.Set(index, val)

		m.Done()
	}
}

func (c *Log) server() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		emptychan := false
		for {
			// Check if we have been signaled through <-quit
			// If we have, we now know that as soon as the
			// message channel is empty, we can quit.
			if emptychan {
				if len(c.Msgchan) == 0 {
					break
				}
			}

			select {
			case msg := <-c.Msgchan:
				switch msg.Type {
				case message.MsgPut:
					c.put(msg)
				case message.MsgGet:
					c.get(msg)
				}
			case <-c.quitchan:
				// :TODO: Ok for now, but we cannot just quit
				// We need to empty the Iochan
				emptychan = true
			}
		}

		// We are closing the log.  Need to shut down the channels
		if c.segment.written {
			c.sync()
		}
		close(c.chwriting)
		close(c.logreaders)

	}()
}

func (c *Log) writer() {

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

func (c *Log) reader() {

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for s := range c.chreader {
			s.lock.Lock()

			// Reset the bufferIO managers
			s.data.Reset()

			// Move to the next offset
			c.current += c.segmentsize
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

func (c *Log) sync() {
	// Send to writer
	c.chwriting <- c.segment

	// Get a new available buffer
	c.segment = <-c.chavailable
}

func (c *Log) offset(index uint64) uint64 {
	return (index * c.blocksize)
}

func (c *Log) inRange(index uint64, s *IoSegment) bool {
	offset := c.offset(index)

	return ((offset >= s.offset) &&
		(offset < (s.offset + c.segmentsize)))
}

func (c *Log) put(msg *message.Message) error {

	iopkt := msg.IoPkt()
	godbc.Require(iopkt.BlockNum < c.blocks)

	// Make sure the block number curresponds to the
	// current segment.  If not, c.sync() will place
	// the next available segment into c.segment
	for !c.inRange(iopkt.BlockNum, c.segment) {
		c.sync()
	}

	// get log offset
	offset := c.offset(iopkt.BlockNum)

	// Buffer cache is a Read-miss cache
	//c.bc.Invalidate(iopkt.BlockNum)

	// Write to current buffer
	n, err := c.segment.data.WriteAt(iopkt.Buffer, int64(offset-c.segment.offset))
	godbc.Check(n == len(iopkt.Buffer))
	godbc.Check(err == nil)

	c.segment.written = true

	// We have written the data, and we are done with the message
	msg.Done()

	return err
}

func (c *Log) get(msg *message.Message) error {

	var n int
	var err error

	iopkt := msg.IoPkt()
	offset := c.offset(iopkt.BlockNum)

	/*
		err = c.bc.Get(iopkt.BlockNum, iopkt.Buffer)
		if err == nil {
			c.stats.BufferHit()
			return nil
		}
	*/

	// Check if the data is in RAM.  Go through each buffered segment
	for i := 0; i < c.segmentbuffers; i++ {

		c.segments[i].lock.RLock()

		if c.inRange(iopkt.BlockNum, &c.segments[i]) {

			n, err = c.segments[i].data.ReadAt(iopkt.Buffer, int64(offset-c.segments[i].offset))

			godbc.Check(err == nil)
			godbc.Check(uint64(n) == c.blocksize,
				fmt.Sprintf("Read %v expected:%v from location:%v iopkt.BlockNum:%v",
					n, c.blocksize, offset, iopkt.BlockNum))
			c.stats.RamHit()

			c.segments[i].lock.RUnlock()

			// Save in buffer cache
			//c.bc.Set(iopkt.BlockNum, iopkt.Buffer)
			msg.Done()

			return nil
		}

		c.segments[i].lock.RUnlock()
	}

	// We do not have the data yet, so we need to
	// read it from the storage system
	c.logreaders <- msg

	return nil
}

func (c *Log) Close() {

	// Shut down server first
	close(c.quitchan)
	c.wg.Wait()

	// Close the storage
	c.fp.Close()
}

func (c *Log) String() string {
	return fmt.Sprintf(
		"== IoDB Information ==\n") +
		c.stats.String()
}
