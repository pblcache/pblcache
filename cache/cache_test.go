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
	"github.com/lpabon/tm"
	"github.com/pblcache/pblcache/message"
	"github.com/pblcache/pblcache/tests"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestNewCache(t *testing.T) {
	nc := message.NewNullTerminator()
	nc.Start()
	defer nc.Close()

	c := NewCache(8, 4096, nc.In)
	tests.Assert(t, c != nil)
	c.Close()
}

func TestInvalidateMultipleBlocks(t *testing.T) {
	nc := message.NewNullTerminator()
	nc.Start()
	defer nc.Close()

	c := NewCache(8, 4096, nc.In)
	tests.Assert(t, c != nil)
	defer c.Close()

	// Insert some values in the addressmap
	for i := uint64(0); i < 4; i++ {

		// The key is offset in bytes
		c.addressmap[i*4096] = i
	}

	// This value should still be on the addressmap
	c.addressmap[8*4096] = 8

	iopkt := &message.IoPkt{
		Offset:  0,
		Nblocks: 8,
	}

	c.Invalidate(iopkt)
	tests.Assert(t, c.stats.invalidations == uint64(iopkt.Nblocks))
	tests.Assert(t, c.stats.invalidatehits == 4)
	tests.Assert(t, c.addressmap[8*4096] == 8)
}

func TestCacheSimple(t *testing.T) {
	mocklog := make(chan *message.Message)

	// This service will run in its own goroutine
	// and send to mocklog any messages
	pipeline := message.NewNullPipeline(mocklog)
	pipeline.Start()
	defer pipeline.Close()

	c := NewCache(8, 4096, pipeline.In)
	tests.Assert(t, c != nil)

	here := make(chan *message.Message)
	buffer := make([]byte, 4096)
	m := message.NewMsgPut()
	m.RetChan = here
	io := m.IoPkt()
	io.Buffer = buffer
	io.Offset = 1
	m.Priv = c

	// First Put
	err := c.Put(m)
	tests.Assert(t, err == nil)

	logmsg := <-mocklog
	logio := logmsg.IoPkt()
	tests.Assert(t, m.Type == logmsg.Type)
	tests.Assert(t, logmsg.Priv.(*Cache) == c)
	tests.Assert(t, io.Nblocks == logio.Nblocks)
	tests.Assert(t, io.Offset == logio.Offset)
	tests.Assert(t, logio.BlockNum == 0)
	logmsg.Done()

	returnedmsg := <-here
	rio := returnedmsg.IoPkt()
	tests.Assert(t, m.Type == returnedmsg.Type)
	tests.Assert(t, returnedmsg.Priv.(*Cache) == c)
	tests.Assert(t, io.Nblocks == rio.Nblocks)
	tests.Assert(t, io.Offset == rio.Offset)
	tests.Assert(t, c.stats.insertions == 1)
	tests.Assert(t, returnedmsg.Err == nil)

	val, ok := c.addressmap[io.Offset]
	tests.Assert(t, val == 0)
	tests.Assert(t, ok == true)

	// Check that we cannot resend this message
	err = c.Put(m)
	tests.Assert(t, err == message.ErrMessageUsed)

	// Insert again.  Should allocate
	// next block
	m = message.NewMsgPut()
	m.RetChan = here
	io = m.IoPkt()
	io.Buffer = buffer
	io.Offset = 1
	m.Priv = c
	err = c.Put(m)
	tests.Assert(t, err == nil)

	logmsg = <-mocklog
	logio = logmsg.IoPkt()
	tests.Assert(t, logio.BlockNum == 1)
	logmsg.Done()

	returnedmsg = <-here
	rio = returnedmsg.IoPkt()
	tests.Assert(t, returnedmsg.Err == nil)
	tests.Assert(t, c.stats.insertions == 2)

	val, ok = c.addressmap[io.Offset]
	tests.Assert(t, val == 1)
	tests.Assert(t, ok == true)

	// Send a Get
	mg := message.NewMsgGet()
	io = mg.IoPkt()
	io.Offset = 1
	io.Buffer = buffer
	mg.RetChan = here

	hitmap, err := c.Get(mg)
	tests.Assert(t, err == nil)
	tests.Assert(t, hitmap.Hits == 1)
	tests.Assert(t, hitmap.Hitmap[0] == true)
	tests.Assert(t, hitmap.Hits == io.Nblocks)

	logmsg = <-mocklog
	logio = logmsg.IoPkt()
	tests.Assert(t, logio.BlockNum == 1)
	logmsg.Done()

	returnedmsg = <-here
	io = returnedmsg.IoPkt()
	tests.Assert(t, returnedmsg.Err == nil)
	tests.Assert(t, c.stats.insertions == 2)
	tests.Assert(t, c.stats.readhits == 1)
	tests.Assert(t, c.stats.reads == 1)

	// Test we cannot send the same message
	hitmap, err = c.Get(mg)
	tests.Assert(t, err == message.ErrMessageUsed)
	tests.Assert(t, hitmap == nil)

	// Send Invalidate
	iopkt := &message.IoPkt{}
	iopkt.Offset = 1
	iopkt.Nblocks = 1
	c.Invalidate(iopkt)
	tests.Assert(t, c.stats.insertions == 2)
	tests.Assert(t, c.stats.readhits == 1)
	tests.Assert(t, c.stats.reads == 1)
	tests.Assert(t, c.stats.invalidations == 1)
	tests.Assert(t, c.stats.invalidatehits == 1)

	// Send Invalidate
	iopkt = &message.IoPkt{}
	iopkt.Offset = 1
	iopkt.Nblocks = 1
	c.Invalidate(iopkt)
	tests.Assert(t, c.stats.insertions == 2)
	tests.Assert(t, c.stats.readhits == 1)
	tests.Assert(t, c.stats.reads == 1)
	tests.Assert(t, c.stats.invalidations == 2)
	tests.Assert(t, c.stats.invalidatehits == 1)

	// Send a Get again, but it should not be there
	mg = message.NewMsgGet()
	io = mg.IoPkt()
	io.Offset = 1
	io.Buffer = buffer
	mg.RetChan = here
	hitmap, err = c.Get(mg)
	tests.Assert(t, err == ErrNotFound)
	tests.Assert(t, hitmap == nil)
	tests.Assert(t, c.stats.insertions == 2)
	tests.Assert(t, c.stats.readhits == 1)
	tests.Assert(t, c.stats.reads == 2)
	tests.Assert(t, c.stats.invalidations == 2)
	tests.Assert(t, c.stats.invalidatehits == 1)

	// Check the stats
	stats := c.Stats()
	tests.Assert(t, stats.Readhits == c.stats.readhits)
	tests.Assert(t, stats.Invalidatehits == c.stats.invalidatehits)
	tests.Assert(t, stats.Reads == c.stats.reads)
	tests.Assert(t, stats.Evictions == c.stats.evictions)
	tests.Assert(t, stats.Invalidations == c.stats.invalidations)
	tests.Assert(t, stats.Insertions == c.stats.insertions)

	// Clear the stats
	c.StatsClear()
	tests.Assert(t, 0 == c.stats.readhits)
	tests.Assert(t, 0 == c.stats.invalidatehits)
	tests.Assert(t, 0 == c.stats.reads)
	tests.Assert(t, 0 == c.stats.evictions)
	tests.Assert(t, 0 == c.stats.invalidations)
	tests.Assert(t, 0 == c.stats.insertions)

	c.Close()
}

// This test wil check that the cache tries to place
// as many contigous blocks as possible. We will initialize
// the 8 slot cache with four slots, then remove slot 1 and 2
// to leave the following: [X__X____]
// When we put a message with 6 blocks the cache should be
// populated as follows:  [X45X01234]
//
// At the end, check multiblock Get()
//
func TestCacheMultiblock(t *testing.T) {
	// This service will run in its own goroutine
	// and send to mocklog any messages
	mocklog := make(chan *message.Message)
	pipe := message.NewNullPipeline(mocklog)
	pipe.Start()
	defer pipe.Close()

	c := NewCache(8, 4096, pipe.In)
	tests.Assert(t, c != nil)

	here := make(chan *message.Message)
	buffer := make([]byte, 4096)

	// Initialize data in cache
	for i := uint64(0); i < 4; i++ {
		m := message.NewMsgPut()
		m.RetChan = here
		io := m.IoPkt()
		io.Buffer = buffer
		io.Offset = i * 4096

		// First Put
		err := c.Put(m)
		tests.Assert(t, err == nil)
		retmsg := <-mocklog
		retmsg.Done()
		<-here
	}

	c.Invalidate(&message.IoPkt{Offset: 4096, Nblocks: 2})
	tests.Assert(t, c.stats.insertions == 4)
	tests.Assert(t, c.stats.invalidatehits == 2)
	tests.Assert(t, c.cachemap.bds[0].Used == true)
	tests.Assert(t, c.cachemap.bds[0].Key == 0)
	tests.Assert(t, c.cachemap.bds[1].Used == false)
	tests.Assert(t, c.cachemap.bds[2].Used == false)
	tests.Assert(t, c.cachemap.bds[3].Used == true)
	tests.Assert(t, c.cachemap.bds[3].Key == 3*4096)

	// Set the clock so they do not get erased
	c.cachemap.bds[0].Mru = true
	c.cachemap.bds[3].Mru = true

	// Insert multiblock
	largebuffer := make([]byte, 6*4096)
	m := message.NewMsgPut()
	m.RetChan = here
	io := m.IoPkt()
	io.Buffer = largebuffer
	io.Offset = 10 * 4096
	io.Nblocks = 6

	// First Put
	err := c.Put(m)
	tests.Assert(t, err == nil)
	for i := 0; i < io.Nblocks; i++ {
		// Put send a message for each block
		retmsg := <-mocklog
		retmsg.Done()
	}
	<-here

	tests.Assert(t, c.stats.insertions == 10)
	tests.Assert(t, c.stats.invalidatehits == 2)

	// Check the two blocks left from before
	tests.Assert(t, c.cachemap.bds[0].Used == true)
	tests.Assert(t, c.cachemap.bds[0].Key == 0)
	tests.Assert(t, c.cachemap.bds[0].Mru == false)

	tests.Assert(t, c.cachemap.bds[3].Used == true)
	tests.Assert(t, c.cachemap.bds[3].Key == 3*4096)
	tests.Assert(t, c.cachemap.bds[3].Mru == true)

	// Now check the blocks we inserted
	tests.Assert(t, c.cachemap.bds[4].Used == true)
	tests.Assert(t, c.cachemap.bds[4].Key == 10*4096)
	tests.Assert(t, c.cachemap.bds[4].Mru == false)

	tests.Assert(t, c.cachemap.bds[5].Used == true)
	tests.Assert(t, c.cachemap.bds[5].Key == 11*4096)
	tests.Assert(t, c.cachemap.bds[5].Mru == false)

	tests.Assert(t, c.cachemap.bds[6].Used == true)
	tests.Assert(t, c.cachemap.bds[6].Key == 12*4096)
	tests.Assert(t, c.cachemap.bds[6].Mru == false)

	tests.Assert(t, c.cachemap.bds[7].Used == true)
	tests.Assert(t, c.cachemap.bds[7].Key == 13*4096)
	tests.Assert(t, c.cachemap.bds[7].Mru == false)

	tests.Assert(t, c.cachemap.bds[1].Used == true)
	tests.Assert(t, c.cachemap.bds[1].Key == 14*4096)
	tests.Assert(t, c.cachemap.bds[1].Mru == false)

	tests.Assert(t, c.cachemap.bds[2].Used == true)
	tests.Assert(t, c.cachemap.bds[2].Key == 15*4096)
	tests.Assert(t, c.cachemap.bds[2].Mru == false)

	// Check for a block not in the cache
	m = message.NewMsgGet()
	m.RetChan = here
	io = m.IoPkt()
	io.Buffer = buffer
	io.Offset = 20 * 4096
	io.Nblocks = 1
	hitmap, err := c.Get(m)
	tests.Assert(t, err == ErrNotFound)
	tests.Assert(t, hitmap == nil)
	tests.Assert(t, len(here) == 0)

	// Get offset 0, 4 blocks.  It should return
	// a bit map of [1001]
	buffer4 := make([]byte, 4*4096)
	m = message.NewMsgGet()
	m.RetChan = here
	io = m.IoPkt()
	io.Buffer = buffer4
	io.Offset = 0
	io.Nblocks = 4

	hitmap, err = c.Get(m)
	tests.Assert(t, err == nil)
	tests.Assert(t, hitmap.Hits == 2)
	tests.Assert(t, len(hitmap.Hitmap) == io.Nblocks)
	tests.Assert(t, hitmap.Hitmap[0] == true)
	tests.Assert(t, hitmap.Hitmap[1] == false)
	tests.Assert(t, hitmap.Hitmap[2] == false)
	tests.Assert(t, hitmap.Hitmap[3] == true)
	for i := 0; i < 2; i++ {
		// Get sends a get for each contiguous blocks
		retmsg := <-mocklog
		retmsg.Done()
	}
	<-here

	// Get the 6 blocks we inserted previously.  This
	// should show that there are two sets of continguous
	// blocks
	m = message.NewMsgGet()
	m.RetChan = here
	io = m.IoPkt()
	io.Buffer = largebuffer
	io.Offset = 10 * 4096
	io.Nblocks = 6

	hitmap, err = c.Get(m)
	tests.Assert(t, err == nil)
	tests.Assert(t, hitmap.Hits == 6)
	tests.Assert(t, len(hitmap.Hitmap) == io.Nblocks)
	tests.Assert(t, hitmap.Hitmap[0] == true)
	tests.Assert(t, hitmap.Hitmap[1] == true)
	tests.Assert(t, hitmap.Hitmap[2] == true)
	tests.Assert(t, hitmap.Hitmap[3] == true)
	tests.Assert(t, hitmap.Hitmap[4] == true)
	tests.Assert(t, hitmap.Hitmap[5] == true)

	// The first message to the log
	retmsg := <-mocklog
	retio := retmsg.IoPkt()
	tests.Assert(t, retmsg.RetChan == nil)
	tests.Assert(t, retio.Offset == 10*4096)
	tests.Assert(t, retio.BlockNum == 4)
	tests.Assert(t, retio.Nblocks == 4)
	retmsg.Done()

	// Second message will have the rest of the contigous block
	retmsg = <-mocklog
	retio = retmsg.IoPkt()
	tests.Assert(t, retmsg.RetChan == nil)
	tests.Assert(t, retio.Offset == 14*4096)
	tests.Assert(t, retio.BlockNum == 1)
	tests.Assert(t, retio.Nblocks == 2)
	retmsg.Done()

	<-here

	// Save the cache metadata
	save := tests.Tempfile()
	defer os.Remove(save)
	err = c.Save(save)
	tests.Assert(t, err == nil)

	c.Close()
	c = NewCache(8, 4096, pipe.In)
	tests.Assert(t, c != nil)

	err = c.Load(save)
	tests.Assert(t, err == nil)

	// Get data again.
	m = message.NewMsgGet()
	m.RetChan = here
	io = m.IoPkt()
	io.Buffer = largebuffer
	io.Offset = 10 * 4096
	io.Nblocks = 6

	hitmap, err = c.Get(m)
	tests.Assert(t, err == nil)
	tests.Assert(t, hitmap.Hits == 6)
	tests.Assert(t, len(hitmap.Hitmap) == io.Nblocks)
	tests.Assert(t, hitmap.Hitmap[0] == true)
	tests.Assert(t, hitmap.Hitmap[1] == true)
	tests.Assert(t, hitmap.Hitmap[2] == true)
	tests.Assert(t, hitmap.Hitmap[3] == true)
	tests.Assert(t, hitmap.Hitmap[4] == true)
	tests.Assert(t, hitmap.Hitmap[5] == true)

	// The first message to the log
	retmsg = <-mocklog
	retio = retmsg.IoPkt()
	tests.Assert(t, retmsg.RetChan == nil)
	tests.Assert(t, retio.Offset == 10*4096)
	tests.Assert(t, retio.BlockNum == 4)
	tests.Assert(t, retio.Nblocks == 4)
	retmsg.Done()

	// Second message will have the rest of the contigous block
	retmsg = <-mocklog
	retio = retmsg.IoPkt()
	tests.Assert(t, retmsg.RetChan == nil)
	tests.Assert(t, retio.Offset == 14*4096)
	tests.Assert(t, retio.BlockNum == 1)
	tests.Assert(t, retio.Nblocks == 2)
	retmsg.Done()

	<-here

	c.Close()
}

func response_handler(wg *sync.WaitGroup,
	quit chan struct{},
	m chan *message.Message) {

	var (
		gethits, getmisses, puts int
		tgh, tgm, tp             tm.TimeDuration
	)

	defer wg.Done()

	emptychan := false
	for {

		// Check if we have been signaled through <-quit
		// If we have, we now know that as soon as the
		// message channel is empty, we can quit.
		if emptychan {
			if len(m) == 0 {
				break
			}
		}

		// Check incoming channels
		select {
		case msg := <-m:
			// Collect stats
			switch msg.Type {
			case message.MsgGet:
				if msg.Err == nil {
					gethits++
					tgh.Add(msg.TimeElapsed())
				} else {
					getmisses++
					tgm.Add(msg.TimeElapsed())
				}
			case message.MsgPut:
				puts++
				tp.Add(msg.TimeElapsed())
			}

		case <-quit:
			emptychan = true
		}
	}
	fmt.Printf("Get H:%d M:%d, Puts:%d\n"+
		"Get Hit Rate: %.2f\n"+
		"Mean times in usecs:\n"+
		"Get H:%.2f M:%.2f, Puts:%.2f\n",
		gethits, getmisses, puts,
		float64(gethits)/float64(gethits+getmisses),
		tgh.MeanTimeUsecs(), tgm.MeanTimeUsecs(),
		tp.MeanTimeUsecs())
}

func TestCacheConcurrency(t *testing.T) {
	var wgIo, wgRet sync.WaitGroup

	nc := message.NewNullTerminator()
	nc.Start()
	defer nc.Close()

	c := NewCache(300, 4096, nc.In)

	// Start up response server
	returnch := make(chan *message.Message, 100)
	quit := make(chan struct{})
	wgRet.Add(1)
	go response_handler(&wgRet, quit, returnch)

	// Create 100 clients
	for i := 0; i < 100; i++ {
		wgIo.Add(1)
		go func() {
			defer wgIo.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano()))

			// Each client to send 1k IOs
			for io := 0; io < 1000; io++ {
				var msg *message.Message
				switch r.Intn(2) {
				case 0:
					msg = message.NewMsgGet()
				case 1:
					msg = message.NewMsgPut()
				}
				iopkt := msg.IoPkt()
				iopkt.Buffer = make([]byte, 4096)

				// Maximum "disk" size is 10 times bigger than cache
				iopkt.Offset = uint64(r.Int63n(3000))
				msg.RetChan = returnch

				// Send request
				msg.TimeStart()

				switch msg.Type {
				case message.MsgGet:
					c.Get(msg)
				case message.MsgPut:
					c.Invalidate(iopkt)
					c.Put(msg)
				}

				// Simulate waiting for more work by sleeping
				// anywhere from 100usecs to 10ms
				time.Sleep(time.Microsecond * time.Duration((r.Intn(10000) + 100)))
			}
		}()

	}

	// Wait for all clients to finish
	wgIo.Wait()

	// Send receiver a message that all clients have shut down
	fmt.Print(c)
	c.Close()
	close(quit)

	// Wait for receiver to finish emptying its channel
	wgRet.Wait()
}
