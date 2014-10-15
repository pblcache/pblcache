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
	"github.com/pblcache/pblcache/src/message"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

func assert(t *testing.T, b bool) {
	if !b {
		pc, file, line, _ := runtime.Caller(1)
		caller_func_info := runtime.FuncForPC(pc)

		t.Errorf("\n\rASSERT:\tfunc (%s) 0x%x\n\r\tFile %s:%d",
			caller_func_info.Name(),
			pc,
			file,
			line)
	}
}

func TestNewCache(t *testing.T) {
	c := NewCache(8)
	assert(t, c != nil)
	c.Close()
}

func TestCacheSimple(t *testing.T) {
	c := NewCache(8)
	assert(t, c != nil)

	here := make(chan *message.Message)
	m := message.NewMsgPut()
	m.RetChan = here
	io := m.IoPkt()
	io.Offset = 1

	// First Put
	c.Msgchan <- m
	<-here
	assert(t, io.BlockNum == 0)

	val, ok := c.addressmap[io.Offset]
	assert(t, val == 0)
	assert(t, ok == true)

	// Insert again.  Should allocate
	// next block
	c.Msgchan <- m
	<-here
	assert(t, io.BlockNum == 1)
	assert(t, m.Err == nil)

	val, ok = c.addressmap[io.Offset]
	assert(t, val == 1)
	assert(t, ok == true)

	// Send a Get
	mg := message.NewMsgGet()
	io = mg.IoPkt()
	io.Offset = 1
	mg.RetChan = here
	c.Msgchan <- mg
	<-here
	assert(t, io.BlockNum == 1)
	assert(t, mg.Err == nil)

	// Send Invalidate
	mi := message.NewMsgInvalidate()
	io = mi.IoPkt()
	io.Offset = 1
	mi.RetChan = here
	c.Msgchan <- mi
	<-here
	assert(t, mi.Err == nil)

	// Send Invalidate
	mi = message.NewMsgInvalidate()
	io = mi.IoPkt()
	io.Offset = 1
	mi.RetChan = here
	c.Msgchan <- mi
	<-here
	assert(t, mi.Err == ErrNotFound)

	// Send a Get again, but it should not be there
	mg = message.NewMsgGet()
	io = mg.IoPkt()
	io.Offset = 1
	mg.RetChan = here
	c.Msgchan <- mg
	<-here
	assert(t, mg.Err == ErrNotFound)

	c.Close()
}

func response_handler(wg *sync.WaitGroup,
	quit chan struct{},
	m chan *message.Message) {

	var (
		gethits, getmisses, puts, invalidatehits, invalidatemisses int
		tgh, tgm, tp, tih, tim                                     tm.TimeDuration
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
			case message.MsgInvalidate:
				if msg.Err == nil {
					invalidatehits++
					tih.Add(msg.TimeElapsed())
				} else {
					invalidatemisses++
					tim.Add(msg.TimeElapsed())
				}
			case message.MsgPut:
				puts++
				tp.Add(msg.TimeElapsed())
			}

		case <-quit:
			emptychan = true
		}
	}
	fmt.Printf("Get H:%d M:%d, Puts:%d, Invalidates H:%d M:%d\n"+
		"Get Hit Rate: %.2f Invalidate Hit Rate: %.2f\n"+
		"Mean times in usecs:\n"+
		"Get H:%.2f M:%.2f, Puts:%.2f, Inv H:%.2f M:%.2f\n",
		gethits, getmisses, puts, invalidatehits, invalidatemisses,
		float64(gethits)/float64(gethits+getmisses),
		float64(invalidatehits)/float64(invalidatehits+invalidatemisses),
		tgh.MeanTimeUsecs(), tgm.MeanTimeUsecs(),
		tp.MeanTimeUsecs(),
		tih.MeanTimeUsecs(), tim.MeanTimeUsecs())
}

func TestConcurrency(t *testing.T) {
	var wgIo, wgRet sync.WaitGroup
	c := NewCache(300)

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
				switch r.Intn(3) {
				case 0:
					msg = message.NewMsgGet()
				case 1:
					msg = message.NewMsgPut()
				case 2:
					msg = message.NewMsgInvalidate()
				}
				iopkt := msg.IoPkt()

				// Maximum "disk" size is 10 times bigger than cache
				iopkt.Offset = uint64(r.Int63n(3000))
				msg.RetChan = returnch

				// Send request
				msg.TimeStart()
				c.Msgchan <- msg

				// Simulate waiting for more work by sleeping
				// anywhere from 100usecs to 10ms
				time.Sleep(time.Microsecond * time.Duration((r.Intn(10000) + 100)))
			}
		}()

	}

	// Wait for all clients to finish
	wgIo.Wait()

	// Send receiver a message that all clients have shut down
	c.Close()
	quit <- struct{}{}

	// Wait for receiver to finish emptying its channel
	wgRet.Wait()
}
