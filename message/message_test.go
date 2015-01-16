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
package message

/* -- Needs update

import (
	"github.com/lpabon/tm"
	"runtime"
	"testing"
)

func TestTime(t *testing.T) {
	var td tm.TimeDuration
	m := &Message{}
	for i := 0; i < 100; i++ {
		m.TimeStart()
		for j := 0; j < 10000; j++ {
		}
		td.Add(m.TimeElapsed())
	}
	assert(t, td.MeanTimeUsecs() > 0)
}

type Data struct {
	i   int
	i64 int64
	s   string
	f   float64
}

func TestMessagePriv(t *testing.T) {
	m := &Message{
		Type: MsgGet,
	}

	d := &Data{
		i:   10,
		i64: 100,
		s:   "Test",
		f:   1.001,
	}

	// Save a *data in interface
	m.Priv = d
	newD := m.Priv.(*Data)
	assert(t, newD.i == d.i)
	assert(t, newD.f == d.f)
	assert(t, newD.i64 == d.i64)
	assert(t, newD.s == d.s)
}

func TestMessageDone(t *testing.T) {

	// Channel to send
	worker := make(chan *Message)

	// Return channel
	backhere := make(chan *Message)

	m := &Message{
		Type:    MsgShutdown,
		RetChan: backhere,

		// Create some private data
		Priv: &Data{i: 1},
	}

	// Start 'work' service
	go func() {

		// Wait for work
		msg := <-worker
		d := msg.Priv.(*Data)
		assert(t, msg.Type == MsgShutdown)
		assert(t, d.i == 1)

		// Increment the offset here to test
		d.i += 1

		// Return to channel
		msg.Done()

	}()

	// Send to 'work'
	worker <- m

	// Wait until it is done
	<-backhere

	// Get the priv data
	newD := m.Priv.(*Data)

	// Check results
	assert(t, newD.i == 2)

	// Cleanup
	close(worker)
	close(backhere)
}

*/
