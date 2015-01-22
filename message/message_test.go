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

import (
	"github.com/lpabon/tm"
	"github.com/pblcache/pblcache/tests"
	"strings"
	"testing"
	"time"
)

func TestMessageTime(t *testing.T) {
	var td tm.TimeDuration
	m := &Message{}
	for i := 0; i < 100; i++ {
		m.TimeStart()
		for j := 0; j < 10000; j++ {
		}
		td.Add(m.TimeElapsed())
	}
	tests.Assert(t, td.MeanTimeUsecs() > 0)
}

func TestMessageString(t *testing.T) {
	m := &Message{}
	s := m.String()

	tests.Assert(t, strings.Contains(s, "Type"))
	tests.Assert(t, strings.Contains(s, "Pkg"))
	tests.Assert(t, strings.Contains(s, "Priv"))
	tests.Assert(t, strings.Contains(s, "parent"))
}

func TestMessageAdd(t *testing.T) {
	grandpa := &Message{}
	father := &Message{}
	son := &Message{}
	daughter := &Message{}

	done := make(chan *Message)
	grandpa.RetChan = done

	father.Add(son)
	father.Add(daughter)

	grandpa.Add(father)

	tests.Assert(t, father.parent == grandpa)
	tests.Assert(t, son.parent == father)
	tests.Assert(t, daughter.parent == father)

	// --- This should return before timeout
	go func() {
		time.Sleep(time.Millisecond * 5)
		son.Done()
	}()

	go func() {
		time.Sleep(time.Millisecond * 100)
		daughter.Done()
	}()

	go func() {
		time.Sleep(time.Millisecond * 50)
		father.Done()
	}()

	// Wait here until all children are done
	go func() {
		grandpa.Done()
	}()

	timeout := time.After(time.Second * 2)
	select {
	case <-timeout:
		t.Error("Waiting for children failed")
	case <-done:
	}

}

func TestMessageAddTestError(t *testing.T) {
	grandpa := &Message{}
	father := &Message{}
	son := &Message{}
	daughter := &Message{}

	done := make(chan *Message)
	grandpa.RetChan = done

	father.Add(son)
	father.Add(daughter)

	grandpa.Add(father)

	tests.Assert(t, father.parent == grandpa)
	tests.Assert(t, son.parent == father)
	tests.Assert(t, daughter.parent == father)

	// --- This should timeout since son is waiting for
	//     5 seconds
	go func() {
		time.Sleep(time.Second * 5)
		son.Done()
	}()

	go func() {
		time.Sleep(time.Millisecond * 500)
		daughter.Done()
	}()

	go func() {
		time.Sleep(time.Millisecond * 500)
		father.Done()
	}()

	// Wait here until all children are done
	go func() {
		grandpa.Done()
	}()

	timeout := time.After(time.Second * 1)
	select {
	case <-timeout:

	case <-done:
		t.Error("Children came back too fast!")
	}

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
	tests.Assert(t, newD.i == d.i)
	tests.Assert(t, newD.f == d.f)
	tests.Assert(t, newD.i64 == d.i64)
	tests.Assert(t, newD.s == d.s)
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

	// Check message is clear
	err := m.Check()
	tests.Assert(t, err == nil)

	// Start 'work' service
	go func() {

		// Wait for work
		msg := <-worker
		d := msg.Priv.(*Data)
		tests.Assert(t, msg.Type == MsgShutdown)
		tests.Assert(t, d.i == 1)

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

	// Check message is used
	err = m.Check()
	tests.Assert(t, err == ErrMessageUsed)

	// Check results
	tests.Assert(t, newD.i == 2)

	// Cleanup
	close(worker)
	close(backhere)
}
