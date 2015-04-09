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
	"github.com/pblcache/pblcache/tests"
	"strings"
	"testing"
)

func TestGetIoPkt(t *testing.T) {
	c := make(chan *Message)
	m := NewMsgGet()
	m.RetChan = c
	iopkt := m.IoPkt()
	tests.Assert(t, iopkt.LogBlock == 0)
	tests.Assert(t, iopkt.Buffer == nil)
	tests.Assert(t, iopkt.Address == 0)
	tests.Assert(t, iopkt.Obj == 0)
	tests.Assert(t, m.RetChan == c)
	tests.Assert(t, m.Type == MsgGet)
}

func TestIoPktString(t *testing.T) {
	m := NewMsgGet()
	iopkt := m.IoPkt()
	s := iopkt.String()

	tests.Assert(t, strings.Contains(s, "Address"))
	tests.Assert(t, strings.Contains(s, "LogBlock"))
	tests.Assert(t, strings.Contains(s, "Blocks"))

}

func TestPutIoPkt(t *testing.T) {
	c := make(chan *Message)
	m := NewMsgPut()
	m.RetChan = c
	iopkt := m.IoPkt()
	tests.Assert(t, iopkt.LogBlock == 0)
	tests.Assert(t, iopkt.Buffer == nil)
	tests.Assert(t, iopkt.Address == 0)
	tests.Assert(t, iopkt.Obj == 0)
	tests.Assert(t, m.RetChan == c)
	tests.Assert(t, m.Type == MsgPut)
}

func TestInvalidateIoPkt(t *testing.T) {
	c := make(chan *Message)
	m := NewMsgGet()
	m.RetChan = c
	iopkt := m.IoPkt()
	tests.Assert(t, iopkt.LogBlock == 0)
	tests.Assert(t, iopkt.Buffer == nil)
	tests.Assert(t, iopkt.Address == 0)
	tests.Assert(t, iopkt.Obj == 0)
	tests.Assert(t, m.RetChan == c)
	tests.Assert(t, m.Type == MsgGet)
}

func TestMsgIoDone(t *testing.T) {

	// Channel to send
	worker := make(chan *Message)

	// Return channel
	backhere := make(chan *Message)

	// Message
	m := NewMsgPut()
	m.Priv = &Data{i: 1}
	m.RetChan = backhere

	// Start 'work' service
	go func() {

		// Wait for work
		msg := <-worker
		d := msg.Priv.(*Data)
		io := msg.IoPkt()
		io.Buffer = []byte("TESTSTRING")
		tests.Assert(t, msg.Type == MsgPut)
		tests.Assert(t, d.i == 1)

		// Increment the offset here to test
		d.i += 1

		// Return to channel
		msg.Done()

	}()

	// Send to 'work'
	worker <- m

	// Wait until it is done
	rm := <-backhere

	// Get the data
	newD := rm.Priv.(*Data)
	io := rm.IoPkt()

	// Check results
	tests.Assert(t, newD.i == 2)
	tests.Assert(t, string(io.Buffer) == "TESTSTRING")
}
