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
	"testing"
)

func TestMsgIo(t *testing.T) {
	c := make(chan *MsgIo)
	m := NewMsgIO(MsgGet)
	m.RetChan = c
	assert(t, m.BlockNum == 0)
	assert(t, m.Buffer == nil)
	assert(t, m.Offset == 0)
	assert(t, m.Obj == 0)
	assert(t, m.Message.RetChan == nil)
	assert(t, m.RetChan == c)
}

func TestMsgIoDone(t *testing.T) {

	// Channel to send
	worker := make(chan *MsgIo)

	// Return channel
	backhere := make(chan *MsgIo)

	m := &MsgIo{
		RetChan: backhere,
		Message: Message{
			Type: MsgPut,

			// Create some private data
			Priv: &Data{i: 1},
		},
	}

	// Start 'work' service
	go func() {

		// Wait for work
		msg := <-worker
		d := msg.Priv.(*Data)
		msg.Buffer = []byte("TESTSTRING")
		assert(t, msg.Type == MsgPut)
		assert(t, d.i == 1)

		// Increment the offset here to test
		d.i += 1

		// Return to channel
		msg.Done()

	}()

	// Send to 'work'
	worker <- m

	// Wait until it is done
	rm := <-backhere

	// Get the priv data
	newD := rm.Priv.(*Data)

	// Check results
	assert(t, newD.i == 2)
	assert(t, string(rm.Buffer) == "TESTSTRING")

	// Cleanup
	close(worker)
	close(backhere)
}
