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
	"testing"
)

func TestNullNewPipeline(t *testing.T) {
	out := make(chan *Message)
	np := NewNullPipeline(out)
	defer np.Close()

	tests.Assert(t, np.In != nil)
	tests.Assert(t, len(np.In) == 0)
	tests.Assert(t, np.Out == out)
}

func TestNullNewTerminator(t *testing.T) {
	np := NewNullTerminator()
	defer np.Close()

	tests.Assert(t, np.In != nil)
	tests.Assert(t, len(np.In) == 0)
	tests.Assert(t, np.Out == nil)
}

func TestNullPipeline(t *testing.T) {
	destination := make(chan *Message)
	pipe := make(chan *Message)
	np := NewNullPipeline(pipe)
	defer np.Close()
	np.Start()

	m := &Message{
		Type:    MsgPut,
		RetChan: destination,
	}

	// Send message
	np.In <- m

	// Pipe should be the first to see it
	returnedmsg := <-pipe
	tests.Assert(t, returnedmsg == m)

	// Now we finish the message
	m.Done()

	// It should arrive at destination
	returnedmsg = <-destination
	tests.Assert(t, returnedmsg == m)

}

func TestNullTerminator(t *testing.T) {
	destination := make(chan *Message)

	// Terminator will call m.Done() for us
	np := NewNullTerminator()
	defer np.Close()
	np.Start()

	m := &Message{
		Type:    MsgPut,
		RetChan: destination,
	}

	// Send message
	np.In <- m

	// It should arrive at destination
	returnedmsg := <-destination
	tests.Assert(t, returnedmsg == m)

}
