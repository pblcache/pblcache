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
	"errors"
	"fmt"
	"github.com/lpabon/godbc"
	"sync"
	"sync/atomic"
	"time"
)

type MsgType int

var (
	ErrMessageUsed = errors.New("Message has been used already")
)

const (
	MsgPut MsgType = iota + 1
	MsgGet
)

type MessageStats struct {
	start time.Time
}

type Message struct {
	Type    MsgType
	Pkg     interface{}
	Priv    interface{}
	RetChan chan *Message
	Err     error
	Stats   MessageStats
	parent  *Message
	wg      sync.WaitGroup
	done    uint32
}

func (m *Message) TimeStart() {
	m.Stats.start = time.Now()
}

func (m *Message) TimeElapsed() time.Duration {
	return time.Now().Sub(m.Stats.start)
}

func (m *Message) Add(child *Message) {
	godbc.Require(child.parent == nil, child)

	m.wg.Add(1)
	child.parent = m

	godbc.Ensure(child.parent == m)
}

func (m *Message) String() string {
	return fmt.Sprintf("MSG{"+
		"Type:%d "+
		"Pkg:%v "+
		"Priv:%v "+
		"parent:%v"+
		"}",
		m.Type,
		m.Pkg,
		m.Priv,
		m.parent)
}

// Notify that the message is complete.
// Once a message is done, it cannot be used again.
func (m *Message) Done() {
	godbc.Require(atomic.AddUint32(&m.done, 1) == 1)

	// Create this in a goroutine so that
	// caller does not wait for children
	go func() {

		// Wait for children messages
		m.wg.Wait()

		// Send to channel if set
		if m.RetChan != nil {
			m.RetChan <- m
		}

		// We are finished. Notify parent
		// we are done
		if m.parent != nil {
			m.parent.wg.Done()
			m.parent = nil
		}
	}()
}

func (m *Message) Check() error {
	if atomic.LoadUint32(&m.done) > 0 {
		return ErrMessageUsed
	}

	return nil
}
