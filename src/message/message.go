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
	"fmt"
	"github.com/lpabon/tm"
	"time"
)

type MsgType int

const (
	MsgStart MsgType = iota + 1
	MsgStop
	MsgShutdown
	MsgPut
	MsgGet
	MsgInvalidate
)

type MessageStats struct {
	start    time.Time
	Duration tm.TimeDuration
}

type Message struct {
	Type    MsgType
	Obj     uint16
	Offset  uint64
	Buffer  []byte
	Stats   MessageStats
	Priv    interface{}
	Err     error
	RetChan chan *Message
}

func (m *Message) TimeStart() {
	m.Stats.start = time.Now()
}

func (m *Message) TimeStop() {
	m.Stats.Duration.Add(time.Now().Sub(m.Stats.start))
}

func (m *Message) String() string {
	return fmt.Sprintf("Duration %.2f usecs\n", m.Stats.Duration.MeanTimeUsecs())
}

func (m *Message) Done() {
	m.RetChan <- m
}
