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
	"github.com/lpabon/godbc"
	"time"
)

type MsgType int

const (
	MsgStart MsgType = iota + 1
	MsgStop
	MsgShutdown
	MsgPut
	MsgGet
	MsgHitmap
	MsgInvalidate
	MsgStats
	MsgObjCreate
	MsgObjGet
	MsgObjDelete
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
}

func (m *Message) TimeStart() {
	m.Stats.start = time.Now()
}

func (m *Message) TimeElapsed() time.Duration {
	return time.Now().Sub(m.Stats.start)
}

func (m *Message) Done() {
	godbc.Require(m.RetChan != nil)
	m.RetChan <- m
}

type HitmapPkt struct {
	Hitmap []bool
	Hits   int
}

func NewMsgHitmap(h []bool, hits int, rc chan *Message) *Message {
	return &Message{
		Type:    MsgHitmap,
		RetChan: rc,
		Pkg: &HitmapPkt{
			Hitmap: h,
			Hits:   hits,
		},
	}
}

func (m *Message) HitmapPkt() *HitmapPkt {
	return m.Pkg.(*HitmapPkt)
}
