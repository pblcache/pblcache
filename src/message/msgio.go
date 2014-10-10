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
)

type MsgIo struct {
	// Object descriptor
	Obj uint16

	// Offset in either bytes or LBA
	Offset uint64

	// Buffer transfer data in or out
	Buffer []byte

	// Block number on the Log to read
	// from or write to
	BlockNum uint64

	// Return pointer to MsgIo
	RetChan chan *MsgIo

	// Contains Message
	Message
}

func NewMsgIO(msgtype MsgType) *MsgIo {
	return &MsgIo{
		Message: Message{
			Type: msgtype,
		},
	}
}

// Override Message Done function
func (m *MsgIo) Done() {
	godbc.Require(m.RetChan != nil)
	m.RetChan <- m
}
