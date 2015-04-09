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
)

type IoPkt struct {
	// Backend id + offset
	Address uint64

	// Buffer transfer data in or out
	Buffer []byte

	// Block number on the Log to read
	// from or write to
	LogBlock uint32

	// Number of blocks
	Blocks uint32
}

func newio(msgtype MsgType) *Message {
	return &Message{
		Type: msgtype,
		Pkg: &IoPkt{
			Blocks: 1,
		},
	}
}

func NewMsgGet() *Message {
	return newio(MsgGet)
}

func NewMsgPut() *Message {
	return newio(MsgPut)
}

func (m *Message) IoPkt() *IoPkt {
	return m.Pkg.(*IoPkt)
}

func (i *IoPkt) String() string {
	return fmt.Sprintf("IoPkt{"+
		"Address:%v "+
		"LogBlock:%v "+
		"Blocks:%v"+
		"}",
		i.Address,
		i.LogBlock,
		i.Blocks)
}
