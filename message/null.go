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

type NullPipeline struct {
	In  chan *Message
	Out chan *Message
}

func NewNullPipeline(out chan *Message) *NullPipeline {
	return &NullPipeline{
		In:  make(chan *Message, 32),
		Out: out,
	}
}

func NewNullTerminator() *NullPipeline {
	return &NullPipeline{
		In:  make(chan *Message, 32),
		Out: nil,
	}
}

func (n *NullPipeline) Start() {
	go func() {
		for m := range n.In {
			if n.Out != nil {
				n.Out <- m
			} else {
				m.Done()
			}
		}
	}()
}

func (n *NullPipeline) Close() {
	close(n.In)
}
