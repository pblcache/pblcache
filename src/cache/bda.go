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

package cache

import (
	"github.com/lpabon/godbc"
	"sync"
)

type BlockDescriptor struct {
	address AddressMapKey
	mru     bool
	used    bool
}

type BlockDescriptorArray struct {
	bds        []BlockDescriptor
	size       uint64
	index      uint64
	addressmap *AddressMap
	lock       sync.Mutex
}

func NewBlockDescriptorArray(size uint64, addressmap *AddressMap) *BlockDescriptorArray {
	b := &BlockDescriptorArray{}
	b.bds = make([]BlockDescriptor, size)
	b.size = size
	b.addressmap = addressmap
	return b
}

func (c *BlockDescriptorArray) Alloc(address AddressMapKey) (newindex uint64, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for {
		for ; c.index < c.size; c.index++ {
			bd := &c.bds[c.index]

			if bd.mru {
				bd.mru = false
			} else {
				if bd.used {
					c.addressmap.DeleteAddressMapKey(bd.address)
				}

				newindex = c.index
				err = nil

				bd.address = address
				bd.mru = false
				bd.used = true
				c.index++

				return
			}
		}
		c.index = 0
	}
}

func (c *BlockDescriptorArray) Using(index uint64) {
	godbc.Require(index < c.size)
	c.lock.Lock()
	defer c.lock.Unlock()

	c.bds[index].mru = true
}

func (c *BlockDescriptorArray) Free(index uint64) {
	godbc.Require(index < c.size)
	c.lock.Lock()
	defer c.lock.Unlock()

	c.bds[index].mru = false
	c.bds[index].used = false

	c.addressmap.DeleteAddressMapKey(c.bds[index].address)
}
