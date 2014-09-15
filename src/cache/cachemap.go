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

type CacheMap struct {
	bds   []BlockDescriptor
	size  uint64
	index uint64
	lock  sync.Mutex

	AddressMap
}

func NewCacheMap(size uint64) *CacheMap {

	if 0 == size {
		return nil
	}

	b := &CacheMap{}
	b.bds = make([]BlockDescriptor, size)
	b.size = size
	b.addressmap = make(map[AddressMapKey]uint64)

	godbc.Ensure(b.bds != nil)
	godbc.Ensure(b.addressmap != nil)

	return b
}

func (c *CacheMap) Alloc(address AddressMapKey) (newindex uint64, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for {
		for ; c.index < c.size; c.index++ {
			bd := &c.bds[c.index]

			if bd.mru {
				bd.mru = false
			} else {
				if bd.used {
					c.DeleteAddressMapKey(bd.address)
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

func (c *CacheMap) Using(index uint64) {
	godbc.Require(index < c.size)
	c.lock.Lock()
	defer c.lock.Unlock()

	c.bds[index].mru = true
}

func (c *CacheMap) Free(index uint64) {
	godbc.Require(index < c.size)
	c.lock.Lock()
	defer c.lock.Unlock()

	c.bds[index].mru = false
	c.bds[index].used = false

	c.DeleteAddressMapKey(c.bds[index].address)
}

func (c *CacheMap) FreeAddress(key AddressMapKey) {
	if index, ok := c.GetAddressMapKey(key); ok {
		c.Free(index)
	}
}

func (c *CacheMap) UsingAddress(key AddressMapKey) {
	if index, ok := c.GetAddressMapKey(key); ok {
		c.Using(index)
	}
}
