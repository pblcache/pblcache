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
	"errors"
	"github.com/lpabon/godbc"
)

const (
	INVALID_KEY = ^uint64(0)
)

type BlockDescriptor struct {
	key       uint64
	clock_set bool
	used      bool
}

type BlockDescriptorArraySave struct {
	Index uint32
	Size  uint32
}

type BlockDescriptorArray struct {
	bds   []BlockDescriptor
	size  uint32
	index uint32
}

func NewBlockDescriptorArray(blocks uint32) *BlockDescriptorArray {

	godbc.Require(blocks > 0)

	c := &BlockDescriptorArray{}

	c.size = blocks
	c.bds = make([]BlockDescriptor, blocks)

	return c
}

func (c *BlockDescriptorArray) Insert(key uint64) (newindex uint32, evictkey uint64, evict bool) {
	for {

		// Use the current index to check the current entry
		for ; c.index < c.size; c.index++ {
			entry := &c.bds[c.index]

			// CLOCK: If it has been used recently, then do not evict
			if entry.clock_set {
				entry.clock_set = false
			} else {

				// If it is in use, then we need to evict the older key
				if entry.used {
					evictkey = entry.key
					evict = true
				} else {
					evictkey = INVALID_KEY
					evict = false
				}

				// Set return values
				newindex = c.index

				// Setup current cachemap entry
				entry.key = key
				entry.clock_set = false
				entry.used = true

				// Set index to next cachemap entry
				c.index++

				return
			}
		}
		c.index = 0
	}
}

func (c *BlockDescriptorArray) Using(index uint32) {
	c.bds[index].clock_set = true
}

func (c *BlockDescriptorArray) Free(index uint32) {
	c.bds[index].clock_set = false
	c.bds[index].used = false
	c.bds[index].key = INVALID_KEY
}

func (c *BlockDescriptorArray) Save() (*BlockDescriptorArraySave, error) {
	cms := &BlockDescriptorArraySave{}
	cms.Index = c.index
	cms.Size = c.size

	return cms, nil
}

func (c *BlockDescriptorArray) Load(cms *BlockDescriptorArraySave, addressmap map[uint64]uint32) error {

	if cms.Size != c.size {
		return errors.New("Loaded metadata cache map size is not equal to the current cache map size")
	}

	for key, index := range addressmap {
		c.bds[index].used = true
		c.bds[index].key = key
	}

	c.index = cms.Index

	return nil
}
