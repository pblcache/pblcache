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

type IoCacheBlockInfo struct {
	key  string
	mru  bool
	used bool
}

type IoCacheBlocks struct {
	cacheblocks []IoCacheBlockInfo
	size        uint64
	index       uint64
}

func NewIoCacheBlocks(cachesize uint64) *IoCacheBlocks {
	icb := &IoCacheBlocks{}
	icb.cacheblocks = make([]IoCacheBlockInfo, cachesize)
	icb.size = cachesize
	return icb
}

func (c *IoCacheBlocks) Insert(key string) (evictkey string, newindex uint64, err error) {
	for {
		for ; c.index < c.size; c.index++ {
			if c.cacheblocks[c.index].mru {
				c.cacheblocks[c.index].mru = false
			} else {
				if c.cacheblocks[c.index].used {
					evictkey = c.cacheblocks[c.index].key
				} else {
					evictkey = ""
				}
				newindex = c.index
				err = nil
				c.cacheblocks[c.index].key = key
				c.cacheblocks[c.index].mru = false
				c.cacheblocks[c.index].used = true
				c.index++
				return
			}
		}
		c.index = 0
	}
}

func (c *IoCacheBlocks) Using(index uint64) {
	c.cacheblocks[index].mru = true
}

func (c *IoCacheBlocks) Free(index uint64) {
	c.cacheblocks[index].mru = false
	c.cacheblocks[index].used = false
	c.cacheblocks[index].key = ""
}
