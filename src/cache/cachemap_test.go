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
	"testing"
)

func TestAlloc(t *testing.T) {
	cmap := NewCacheMap(2)

	id := AddressMapKey{1, 2}
	cmap.SetAddressMapKey(id, 3)
	assert(t, cmap.HasAddressMapKey(id))

	index, err := cmap.Alloc(id)
	assert(t, cmap.bds[0].address == id)
	assert(t, cmap.bds[0].mru == false)
	assert(t, cmap.bds[0].used == true)
	assert(t, index == 0)
	assert(t, err == nil)
}

func TestUsing(t *testing.T) {
	cmap := NewCacheMap(2)

	id := AddressMapKey{1, 2}
	cmap.SetAddressMapKey(id, 3)
	assert(t, cmap.HasAddressMapKey(id))

	index, err := cmap.Alloc(id)
	assert(t, cmap.bds[0].address == id)
	assert(t, cmap.bds[0].mru == false)
	assert(t, cmap.bds[0].used == true)
	assert(t, index == 0)
	assert(t, err == nil)

	cmap.Using(index)
	assert(t, cmap.bds[0].address == id)
	assert(t, cmap.bds[0].mru == true)
	assert(t, cmap.bds[0].used == true)
}

func TestFree(t *testing.T) {
	cmap := NewCacheMap(2)

	id := AddressMapKey{1, 2}
	cmap.SetAddressMapKey(id, 3)
	assert(t, cmap.HasAddressMapKey(id))

	index, err := cmap.Alloc(id)
	assert(t, cmap.bds[0].address == id)
	assert(t, cmap.bds[0].mru == false)
	assert(t, cmap.bds[0].used == true)
	assert(t, index == 0)
	assert(t, err == nil)

	cmap.Free(index)
	assert(t, cmap.bds[0].mru == false)
	assert(t, cmap.bds[0].used == false)
}

func TestEvictions(t *testing.T) {
	cmap := NewCacheMap(2)

	id1 := AddressMapKey{1, 2}
	id2 := AddressMapKey{1, 3}
	id3 := AddressMapKey{2, 2}

	index, err := cmap.Alloc(id1)
	cmap.SetAddressMapKey(id1, index)
	assert(t, cmap.HasAddressMapKey(id1))
	assert(t, !cmap.HasAddressMapKey(id2))
	assert(t, !cmap.HasAddressMapKey(id3))
	assert(t, cmap.bds[0].address == id1)
	assert(t, cmap.bds[0].mru == false)
	assert(t, cmap.bds[0].used == true)
	assert(t, index == 0)
	assert(t, err == nil)

	index, err = cmap.Alloc(id2)
	cmap.SetAddressMapKey(id2, index)
	assert(t, cmap.HasAddressMapKey(id1))
	assert(t, cmap.HasAddressMapKey(id2))
	assert(t, !cmap.HasAddressMapKey(id3))
	assert(t, cmap.bds[1].address == id2)
	assert(t, cmap.bds[1].mru == false)
	assert(t, cmap.bds[1].used == true)
	assert(t, index == 1)
	assert(t, err == nil)

	cmap.Using(0)
	assert(t, cmap.bds[0].address == id1)
	assert(t, cmap.bds[0].mru == true)
	assert(t, cmap.bds[0].used == true)

	index, err = cmap.Alloc(id3)
	cmap.SetAddressMapKey(id3, index)
	assert(t, cmap.HasAddressMapKey(id1))
	assert(t, !cmap.HasAddressMapKey(id2))
	assert(t, cmap.HasAddressMapKey(id3))
	assert(t, cmap.bds[0].address == id1)
	assert(t, cmap.bds[0].mru == false)
	assert(t, cmap.bds[0].used == true)
	assert(t, cmap.bds[1].address == id3)
	assert(t, cmap.bds[1].mru == false)
	assert(t, cmap.bds[1].used == true)
	assert(t, index == 1)
	assert(t, err == nil)

	cmap.Free(1)
	assert(t, cmap.HasAddressMapKey(id1))
	assert(t, !cmap.HasAddressMapKey(id2))
	assert(t, !cmap.HasAddressMapKey(id3))
	assert(t, cmap.bds[1].mru == false)
	assert(t, cmap.bds[1].used == false)

	index, err = cmap.Alloc(id2)
	cmap.SetAddressMapKey(id2, index)
	assert(t, !cmap.HasAddressMapKey(id1))
	assert(t, cmap.HasAddressMapKey(id2))
	assert(t, !cmap.HasAddressMapKey(id3))
	assert(t, cmap.bds[0].address == id2)
	assert(t, cmap.bds[0].mru == false)
	assert(t, cmap.bds[0].used == true)
	assert(t, cmap.bds[1].mru == false)
	assert(t, cmap.bds[1].used == false)
	assert(t, index == 0)
	assert(t, err == nil)

}
