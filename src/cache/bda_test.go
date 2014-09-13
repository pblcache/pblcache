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
	amap := NewAddressMap()
	bda := NewBlockDescriptorArray(2, amap)

	id := AddressMapKey{1, 2}
	amap.SetAddressMapKey(id, 3)
	assert(t, amap.HasAddressMapKey(id))

	index, err := bda.Alloc(id)
	assert(t, bda.bds[0].address == id)
	assert(t, bda.bds[0].mru == false)
	assert(t, bda.bds[0].used == true)
	assert(t, index == 0)
	assert(t, err == nil)
}

func TestUsing(t *testing.T) {
	amap := NewAddressMap()
	bda := NewBlockDescriptorArray(2, amap)

	id := AddressMapKey{1, 2}
	amap.SetAddressMapKey(id, 3)
	assert(t, amap.HasAddressMapKey(id))

	index, err := bda.Alloc(id)
	assert(t, bda.bds[0].address == id)
	assert(t, bda.bds[0].mru == false)
	assert(t, bda.bds[0].used == true)
	assert(t, index == 0)
	assert(t, err == nil)

	bda.Using(index)
	assert(t, bda.bds[0].address == id)
	assert(t, bda.bds[0].mru == true)
	assert(t, bda.bds[0].used == true)
}

func TestFree(t *testing.T) {
	amap := NewAddressMap()
	bda := NewBlockDescriptorArray(2, amap)

	id := AddressMapKey{1, 2}
	amap.SetAddressMapKey(id, 3)
	assert(t, amap.HasAddressMapKey(id))

	index, err := bda.Alloc(id)
	assert(t, bda.bds[0].address == id)
	assert(t, bda.bds[0].mru == false)
	assert(t, bda.bds[0].used == true)
	assert(t, index == 0)
	assert(t, err == nil)

	bda.Free(index)
	assert(t, bda.bds[0].mru == false)
	assert(t, bda.bds[0].used == false)
}

func TestEvictions(t *testing.T) {
	amap := NewAddressMap()
	bda := NewBlockDescriptorArray(2, amap)

	id1 := AddressMapKey{1, 2}
	id2 := AddressMapKey{1, 3}
	id3 := AddressMapKey{2, 2}

	index, err := bda.Alloc(id1)
	amap.SetAddressMapKey(id1, index)
	assert(t, amap.HasAddressMapKey(id1))
	assert(t, !amap.HasAddressMapKey(id2))
	assert(t, !amap.HasAddressMapKey(id3))
	assert(t, bda.bds[0].address == id1)
	assert(t, bda.bds[0].mru == false)
	assert(t, bda.bds[0].used == true)
	assert(t, index == 0)
	assert(t, err == nil)

	index, err = bda.Alloc(id2)
	amap.SetAddressMapKey(id2, index)
	assert(t, amap.HasAddressMapKey(id1))
	assert(t, amap.HasAddressMapKey(id2))
	assert(t, !amap.HasAddressMapKey(id3))
	assert(t, bda.bds[1].address == id2)
	assert(t, bda.bds[1].mru == false)
	assert(t, bda.bds[1].used == true)
	assert(t, index == 1)
	assert(t, err == nil)

	bda.Using(0)
	assert(t, bda.bds[0].address == id1)
	assert(t, bda.bds[0].mru == true)
	assert(t, bda.bds[0].used == true)

	index, err = bda.Alloc(id3)
	amap.SetAddressMapKey(id3, index)
	assert(t, amap.HasAddressMapKey(id1))
	assert(t, !amap.HasAddressMapKey(id2))
	assert(t, amap.HasAddressMapKey(id3))
	assert(t, bda.bds[0].address == id1)
	assert(t, bda.bds[0].mru == false)
	assert(t, bda.bds[0].used == true)
	assert(t, bda.bds[1].address == id3)
	assert(t, bda.bds[1].mru == false)
	assert(t, bda.bds[1].used == true)
	assert(t, index == 1)
	assert(t, err == nil)

	bda.Free(1)
	assert(t, amap.HasAddressMapKey(id1))
	assert(t, !amap.HasAddressMapKey(id2))
	assert(t, !amap.HasAddressMapKey(id3))
	assert(t, bda.bds[1].mru == false)
	assert(t, bda.bds[1].used == false)

	index, err = bda.Alloc(id2)
	amap.SetAddressMapKey(id2, index)
	assert(t, !amap.HasAddressMapKey(id1))
	assert(t, amap.HasAddressMapKey(id2))
	assert(t, !amap.HasAddressMapKey(id3))
	assert(t, bda.bds[0].address == id2)
	assert(t, bda.bds[0].mru == false)
	assert(t, bda.bds[0].used == true)
	assert(t, bda.bds[1].mru == false)
	assert(t, bda.bds[1].used == false)
	assert(t, index == 0)
	assert(t, err == nil)

}
