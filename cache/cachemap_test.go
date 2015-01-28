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
	"github.com/pblcache/pblcache/tests"
	"testing"
)

func TestInsert(t *testing.T) {
	cmap := NewCacheMap(2)

	id := uint64(123)
	index, evictkey, evict := cmap.Insert(id)
	tests.Assert(t, cmap.bds[0].Key == id)
	tests.Assert(t, cmap.bds[0].Mru == false)
	tests.Assert(t, cmap.bds[0].Used == true)
	tests.Assert(t, index == 0)
	tests.Assert(t, evictkey == INVALID_KEY)
	tests.Assert(t, evict == false)
}

func TestUsing(t *testing.T) {
	cmap := NewCacheMap(2)

	id := uint64(123)
	index, evictkey, evict := cmap.Insert(id)
	tests.Assert(t, cmap.bds[0].Key == id)
	tests.Assert(t, cmap.bds[0].Mru == false)
	tests.Assert(t, cmap.bds[0].Used == true)
	tests.Assert(t, index == 0)
	tests.Assert(t, evictkey == INVALID_KEY)
	tests.Assert(t, evict == false)

	cmap.Using(index)
	tests.Assert(t, cmap.bds[0].Key == id)
	tests.Assert(t, cmap.bds[0].Mru == true)
	tests.Assert(t, cmap.bds[0].Used == true)
}

func TestFree(t *testing.T) {
	cmap := NewCacheMap(2)

	id := uint64(123)
	index, evictkey, evict := cmap.Insert(id)
	tests.Assert(t, cmap.bds[0].Key == id)
	tests.Assert(t, cmap.bds[0].Mru == false)
	tests.Assert(t, cmap.bds[0].Used == true)
	tests.Assert(t, index == 0)
	tests.Assert(t, evictkey == INVALID_KEY)
	tests.Assert(t, evict == false)

	cmap.Free(index)
	tests.Assert(t, cmap.bds[0].Mru == false)
	tests.Assert(t, cmap.bds[0].Used == false)
}

func TestEvictions(t *testing.T) {
	cmap := NewCacheMap(2)

	id1 := uint64(123)
	id2 := uint64(456)
	id3 := uint64(678)

	index, evictkey, evict := cmap.Insert(id1)
	tests.Assert(t, cmap.bds[0].Key == id1)
	tests.Assert(t, cmap.bds[0].Mru == false)
	tests.Assert(t, cmap.bds[0].Used == true)
	tests.Assert(t, index == 0)
	tests.Assert(t, evictkey == INVALID_KEY)
	tests.Assert(t, evict == false)

	index, evictkey, evict = cmap.Insert(id2)
	tests.Assert(t, cmap.bds[0].Key == id1)
	tests.Assert(t, cmap.bds[0].Mru == false)
	tests.Assert(t, cmap.bds[0].Used == true)
	tests.Assert(t, cmap.bds[1].Key == id2)
	tests.Assert(t, cmap.bds[1].Mru == false)
	tests.Assert(t, cmap.bds[1].Used == true)
	tests.Assert(t, index == 1)
	tests.Assert(t, evictkey == INVALID_KEY)
	tests.Assert(t, evict == false)

	cmap.Using(0)
	tests.Assert(t, cmap.bds[0].Key == id1)
	tests.Assert(t, cmap.bds[0].Mru == true)
	tests.Assert(t, cmap.bds[0].Used == true)

	index, evictkey, evict = cmap.Insert(id3)
	tests.Assert(t, cmap.bds[0].Key == id1)
	tests.Assert(t, cmap.bds[0].Mru == false)
	tests.Assert(t, cmap.bds[0].Used == true)
	tests.Assert(t, cmap.bds[1].Key == id3)
	tests.Assert(t, cmap.bds[1].Mru == false)
	tests.Assert(t, cmap.bds[1].Used == true)
	tests.Assert(t, index == 1)
	tests.Assert(t, evictkey == id2)
	tests.Assert(t, evict == true)

	cmap.Free(1)
	tests.Assert(t, cmap.bds[1].Mru == false)
	tests.Assert(t, cmap.bds[1].Used == false)

	index, evictkey, evict = cmap.Insert(id2)
	tests.Assert(t, cmap.bds[0].Key == id2)
	tests.Assert(t, cmap.bds[0].Mru == false)
	tests.Assert(t, cmap.bds[0].Used == true)
	tests.Assert(t, cmap.bds[1].Mru == false)
	tests.Assert(t, cmap.bds[1].Used == false)
	tests.Assert(t, index == 0)
	tests.Assert(t, evictkey == id1)
	tests.Assert(t, evict == true)
}
