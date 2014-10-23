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

/*
import (
	"github.com/lpabon/godbc"
	"sync"
)

type AddressMap struct {
	addressmap map[uint64]uint64
	rwlock     sync.RWMutex
}

func NewAddressMap() *AddressMap {
	a := AddressMap{}

	InitAddressmap(&a)

	return &a

}

func InitAddressmap(a *AddressMap) {
	a.addressmap = make(map[uint64]uint64)
	godbc.Ensure(a.addressmap != nil)
}

func (a *AddressMap) Set(address, index uint64) {
	a.rwlock.Lock()
	defer a.rwlock.Unlock()

	a.addressmap[address] = index
}

func (a *AddressMap) Get(address uint64) (index uint64, found bool) {
	a.rwlock.RLock()
	defer a.rwlock.RUnlock()

	index, found = a.addressmap[address]
	return
}

func (a *AddressMap) Delete(address uint64) {
	a.rwlock.Lock()
	defer a.rwlock.Unlock()

	delete(a.addressmap, address)
}

func (a *AddressMap) Has(address uint64) bool {
	_, ok := a.Get(address)

	return ok
}
*/
