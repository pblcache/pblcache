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

const (
	MAX_BLOCKS = uint64(1 << 32)
)

type Address struct {
	Devid uint32
	Block uint32
}

func Address64(address Address) uint64 {
	return (uint64(address.Devid) << 32) | uint64(address.Block)
}

func AddressValue(address uint64) Address {
	var a Address

	a.Devid = uint32(address >> 32)
	a.Block = uint32(address)

	return a
}
