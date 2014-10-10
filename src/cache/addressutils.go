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
)

const (
	MAX_LBA = uint64(1 << 48)
)

type Address struct {
	devid uint16
	lba   uint64
}

func Address64(address Address) uint64 {
	godbc.Require(address.lba < MAX_LBA)
	return (uint64(address.devid) << 48) | uint64(address.lba)
}

func AddressValue(address uint64) Address {
	var a Address

	a.devid = uint16(address >> 48)
	a.lba = uint64(0xFFFFFFFFFFFF) & address

	return a
}
