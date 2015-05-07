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

func TestAddressUtils(t *testing.T) {
	a := Address{
		Devid: 9876,
		Block: 123456789,
	}

	merged := Address64(a)
	converted := AddressValue(merged)

	tests.Assert(t, a.Devid == converted.Devid)
	tests.Assert(t, a.Block == converted.Block)
}
