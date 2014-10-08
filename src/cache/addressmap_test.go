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
	"runtime"
	"testing"
)

func assert(t *testing.T, b bool) {
	if !b {
		pc, file, line, _ := runtime.Caller(1)
		caller_func_info := runtime.FuncForPC(pc)

		t.Errorf("\n\rASSERT:\tfunc (%s) 0x%x\n\r\tFile %s:%d",
			caller_func_info.Name(),
			pc,
			file,
			line)
	}
}

func TestSet(t *testing.T) {
	a := NewAddressMap()
	a.Set(1, 2)
	assert(t, a.addressmap[1] == 2)
}

func TestGet(t *testing.T) {
	a := NewAddressMap()
	a.Set(1, 2)

	index, ok := a.Get(1)
	assert(t, index == 2)
	assert(t, ok == true)

	_, ok = a.Get(10)
	assert(t, ok == false)
}

func TestDelete(t *testing.T) {
	a := NewAddressMap()
	a.Set(1, 2)

	index, ok := a.Get(1)
	assert(t, index == 2)
	assert(t, ok == true)
	assert(t, a.Has(1))

	a.Delete(1)

	_, ok = a.Get(1)
	assert(t, ok == false)
	assert(t, !a.Has(1))
}

func BenchmarkSet(b *testing.B) {
	a := NewAddressMap()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ui := uint64(i)
		a.Set(ui, ui)
	}
}

func BenchmarkGet(b *testing.B) {
	a := NewAddressMap()

	for i := 0; i < b.N; i++ {
		ui := uint64(i)
		a.Set(ui, ui)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ui := uint64(i)
		a.Get(ui)
	}
}
