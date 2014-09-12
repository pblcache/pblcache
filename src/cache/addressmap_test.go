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
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
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

func concurrentTester(t *testing.T,
	a *AddressMap,
	wg *sync.WaitGroup,
	count int) {

	defer wg.Done()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < count; i++ {
		objid := uint64(r.Int63())
		lba := uint64(r.Int63())
		val := uint64(r.Int63())

		a.Set(objid, lba, val)

		time.Sleep(time.Microsecond)

		index, ok := a.Get(objid, lba)
		assert(t, index == val)
		assert(t, ok == true)
	}

}

func TestSet(t *testing.T) {
	a := NewAddressMap()
	a.Set(1, 2, 3)
	assert(t, a.addressmap[AddressMapKey{1, 2}] == 3)
}

func TestGet(t *testing.T) {
	a := NewAddressMap()
	a.Set(1, 2, 3)

	index, ok := a.Get(1, 2)
	assert(t, index == 3)
	assert(t, ok == true)

	index, ok = a.Get(10, 10)
	assert(t, ok == false)
}

func TestConcurrent(t *testing.T) {

	var wg sync.WaitGroup

	a := NewAddressMap()
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go concurrentTester(t, a, &wg, 500)
	}

	wg.Wait()
}

func BenchmarkSet(b *testing.B) {
	a := NewAddressMap()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ui := uint64(i)
		a.Set(ui, ui, ui)
	}
}

func BenchmarkGet(b *testing.B) {
	a := NewAddressMap()

	for i := 0; i < b.N; i++ {
		ui := uint64(i)
		a.Set(ui, ui, ui)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ui := uint64(i)
		a.Get(ui, ui)
	}
}
