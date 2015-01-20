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
package spc

import (
	"github.com/pblcache/pblcache/tests"
	"testing"
)

func TestAsuNew(t *testing.T) {
	usedirectio := false
	asu := NewAsu(usedirectio)
	tests.Assert(t, asu.usedirectio == usedirectio)
	tests.Assert(t, len(asu.fps) == 0)
	tests.Assert(t, asu.len == 0)

	usedirectio = true
	asu = NewAsu(usedirectio)
	tests.Assert(t, asu.usedirectio == usedirectio)
	tests.Assert(t, len(asu.fps) == 0)
	tests.Assert(t, asu.len == 0)
}

func TestAsuSize(t *testing.T) {

	usedirectio := false
	asu := NewAsu(usedirectio)

	// set fake length
	asu.len = 1234 * GB / (4 * KB)
	tests.Assert(t, float64(1234) == asu.Size())
}
