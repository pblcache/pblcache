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
	"fmt"
	"github.com/lpabon/goioworkload/spc1"
	"github.com/pblcache/pblcache/tests"
	"strings"
	"testing"
	"time"
)

func TestAsuStatCollect(t *testing.T) {

	spc1info := &spc1.Spc1Io{
		Asu:     1,
		Blocks:  1,
		Isread:  true,
		Stream:  2,
		Address: 123456,
		When:    time.Second * 2,
	}

	stat := &IoStats{
		Io:      spc1info,
		Start:   time.Now(),
		Latency: time.Millisecond * 3,
	}

	asumeter := &AsuStats{}
	asumeter.Collect(stat)
	tests.Assert(t, asumeter.Read.Blocks == 1)
	tests.Assert(t, asumeter.Write.Blocks == 0)
	tests.Assert(t, asumeter.Total.Blocks == 1)

	// Inster second data point
	spc1info.Blocks = 4
	spc1info.Isread = false
	stat.Latency = time.Millisecond
	asumeter.Collect(stat)
	tests.Assert(t, asumeter.Read.Blocks == 1)
	tests.Assert(t, asumeter.Write.Blocks == 4)
	tests.Assert(t, asumeter.Total.Blocks == 5)

}

func TestAsuStatCsvDelta(t *testing.T) {
	spc1info := &spc1.Spc1Io{
		Asu:     1,
		Blocks:  1,
		Isread:  true,
		Stream:  2,
		Address: 123456,
		When:    time.Second * 2,
	}

	stat := &IoStats{
		Io:      spc1info,
		Start:   time.Now(),
		Latency: time.Millisecond * 3,
	}

	asumeter := &AsuStats{}
	asumeter.Collect(stat)
	prev := &AsuStats{}
	*prev = *asumeter

	// Inster second data point
	spc1info.Blocks = 4
	spc1info.Isread = false
	stat.Latency = time.Millisecond
	asumeter.Collect(stat)

	spc1info.Blocks = 2
	spc1info.Isread = false
	stat.Latency = time.Millisecond
	asumeter.Collect(stat)

	spc1info.Blocks = 1
	spc1info.Isread = true
	stat.Latency = time.Millisecond
	asumeter.Collect(stat)

	s := asumeter.CsvDelta(prev, time.Millisecond)
	split := strings.Split(s, ",")

	// Read
	tests.Assert(t, split[0] == "1")
	tests.Assert(t, split[1] == "4096")

	// Write
	tests.Assert(t, split[4] == "2")                       // 1 io
	tests.Assert(t, split[5] == fmt.Sprintf("%v", 4*KB*6)) // 6 4KB Blocks

	// Total
	tests.Assert(t, split[8] == "3")
	tests.Assert(t, split[9] == fmt.Sprintf("%v", 4*KB*7)) // 7 4KB Blocks
}
