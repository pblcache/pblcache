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

func TestNewSpcStats(t *testing.T) {
	s := NewSpcStats()
	tests.Assert(t, len(s.Asustats) == 3)
	tests.Assert(t, s.Asustats[ASU1] != nil)
	tests.Assert(t, s.Asustats[ASU2] != nil)
	tests.Assert(t, s.Asustats[ASU3] != nil)
}

func TestSpcStatsCopy(t *testing.T) {
	s := NewSpcStats()
	c := s.Copy()

	tests.Assert(t, len(c.Asustats) == 3)
	tests.Assert(t, c.Asustats[ASU1] != nil)
	tests.Assert(t, c.Asustats[ASU2] != nil)
	tests.Assert(t, c.Asustats[ASU3] != nil)

	tests.Assert(t, c.Asustats[ASU1] != s.Asustats[ASU1])
	tests.Assert(t, c.Asustats[ASU2] != s.Asustats[ASU2])
	tests.Assert(t, c.Asustats[ASU3] != s.Asustats[ASU3])
}

func TestSpcStats(t *testing.T) {

	// Send to ASU1
	spc1info := &spc1.Spc1Io{
		Asu:    1,
		Blocks: 2,
		Isread: true,
		Stream: 2,
		Offset: 123456,
		When:   time.Second * 2,
	}

	stat := &IoStats{
		Io:      spc1info,
		Start:   time.Now(),
		Latency: time.Millisecond * 3,
	}

	s := NewSpcStats()
	s.Collect(stat)

	tests.Assert(t, s.Asustats[ASU1].read.ios == 1)
	tests.Assert(t, s.Asustats[ASU1].read.blocks == 2)
	tests.Assert(t, s.Asustats[ASU1].write.ios == 0)
	tests.Assert(t, s.Asustats[ASU1].write.blocks == 0)
	tests.Assert(t, s.Asustats[ASU1].total.ios == 1)
	tests.Assert(t, s.Asustats[ASU1].total.blocks == 2)

	tests.Assert(t, s.Asustats[ASU2].read.ios == 0)
	tests.Assert(t, s.Asustats[ASU2].read.blocks == 0)
	tests.Assert(t, s.Asustats[ASU2].write.ios == 0)
	tests.Assert(t, s.Asustats[ASU2].write.blocks == 0)
	tests.Assert(t, s.Asustats[ASU2].total.ios == 0)
	tests.Assert(t, s.Asustats[ASU2].total.blocks == 0)

	tests.Assert(t, s.Asustats[ASU3].read.ios == 0)
	tests.Assert(t, s.Asustats[ASU3].read.blocks == 0)
	tests.Assert(t, s.Asustats[ASU3].write.ios == 0)
	tests.Assert(t, s.Asustats[ASU3].write.blocks == 0)
	tests.Assert(t, s.Asustats[ASU3].total.ios == 0)
	tests.Assert(t, s.Asustats[ASU3].total.blocks == 0)

	tests.Assert(t, s.read.ios == 1)
	tests.Assert(t, s.read.blocks == 2)
	tests.Assert(t, s.write.ios == 0)
	tests.Assert(t, s.write.blocks == 0)
	tests.Assert(t, s.total.ios == 1)
	tests.Assert(t, s.total.blocks == 2)

	// For CsvDelta test
	prev := s.Copy()

	// Send Read to ASU1
	s.Collect(stat)

	tests.Assert(t, s.Asustats[ASU1].read.ios == 2)
	tests.Assert(t, s.Asustats[ASU1].read.blocks == 4)
	tests.Assert(t, s.Asustats[ASU1].write.ios == 0)
	tests.Assert(t, s.Asustats[ASU1].write.blocks == 0)
	tests.Assert(t, s.Asustats[ASU1].total.ios == 2)
	tests.Assert(t, s.Asustats[ASU1].total.blocks == 4)

	tests.Assert(t, s.Asustats[ASU2].read.ios == 0)
	tests.Assert(t, s.Asustats[ASU2].read.blocks == 0)
	tests.Assert(t, s.Asustats[ASU2].write.ios == 0)
	tests.Assert(t, s.Asustats[ASU2].write.blocks == 0)
	tests.Assert(t, s.Asustats[ASU2].total.ios == 0)
	tests.Assert(t, s.Asustats[ASU2].total.blocks == 0)

	tests.Assert(t, s.Asustats[ASU3].read.ios == 0)
	tests.Assert(t, s.Asustats[ASU3].read.blocks == 0)
	tests.Assert(t, s.Asustats[ASU3].write.ios == 0)
	tests.Assert(t, s.Asustats[ASU3].write.blocks == 0)
	tests.Assert(t, s.Asustats[ASU3].total.ios == 0)
	tests.Assert(t, s.Asustats[ASU3].total.blocks == 0)

	tests.Assert(t, s.read.ios == 2)
	tests.Assert(t, s.read.blocks == 4)
	tests.Assert(t, s.write.ios == 0)
	tests.Assert(t, s.write.blocks == 0)
	tests.Assert(t, s.total.ios == 2)
	tests.Assert(t, s.total.blocks == 4)

	// Send Write to ASU3
	spc1info.Asu = 3
	spc1info.Isread = false
	spc1info.Blocks = 4
	s.Collect(stat)

	tests.Assert(t, s.Asustats[ASU1].read.ios == 2)
	tests.Assert(t, s.Asustats[ASU1].read.blocks == 4)
	tests.Assert(t, s.Asustats[ASU1].write.ios == 0)
	tests.Assert(t, s.Asustats[ASU1].write.blocks == 0)
	tests.Assert(t, s.Asustats[ASU1].total.ios == 2)
	tests.Assert(t, s.Asustats[ASU1].total.blocks == 4)

	tests.Assert(t, s.Asustats[ASU2].read.ios == 0)
	tests.Assert(t, s.Asustats[ASU2].read.blocks == 0)
	tests.Assert(t, s.Asustats[ASU2].write.ios == 0)
	tests.Assert(t, s.Asustats[ASU2].write.blocks == 0)
	tests.Assert(t, s.Asustats[ASU2].total.ios == 0)
	tests.Assert(t, s.Asustats[ASU2].total.blocks == 0)

	tests.Assert(t, s.Asustats[ASU3].read.ios == 0)
	tests.Assert(t, s.Asustats[ASU3].read.blocks == 0)
	tests.Assert(t, s.Asustats[ASU3].write.ios == 1)
	tests.Assert(t, s.Asustats[ASU3].write.blocks == 4)
	tests.Assert(t, s.Asustats[ASU3].total.ios == 1)
	tests.Assert(t, s.Asustats[ASU3].total.blocks == 4)

	tests.Assert(t, s.read.ios == 2)
	tests.Assert(t, s.read.blocks == 4)
	tests.Assert(t, s.write.ios == 1)
	tests.Assert(t, s.write.blocks == 4)
	tests.Assert(t, s.total.ios == 3)
	tests.Assert(t, s.total.blocks == 8)

	// Send Write to ASU2
	spc1info.Asu = 2
	spc1info.Isread = false
	spc1info.Blocks = 8
	s.Collect(stat)

	tests.Assert(t, s.Asustats[ASU1].read.ios == 2)
	tests.Assert(t, s.Asustats[ASU1].read.blocks == 4)
	tests.Assert(t, s.Asustats[ASU1].write.ios == 0)
	tests.Assert(t, s.Asustats[ASU1].write.blocks == 0)
	tests.Assert(t, s.Asustats[ASU1].total.ios == 2)
	tests.Assert(t, s.Asustats[ASU1].total.blocks == 4)

	tests.Assert(t, s.Asustats[ASU2].read.ios == 0)
	tests.Assert(t, s.Asustats[ASU2].read.blocks == 0)
	tests.Assert(t, s.Asustats[ASU2].write.ios == 1)
	tests.Assert(t, s.Asustats[ASU2].write.blocks == 8)
	tests.Assert(t, s.Asustats[ASU2].total.ios == 1)
	tests.Assert(t, s.Asustats[ASU2].total.blocks == 8)

	tests.Assert(t, s.Asustats[ASU3].read.ios == 0)
	tests.Assert(t, s.Asustats[ASU3].read.blocks == 0)
	tests.Assert(t, s.Asustats[ASU3].write.ios == 1)
	tests.Assert(t, s.Asustats[ASU3].write.blocks == 4)
	tests.Assert(t, s.Asustats[ASU3].total.ios == 1)
	tests.Assert(t, s.Asustats[ASU3].total.blocks == 4)

	tests.Assert(t, s.read.ios == 2)
	tests.Assert(t, s.read.blocks == 4)
	tests.Assert(t, s.write.ios == 2)
	tests.Assert(t, s.write.blocks == 12)
	tests.Assert(t, s.total.ios == 4)
	tests.Assert(t, s.total.blocks == 16)

	// Test CsvDelta
	// These are a delta stat, they are equal to (s-prev)
	csv := s.CsvDelta(prev, time.Millisecond)
	split := strings.Split(csv, ",")

	// Total Reads
	tests.Assert(t, split[0] == "1")
	tests.Assert(t, split[1] == fmt.Sprintf("%v", 2*4*KB))

	// Total Writes
	tests.Assert(t, split[4] == "2")
	tests.Assert(t, split[5] == fmt.Sprintf("%v", 12*4*KB))

	// Total
	tests.Assert(t, split[8] == "3")
	tests.Assert(t, split[9] == fmt.Sprintf("%v", 14*4*KB))

	latency := s.MeanLatencyDeltaUsecs(prev)
	tests.Assert(t, latency == 3000.0)

	latency = s.MeanLatencyUsecs()
	tests.Assert(t, latency == 3000.0)

	ios := s.IosDelta(prev)
	tests.Assert(t, ios == 3)
}
