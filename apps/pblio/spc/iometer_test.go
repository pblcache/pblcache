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
	"github.com/lpabon/goioworkload/spc1"
	"github.com/pblcache/pblcache/tests"
	"strings"
	"testing"
	"time"
)

func TestIoMeterCollect(t *testing.T) {

	spc1info := &spc1.Spc1Io{
		Asu:     1,
		Blocks:  1,
		Stream:  2,
		Address: 123456,
		When:    time.Second * 2,
	}

	stat := &IoStats{
		Io:      spc1info,
		Start:   time.Now(),
		Latency: time.Millisecond * 3,
	}

	// Inster first data point
	meter := &IoMeter{}

	meter.Collect(stat)
	tests.Assert(t, meter.Blocks == 1)
	tests.Assert(t, meter.Ios == 1)
	tests.Assert(t, meter.Latency.MeanTimeUsecs() == 3000.0)

	// Inster second data point
	spc1info.Blocks = 4
	stat.Latency = time.Millisecond
	meter.Collect(stat)
	tests.Assert(t, meter.Blocks == 5)
	tests.Assert(t, meter.Ios == 2)
	tests.Assert(t, meter.Latency.MeanTimeUsecs() == 2000.0)

}

func TestIoMeterCsvDelta(t *testing.T) {
	spc1info := &spc1.Spc1Io{
		Asu:     1,
		Blocks:  1,
		Stream:  2,
		Address: 123456,
		When:    time.Second * 2,
	}

	stat := &IoStats{
		Io:      spc1info,
		Start:   time.Now(),
		Latency: time.Millisecond * 3,
	}

	// Inster first data point
	meter := &IoMeter{}

	meter.Collect(stat)
	tests.Assert(t, meter.Blocks == 1)
	tests.Assert(t, meter.Ios == 1)
	tests.Assert(t, meter.Latency.MeanTimeUsecs() == 3000.0)

	prev := &IoMeter{}
	s := meter.CsvDelta(prev, time.Millisecond)
	split := strings.Split(s, ",")
	tests.Assert(t, split[0] == "1")
	tests.Assert(t, split[1] == "4096")
	tests.Assert(t, strings.Contains(split[2], "3.9"))
	tests.Assert(t, strings.Contains(split[3], "3000"))
}

func TestIoMeterDeltas(t *testing.T) {

	prev := &IoMeter{
		Ios:    1000,
		Blocks: 5000,
	}
	prev.Latency.Add(time.Millisecond)
	prev.Latency.Add(time.Millisecond)
	prev.Latency.Add(time.Millisecond)
	prev.Latency.Add(time.Millisecond)

	meter := &IoMeter{}
	*meter = *prev
	meter.Ios += 500
	meter.Blocks += 500
	meter.Latency.Add(time.Millisecond * 2)
	meter.Latency.Add(time.Millisecond * 2)
	meter.Latency.Add(time.Millisecond * 2)
	meter.Latency.Add(time.Millisecond * 2)

	// Test Accessors
	tests.Assert(t, meter.MeanLatencyDeltaUsecs(prev) == 2000.0)
	tests.Assert(t, meter.IosDelta(prev) == 500)
	tests.Assert(t, meter.MeanLatencyUsecs() == 1500.0)
}
