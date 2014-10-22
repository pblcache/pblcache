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
	"encoding/json"
	"github.com/pblcache/pblcache/src/tests"
	"reflect"
	"testing"
)

func TestLogStatsRamhits(t *testing.T) {
	s := &logstats{}
	s.RamHit()
	tests.Assert(t, s.ramhits == 1)
	tests.Assert(t, s.storagehits == 0)
	tests.Assert(t, s.wraps == 0)
	tests.Assert(t, s.seg_skipped == 0)
	tests.Assert(t, s.bufferhits == 0)
	tests.Assert(t, s.totalhits == 1)
}

func TestLogStatsBufferHit(t *testing.T) {
	s := &logstats{}
	s.BufferHit()
	tests.Assert(t, s.ramhits == 0)
	tests.Assert(t, s.storagehits == 0)
	tests.Assert(t, s.wraps == 0)
	tests.Assert(t, s.seg_skipped == 0)
	tests.Assert(t, s.bufferhits == 1)
	tests.Assert(t, s.totalhits == 1)
}

func TestLogStatsSegmentSkipped(t *testing.T) {
	s := &logstats{}
	s.SegmentSkipped()
	tests.Assert(t, s.ramhits == 0)
	tests.Assert(t, s.storagehits == 0)
	tests.Assert(t, s.wraps == 0)
	tests.Assert(t, s.seg_skipped == 1)
	tests.Assert(t, s.bufferhits == 0)
	tests.Assert(t, s.totalhits == 0)
}

func TestLogStatsStorageHit(t *testing.T) {
	s := &logstats{}
	s.StorageHit()
	tests.Assert(t, s.ramhits == 0)
	tests.Assert(t, s.storagehits == 1)
	tests.Assert(t, s.wraps == 0)
	tests.Assert(t, s.seg_skipped == 0)
	tests.Assert(t, s.bufferhits == 0)
	tests.Assert(t, s.totalhits == 1)
}

func TestLogStatsWrapped(t *testing.T) {
	s := &logstats{}
	s.Wrapped()
	tests.Assert(t, s.ramhits == 0)
	tests.Assert(t, s.storagehits == 0)
	tests.Assert(t, s.wraps == 1)
	tests.Assert(t, s.seg_skipped == 0)
	tests.Assert(t, s.bufferhits == 0)
	tests.Assert(t, s.totalhits == 0)
}

func TestLogStatsReadTimeRecord(t *testing.T) {
	s := &logstats{}

	s.ReadTimeRecord(50 * 1000)
	s.ReadTimeRecord(50 * 1000)

	tests.Assert(t, s.readtime.MeanTimeUsecs() == 50.0)
	tests.Assert(t, s.segmentreadtime.MeanTimeUsecs() == 0.0)
	tests.Assert(t, s.writetime.MeanTimeUsecs() == 0.0)
	tests.Assert(t, s.ramhits == 0)
	tests.Assert(t, s.storagehits == 0)
	tests.Assert(t, s.wraps == 0)
	tests.Assert(t, s.seg_skipped == 0)
	tests.Assert(t, s.bufferhits == 0)
	tests.Assert(t, s.totalhits == 0)
}

func TestLogStatsSegmentReadTimeRecord(t *testing.T) {
	s := &logstats{}

	s.SegmentReadTimeRecord(50 * 1000)
	s.SegmentReadTimeRecord(50 * 1000)

	tests.Assert(t, s.readtime.MeanTimeUsecs() == 0.0)
	tests.Assert(t, s.segmentreadtime.MeanTimeUsecs() == 50.0)
	tests.Assert(t, s.writetime.MeanTimeUsecs() == 0.0)
	tests.Assert(t, s.ramhits == 0)
	tests.Assert(t, s.storagehits == 0)
	tests.Assert(t, s.wraps == 0)
	tests.Assert(t, s.seg_skipped == 0)
	tests.Assert(t, s.bufferhits == 0)
	tests.Assert(t, s.totalhits == 0)
}

func TestLogStatsWriteTimeRecord(t *testing.T) {
	s := &logstats{}

	s.WriteTimeRecord(50 * 1000)
	s.WriteTimeRecord(50 * 1000)

	tests.Assert(t, s.readtime.MeanTimeUsecs() == 0.0)
	tests.Assert(t, s.segmentreadtime.MeanTimeUsecs() == 0.0)
	tests.Assert(t, s.writetime.MeanTimeUsecs() == 50.0)
	tests.Assert(t, s.ramhits == 0)
	tests.Assert(t, s.storagehits == 0)
	tests.Assert(t, s.wraps == 0)
	tests.Assert(t, s.seg_skipped == 0)
	tests.Assert(t, s.bufferhits == 0)
	tests.Assert(t, s.totalhits == 0)
}

func TestRamHitRate(t *testing.T) {
	s := &logstats{}
	ls := s.Stats()
	tests.Assert(t, ls.RamHitRate() == 0.0)

	s.ramhits = 100
	s.totalhits = 200
	ls = s.Stats()
	tests.Assert(t, ls.RamHitRate() == 0.5)
}

func TestBufferHitRate(t *testing.T) {
	s := &logstats{}
	ls := s.Stats()
	tests.Assert(t, ls.BufferHitRate() == 0.0)

	s.bufferhits = 100
	s.totalhits = 200
	ls = s.Stats()
	tests.Assert(t, ls.BufferHitRate() == 0.5)
}

func TestLogStatsJson(t *testing.T) {
	s := &logstats{
		ramhits:     1,
		storagehits: 12,
		wraps:       123,
		seg_skipped: 1234,
		bufferhits:  12345,
		totalhits:   123456,
	}
	s.readtime.Add(1234)
	s.readtime.Add(1234)
	s.readtime.Add(1234)
	s.segmentreadtime.Add(123456)
	s.segmentreadtime.Add(123456)
	s.segmentreadtime.Add(123456)
	s.writetime.Add(1234567)
	s.writetime.Add(1234567)
	s.writetime.Add(1234567)

	// Encode
	exportedstats := s.Stats()
	jsonstats, err := json.Marshal(exportedstats)
	tests.Assert(t, err == nil)

	// Decode and check with original
	decstats := &LogStats{}
	err = json.Unmarshal(jsonstats, &decstats)
	tests.Assert(t, err == nil)
	tests.Assert(t, reflect.DeepEqual(decstats, exportedstats))
	tests.Assert(t, s.ramhits == decstats.Ramhits)
	tests.Assert(t, s.storagehits == decstats.Storagehits)
	tests.Assert(t, s.wraps == decstats.Wraps)
	tests.Assert(t, s.seg_skipped == decstats.Seg_skipped)
	tests.Assert(t, s.bufferhits == decstats.Bufferhits)
	tests.Assert(t, s.totalhits == decstats.Totalhits)
	tests.Assert(t, s.readtime.MeanTimeUsecs() == decstats.Readtime)
	tests.Assert(t, s.segmentreadtime.MeanTimeUsecs() == decstats.Segmentreadtime)
	tests.Assert(t, s.writetime.MeanTimeUsecs() == decstats.Writetime)

}
