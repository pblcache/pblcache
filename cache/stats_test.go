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
	"fmt"
	"github.com/pblcache/pblcache/tests"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestCacheStatsReadHits(t *testing.T) {
	s := cachestats{}
	s.readHit()
	tests.Assert(t, s.readhits == 1)
	tests.Assert(t, s.invalidatehits == 0)
	tests.Assert(t, s.reads == 0)
	tests.Assert(t, s.evictions == 0)
	tests.Assert(t, s.invalidations == 0)
	tests.Assert(t, s.insertions == 0)
}

func TestCacheStatsInvalidateHits(t *testing.T) {
	s := cachestats{}
	s.invalidateHit()
	tests.Assert(t, s.readhits == 0)
	tests.Assert(t, s.invalidatehits == 1)
	tests.Assert(t, s.reads == 0)
	tests.Assert(t, s.evictions == 0)
	tests.Assert(t, s.invalidations == 0)
	tests.Assert(t, s.insertions == 0)
}

func TestCacheStatsReads(t *testing.T) {
	s := cachestats{}
	s.read()
	tests.Assert(t, s.readhits == 0)
	tests.Assert(t, s.invalidatehits == 0)
	tests.Assert(t, s.reads == 1)
	tests.Assert(t, s.evictions == 0)
	tests.Assert(t, s.invalidations == 0)
	tests.Assert(t, s.insertions == 0)
}

func TestCacheStatsEvictions(t *testing.T) {
	s := cachestats{}
	s.eviction()
	tests.Assert(t, s.readhits == 0)
	tests.Assert(t, s.invalidatehits == 0)
	tests.Assert(t, s.reads == 0)
	tests.Assert(t, s.evictions == 1)
	tests.Assert(t, s.invalidations == 0)
	tests.Assert(t, s.insertions == 0)
}

func TestCacheStatsInvalidations(t *testing.T) {
	s := cachestats{}
	s.invalidation()
	tests.Assert(t, s.readhits == 0)
	tests.Assert(t, s.invalidatehits == 0)
	tests.Assert(t, s.reads == 0)
	tests.Assert(t, s.evictions == 0)
	tests.Assert(t, s.invalidations == 1)
	tests.Assert(t, s.insertions == 0)
}

func TestCacheStatsInsertions(t *testing.T) {
	s := cachestats{}
	s.insertion()
	tests.Assert(t, s.readhits == 0)
	tests.Assert(t, s.invalidatehits == 0)
	tests.Assert(t, s.reads == 0)
	tests.Assert(t, s.evictions == 0)
	tests.Assert(t, s.invalidations == 0)
	tests.Assert(t, s.insertions == 1)
}

func TestCacheStatsClear(t *testing.T) {
	s := &cachestats{
		readhits:       1,
		invalidatehits: 12,
		reads:          123,
		evictions:      1234,
		invalidations:  12345,
		insertions:     123456,
	}

	s.clear()
	tests.Assert(t, s.readhits == 0)
	tests.Assert(t, s.invalidatehits == 0)
	tests.Assert(t, s.reads == 0)
	tests.Assert(t, s.evictions == 0)
	tests.Assert(t, s.invalidations == 0)
	tests.Assert(t, s.insertions == 0)
}

func TestCacheStatsCsv(t *testing.T) {
	s := &cachestats{
		readhits:       1,
		invalidatehits: 12,
		reads:          123,
		evictions:      1234,
		invalidations:  12345,
		insertions:     123456,
	}

	stats := s.stats()
	slice := strings.Split(stats.Csv(), ",")

	// 8 elements per csv line
	tests.Assert(t, len(slice) == 8)
	tests.Assert(t, slice[0] == fmt.Sprintf("%v", stats.ReadHitRate()))
	tests.Assert(t, slice[1] == fmt.Sprintf("%v", stats.InvalidateHitRate()))
	tests.Assert(t, slice[2] == strconv.FormatUint(s.readhits, 10))
	tests.Assert(t, slice[3] == strconv.FormatUint(s.invalidatehits, 10))
	tests.Assert(t, slice[4] == strconv.FormatUint(s.reads, 10))
	tests.Assert(t, slice[5] == strconv.FormatUint(s.insertions, 10))
	tests.Assert(t, slice[6] == strconv.FormatUint(s.evictions, 10))
	tests.Assert(t, slice[7] == strconv.FormatUint(s.invalidations, 10))
}

func TestCacheStatsCsvDelta(t *testing.T) {
	s1 := &cachestats{
		readhits:       1,
		invalidatehits: 12,
		reads:          123,
		evictions:      1234,
		invalidations:  12345,
		insertions:     123456,
	}
	s2 := &cachestats{
		readhits:       12,
		invalidatehits: 123,
		reads:          1234,
		evictions:      12345,
		invalidations:  123456,
		insertions:     1234567,
	}

	stats1 := s1.stats()
	stats2 := s2.stats()
	slice := strings.Split(stats2.CsvDelta(stats1), ",")

	// 8 elements per csv line
	tests.Assert(t, len(slice) == 8)
	tests.Assert(t, slice[0] == fmt.Sprintf("%v", stats2.ReadHitRateDelta(stats1)))
	tests.Assert(t, slice[1] == fmt.Sprintf("%v", stats2.InvalidateHitRateDelta(stats1)))
	tests.Assert(t, slice[2] == strconv.FormatUint(s2.readhits-s1.readhits, 10))
	tests.Assert(t, slice[3] == strconv.FormatUint(s2.invalidatehits-s1.invalidatehits, 10))
	tests.Assert(t, slice[4] == strconv.FormatUint(s2.reads-s1.reads, 10))
	tests.Assert(t, slice[5] == strconv.FormatUint(s2.insertions-s1.insertions, 10))
	tests.Assert(t, slice[6] == strconv.FormatUint(s2.evictions-s1.evictions, 10))
	tests.Assert(t, slice[7] == strconv.FormatUint(s2.invalidations-s1.invalidations, 10))
}

func TestCacheStatsRates(t *testing.T) {
	s := &cachestats{}
	stats1 := s.stats()
	stats2 := s.stats()

	tests.Assert(t, stats1.ReadHitRate() == 0.0)
	tests.Assert(t, stats1.InvalidateHitRate() == 0.0)
	tests.Assert(t, stats1.ReadHitRateDelta(stats2) == 0.0)
	tests.Assert(t, stats1.InvalidateHitRateDelta(stats2) == 0.0)
}

func TestCacheStatsJson(t *testing.T) {
	s := &cachestats{
		readhits:       1,
		invalidatehits: 12,
		reads:          123,
		evictions:      1234,
		invalidations:  12345,
		insertions:     123456,
	}

	// Encode
	exportedstats := s.stats()
	jsonstats, err := json.Marshal(exportedstats)
	tests.Assert(t, err == nil)

	// Decode and check with original
	decstats := &CacheStats{}
	err = json.Unmarshal(jsonstats, &decstats)
	tests.Assert(t, err == nil)
	tests.Assert(t, reflect.DeepEqual(decstats, exportedstats))
	tests.Assert(t, s.readhits == decstats.Readhits)
	tests.Assert(t, s.invalidatehits == decstats.Invalidatehits)
	tests.Assert(t, s.reads == decstats.Reads)
	tests.Assert(t, s.evictions == decstats.Evictions)
	tests.Assert(t, s.invalidations == decstats.Invalidations)
	tests.Assert(t, s.insertions == decstats.Insertions)

}
