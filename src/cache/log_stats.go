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
	"fmt"
	"github.com/lpabon/tm"
	"sync"
	"time"
)

type LogStats struct {
	ramhits         uint64
	storagehits     uint64
	wraps           uint64
	seg_skipped     uint64
	bufferhits      uint64
	totalhits       uint64
	readtime        *tm.TimeDuration
	segmentreadtime *tm.TimeDuration
	writetime       *tm.TimeDuration
	lock            sync.Mutex
}

func NewLogStats() *LogStats {

	stats := &LogStats{}
	stats.readtime = &tm.TimeDuration{}
	stats.segmentreadtime = &tm.TimeDuration{}
	stats.writetime = &tm.TimeDuration{}

	return stats
}

func (s *LogStats) Close() {

}

func (s *LogStats) BufferHit() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.bufferhits++
	s.totalhits++
}

func (s *LogStats) SegmentSkipped() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.seg_skipped++
}

func (s *LogStats) RamHit() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.ramhits++
	s.totalhits++
}

func (s *LogStats) StorageHit() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.storagehits++
	s.totalhits++
}

func (s *LogStats) Wrapped() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.wraps++
}

func (s *LogStats) ReadTimeRecord(d time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.readtime.Add(d)
}

func (s *LogStats) WriteTimeRecord(d time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.writetime.Add(d)
}

func (s *LogStats) SegmentReadTimeRecord(d time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.segmentreadtime.Add(d)
}

func (s *LogStats) ramHitRate() float64 {
	if 0 == s.totalhits {
		return 0.0
	} else {
		return float64(s.ramhits) / float64(s.totalhits)
	}
}

func (s *LogStats) bufferHitRate() float64 {
	if 0 == s.totalhits {
		return 0.0
	} else {
		return float64(s.bufferhits) / float64(s.totalhits)
	}
}

func (s *LogStats) String() string {
	s.lock.Lock()
	defer s.lock.Unlock()

	return fmt.Sprintf("Ram Hit Rate: %.4f\n"+
		"Ram Hits: %v\n"+
		"Buffer Hit Rate: %.4f\n"+
		"Buffer Hits: %v\n"+
		"Storage Hits: %v\n"+
		"Wraps: %v\n"+
		"Segments Skipped: %v\n"+
		"Mean Read Latency: %.2f usec\n"+
		"Mean Segment Read Latency: %.2f usec\n"+
		"Mean Write Latency: %.2f usec\n",
		s.ramHitRate(),
		s.ramhits,
		s.bufferHitRate(),
		s.bufferhits,
		s.storagehits,
		s.wraps,
		s.seg_skipped,
		s.readtime.MeanTimeUsecs(),
		s.segmentreadtime.MeanTimeUsecs(),
		s.writetime.MeanTimeUsecs()) // + s.readtime.String() + s.writetime.String()
}
