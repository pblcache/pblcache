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

type logstats struct {
	ramhits         uint64
	storagehits     uint64
	wraps           uint64
	seg_skipped     uint64
	bufferhits      uint64
	totalhits       uint64
	readtime        tm.TimeDuration
	segmentreadtime tm.TimeDuration
	writetime       tm.TimeDuration
	lock            sync.Mutex
}

type LogStats struct {
	Ramhits         uint64  `json:"ramhits"`
	Storagehits     uint64  `json:"storagehits"`
	Wraps           uint64  `json:"wraps"`
	Seg_skipped     uint64  `json:"segments_skipped"`
	Bufferhits      uint64  `json:"buffercachehits"`
	Totalhits       uint64  `json:"totalhits"`
	Readtime        float64 `json:"mean_read_usecs"`
	Segmentreadtime float64 `json:"mean_segmentread_usecs"`
	Writetime       float64 `json:"mean_segmentwrite_usecs"`
}

func (s *logstats) Stats() *LogStats {
	scopy := &logstats{}
	s.lock.Lock()
	*scopy = *s
	s.lock.Unlock()

	return &LogStats{
		Ramhits:         scopy.ramhits,
		Storagehits:     scopy.storagehits,
		Wraps:           scopy.wraps,
		Seg_skipped:     scopy.seg_skipped,
		Bufferhits:      scopy.bufferhits,
		Totalhits:       scopy.totalhits,
		Readtime:        scopy.readtime.MeanTimeUsecs(),
		Segmentreadtime: scopy.segmentreadtime.MeanTimeUsecs(),
		Writetime:       scopy.writetime.MeanTimeUsecs(),
	}
}

func (s *logstats) BufferHit() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.bufferhits++
	s.totalhits++
}

func (s *logstats) SegmentSkipped() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.seg_skipped++
}

func (s *logstats) RamHit() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.ramhits++
	s.totalhits++
}

func (s *logstats) StorageHit() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.storagehits++
	s.totalhits++
}

func (s *logstats) Wrapped() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.wraps++
}

func (s *logstats) ReadTimeRecord(d time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.readtime.Add(d)
}

func (s *logstats) WriteTimeRecord(d time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.writetime.Add(d)
}

func (s *logstats) SegmentReadTimeRecord(d time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.segmentreadtime.Add(d)
}

func (s *logstats) ramHitRate() float64 {
	if 0 == s.totalhits {
		return 0.0
	} else {
		return float64(s.ramhits) / float64(s.totalhits)
	}
}

func (s *logstats) bufferHitRate() float64 {
	if 0 == s.totalhits {
		return 0.0
	} else {
		return float64(s.bufferhits) / float64(s.totalhits)
	}
}

func (s *logstats) String() string {
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
