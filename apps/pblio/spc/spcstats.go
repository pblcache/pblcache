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
	"github.com/lpabon/tm"
	"time"
)

type SpcStats struct {
	total, read, write IoMeter

	Asustats []*AsuStats
	Latency  tm.TimeDuration
}

// dataperiod in seconds is used to calculate MB/s
func NewSpcStats() *SpcStats {
	s := &SpcStats{
		Asustats: make([]*AsuStats, 3),
	}

	for asu := 0; asu < 3; asu++ {
		s.Asustats[asu] = NewAsuStats()
	}

	return s
}

func (s *SpcStats) Copy() *SpcStats {
	c := NewSpcStats()
	*c = *s

	c.Asustats = make([]*AsuStats, 3)
	for asu := 0; asu < 3; asu++ {
		c.Asustats[asu] = NewAsuStats()
		*c.Asustats[asu] = *s.Asustats[asu]
	}

	return c
}

func (s *SpcStats) Collect(iostat *IoStats) {
	// Save Asu stats
	s.Asustats[iostat.Io.Asu-1].Collect(iostat)

	// Collect total stats
	s.total.Collect(iostat)

	if iostat.Io.Isread {
		s.read.Collect(iostat)
	} else {
		s.write.Collect(iostat)
	}
}

func (s *SpcStats) CsvDelta(prev *SpcStats, delta time.Duration) string {
	return s.read.CsvDelta(&prev.read, delta) +
		s.write.CsvDelta(&prev.write, delta) +
		s.total.CsvDelta(&prev.total, delta) +
		s.Asustats[0].CsvDelta(prev.Asustats[0], delta) +
		s.Asustats[1].CsvDelta(prev.Asustats[1], delta) +
		s.Asustats[2].CsvDelta(prev.Asustats[2], delta)
}

func (s *SpcStats) LatencyDeltaUsecs(prev *SpcStats) float64 {
	return s.total.LatencyDeltaUsecs(&prev.total)
}

func (s *SpcStats) LatencyUsecs() float64 {
	return s.total.LatencyUsecs()
}

func (s *SpcStats) IosDelta(prev *SpcStats) uint64 {
	return s.total.IosDelta(&prev.total)
}
