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
	"github.com/lpabon/tm"
	"time"
)

type IoStats struct {
	Io      *spc1.Spc1Io
	Latency time.Duration
}

// --------------------------------------------

type IoMeter struct {
	Latency tm.TimeDuration
	Ios     uint64

	// 4KB Blocks transferred
	Blocks uint64
}

func (i *IoMeter) Collect(iostat *IoStats) {
	i.Ios++
	i.Blocks += uint64(iostat.Io.Blocks)
	i.Latency.Add(iostat.Latency)
}

func (i *IoMeter) CsvDelta(prev *IoMeter) string {
	return fmt.Sprintf("%v,"+ // Ios
		"%v,"+ // KB Transferred
		"%v,", // Latency in usecs
		i.Ios-prev.Ios,
		(i.Blocks-prev.Blocks)*4*KB,
		i.Latency.DeltaMeanTimeUsecs(&prev.Latency))
}

// --------------------------------------------

type AsuStats struct {
	total, read, write IoMeter
	Latency            tm.TimeDuration
}

func NewAsuStats() *AsuStats {
	return &AsuStats{}
}

func (a *AsuStats) Collect(iostat *IoStats) {
	a.total.Collect(iostat)
	if iostat.Io.Isread {
		a.read.Collect(iostat)
	} else {
		a.write.Collect(iostat)
	}
}

func (a *AsuStats) CsvDelta(prev *AsuStats) string {
	return a.read.CsvDelta(&prev.read) +
		a.write.CsvDelta(&prev.write) +
		a.total.CsvDelta(&prev.total)
}

// --------------------------------------------

type SpcStats struct {
	total, read, write IoMeter

	Asustats []*AsuStats
	Latency  tm.TimeDuration
}

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

func (s *SpcStats) CsvDelta(prev *SpcStats) string {
	return s.read.CsvDelta(&prev.read) +
		s.write.CsvDelta(&prev.write) +
		s.total.CsvDelta(&prev.total) +
		s.Asustats[0].CsvDelta(prev.Asustats[0]) +
		s.Asustats[1].CsvDelta(prev.Asustats[1]) +
		s.Asustats[2].CsvDelta(prev.Asustats[2])
}

func (s *SpcStats) LatencyDelta(prev *SpcStats) float64 {
	return s.total.Latency.DeltaMeanTimeUsecs(&prev.total.Latency)
}

func (s *SpcStats) IosDelta(prev *SpcStats) uint64 {
	return s.total.Ios - prev.total.Ios
}
