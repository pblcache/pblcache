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

func (a *AsuStats) CsvDelta(prev *AsuStats, delta time.Duration) string {
	return a.read.CsvDelta(&prev.read, delta) +
		a.write.CsvDelta(&prev.write, delta) +
		a.total.CsvDelta(&prev.total, delta)
}
