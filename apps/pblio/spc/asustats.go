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
	"time"
)

type AsuStats struct {
	Total IoMeter `json:"total"`
	Read  IoMeter `json:"read"`
	Write IoMeter `json:"write"`
}

func NewAsuStats() *AsuStats {
	return &AsuStats{}
}

func (a *AsuStats) Collect(iostat *IoStats) {
	a.Total.Collect(iostat)
	if iostat.Io.Isread {
		a.Read.Collect(iostat)
	} else {
		a.Write.Collect(iostat)
	}
}

func (a *AsuStats) Csv(delta time.Duration) string {
	return a.Read.Csv(delta) +
		a.Write.Csv(delta) +
		a.Total.Csv(delta)
}

func (a *AsuStats) CsvDelta(prev *AsuStats, delta time.Duration) string {
	return a.Read.CsvDelta(&prev.Read, delta) +
		a.Write.CsvDelta(&prev.Write, delta) +
		a.Total.CsvDelta(&prev.Total, delta)
}
