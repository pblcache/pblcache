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
	"github.com/lpabon/tm"
	"time"
)

type IoMeter struct {
	latency tm.TimeDuration
	ios     uint64

	// 4KB Blocks transferred
	blocks uint64
}

func (i *IoMeter) Collect(iostat *IoStats) {
	i.ios++
	i.blocks += uint64(iostat.Io.Blocks)
	i.latency.Add(iostat.Latency)
}

func (i *IoMeter) CsvDelta(prev *IoMeter, delta time.Duration) string {
	return fmt.Sprintf("%v,"+ // ios
		"%v,"+ // Bytes Transferred
		"%v,"+ // MB/s
		"%v,", // latency in usecs
		i.ios-prev.ios,
		(i.blocks-prev.blocks)*4*KB,
		(float64((i.blocks-prev.blocks)*4*KB)/float64(MB))/delta.Seconds(),
		i.latency.DeltaMeanTimeUsecs(&prev.latency))
}

func (i *IoMeter) MeanLatencyDeltaUsecs(prev *IoMeter) float64 {
	return i.latency.DeltaMeanTimeUsecs(&prev.latency)
}

func (i *IoMeter) MeanLatencyUsecs() float64 {
	return i.latency.MeanTimeUsecs()
}

func (i *IoMeter) IosDelta(prev *IoMeter) uint64 {
	return i.ios - prev.ios
}
