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

func (i *IoMeter) CsvDelta(prev *IoMeter, delta time.Duration) string {
	return fmt.Sprintf("%v,"+ // Ios
		"%v,"+ // Bytes Transferred
		"%v,"+ // MB/s
		"%v,", // Latency in usecs
		i.Ios-prev.Ios,
		(i.Blocks-prev.Blocks)*4*KB,
		(float64((i.Blocks-prev.Blocks)*4*KB)/float64(MB))/delta.Seconds(),
		i.Latency.DeltaMeanTimeUsecs(&prev.Latency))
}
