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
package message

import (
	"runtime"
	"testing"
)

func assert(t *testing.T, b bool) {
	if !b {
		pc, file, line, _ := runtime.Caller(1)
		caller_func_info := runtime.FuncForPC(pc)

		t.Errorf("\n\rASSERT:\tfunc (%s) 0x%x\n\r\tFile %s:%d",
			caller_func_info.Name(),
			pc,
			file,
			line)
	}
}

func TestTime(t *testing.T) {
	m := &Message{}
	for i := 0; i < 100; i++ {
		m.TimeStart()
		for j := 0; j < 10000; j++ {
		}
		m.TimeStop()
	}
	assert(t, m.Stats.Duration.MeanTimeUsecs() > 0)
	assert(t, "" != m.String())
}

type Data struct {
	i   int
	i64 int64
	s   string
	f   float64
}

func TestMessage(t *testing.T) {
	m := &Message{
		Type: MsgGet,
	}

	d := &Data{
		i:   10,
		i64: 100,
		s:   "Test",
		f:   1.001,
	}

	// Save a *data in interface
	m.Priv = d
	newD := m.Priv.(*Data)
	assert(t, newD.i == d.i)
	assert(t, newD.f == d.f)
	assert(t, newD.i64 == d.i64)
	assert(t, newD.s == d.s)
}
