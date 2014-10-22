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
package tests

import (
	"fmt"
	"os"
)

func tempfile_generate() func() string {
	counter := 0
	return func() string {
		counter++
		return fmt.Sprintf("/tmp/pblcache_test.%d-%d",
			os.Getpid(), counter)
	}
}

// Return a filename string in the form of
// /tmp/pblcache_test.<Process Id>-<Counter>
func Tempfile() string {
	genname := tempfile_generate()
	return genname()
}
