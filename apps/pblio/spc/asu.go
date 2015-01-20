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
	"github.com/lpabon/godbc"
	"os"
	"syscall"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
)

type Asu struct {
	fps         []*os.File
	len         uint32
	usedirectio bool
	fpsize      int64
}

func NewAsu(usedirectio bool) *Asu {
	return &Asu{
		usedirectio: usedirectio,
		fps:         make([]*os.File, 0),
	}
}

// Size in GB
func (a *Asu) Size() float64 {
	return float64(a.len) * 4 * KB / GB
}

func (a *Asu) Open(filename string) error {

	godbc.Require(filename != "")

	// Set the appropriate flags
	flags := os.O_RDWR | os.O_EXCL
	if a.usedirectio {
		flags |= syscall.O_DIRECT
	}

	// Open the file
	fp, err := os.OpenFile(filename, flags, os.ModePerm)
	if err != nil {
		return err
	}

	// Get storage size
	var size int64
	size, err = fp.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	if size == 0 {
		return fmt.Errorf("Size of %s cannot be zero", filename)
	}

	// Check max size for all fps in this asu
	if a.fpsize == 0 || a.fpsize > size {
		a.fpsize = size
	}

	// Append to ASU
	a.fps = append(a.fps, fp)
	a.len = uint32(a.fpsize/(4*KB)) * uint32(len(a.fps))

	godbc.Ensure(a.len > 0, a.len)
	godbc.Ensure(len(a.fps) > 0)
	godbc.Ensure(a.fpsize > 0)

	return nil
}

func (a *Asu) ReadAt(b []byte, offset int64) (n int, err error) {

	fp := int(offset / a.fpsize)
	fp_off := int64(offset % a.fpsize)
	godbc.Check(fp < len(a.fps), fp, len(a.fps))

	return a.fps[fp].ReadAt(b, fp_off)
}

func (a *Asu) WriteAt(b []byte, offset int64) (n int, err error) {
	fp := int(offset / a.fpsize)
	fp_off := int64(offset % a.fpsize)
	godbc.Check(fp < len(a.fps), fp, len(a.fps))

	return a.fps[fp].WriteAt(b, fp_off)
}

func (a *Asu) Close() {
	for _, fp := range a.fps {
		fp.Close()
	}
}
