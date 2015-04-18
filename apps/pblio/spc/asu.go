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
	"github.com/pblcache/pblcache/cache"
	"io"
	"os"
	"sync"
)

// Allows these functions to be mocked by tests
var (
	openFile = func(name string, flag int, perm os.FileMode) (Filer, error) {
		return os.OpenFile(name, flag, perm)
	}
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
)

type Filer interface {
	io.Closer
	io.Seeker
	io.ReaderAt
	io.WriterAt
}

type Asu struct {
	fps         []Filer
	len         uint32
	usedirectio bool
	fpsize      int64
}

func NewAsu(usedirectio bool) *Asu {
	return &Asu{
		usedirectio: usedirectio,
		fps:         make([]Filer, 0),
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
		flags |= cache.OSSYNC
	}

	// Open the file
	//fp, err := os.OpenFile(filename, flags, os.ModePerm)
	fp, err := openFile(filename, flags, os.ModePerm)
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

func (a *Asu) ioAt(b []byte, offset int64, isread bool) (n int, err error) {
	godbc.Require(a.fpsize != 0)

	// Head
	head_fp := int(offset / a.fpsize)
	head_fp_off := int64(offset % a.fpsize)
	godbc.Check(head_fp < len(a.fps), head_fp, len(a.fps))

	// Tail
	tail_fp := int((offset + int64(len(b))) / a.fpsize)
	godbc.Check(tail_fp < len(a.fps), tail_fp, len(a.fps))

	if head_fp == tail_fp {
		if isread {
			return a.fps[head_fp].ReadAt(b, head_fp_off)
		} else {
			return a.fps[head_fp].WriteAt(b, head_fp_off)
		}
	} else {
		var (
			wg                 sync.WaitGroup
			head_n, tail_n     int
			head_err, tail_err error
		)
		wg.Add(2)

		// Read head
		go func() {
			defer wg.Done()
			if isread {
				head_n, head_err = a.fps[head_fp].ReadAt(b[:a.fpsize-head_fp_off], head_fp_off)
			} else {
				head_n, head_err = a.fps[head_fp].WriteAt(b[:a.fpsize-head_fp_off], head_fp_off)
			}
		}()

		// Read tail
		go func() {
			defer wg.Done()
			if isread {
				tail_n, tail_err = a.fps[tail_fp].ReadAt(b[a.fpsize-head_fp_off:], 0)
			} else {
				tail_n, tail_err = a.fps[tail_fp].WriteAt(b[a.fpsize-head_fp_off:], 0)
			}
		}()

		wg.Wait()

		if head_err != nil {
			return head_n, head_err
		} else if tail_err != nil {
			return tail_n, tail_err
		} else {
			return head_n + tail_n, nil
		}
	}
}

func (a *Asu) ReadAt(b []byte, offset int64) (n int, err error) {
	return a.ioAt(b, offset, true)
}

func (a *Asu) WriteAt(b []byte, offset int64) (n int, err error) {
	return a.ioAt(b, offset, false)
}

func (a *Asu) Close() {
	for _, fp := range a.fps {
		fp.Close()
	}
}
