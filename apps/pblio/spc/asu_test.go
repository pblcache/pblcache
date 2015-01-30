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
	"errors"
	"github.com/pblcache/pblcache/tests"
	"os"
	"syscall"
	"testing"
)

func TestAsuNew(t *testing.T) {
	usedirectio := false
	asu := NewAsu(usedirectio)
	tests.Assert(t, asu.usedirectio == usedirectio)
	tests.Assert(t, len(asu.fps) == 0)
	tests.Assert(t, asu.len == 0)

	usedirectio = true
	asu = NewAsu(usedirectio)
	tests.Assert(t, asu.usedirectio == usedirectio)
	tests.Assert(t, len(asu.fps) == 0)
	tests.Assert(t, asu.len == 0)
}

func TestAsuSize(t *testing.T) {

	usedirectio := false
	asu := NewAsu(usedirectio)

	// set fake length
	asu.len = 1234 * GB / (4 * KB)
	tests.Assert(t, float64(1234) == asu.Size())
}

func TestAsuOpenFile(t *testing.T) {
	usedirectio := true
	asu := NewAsu(usedirectio)
	mockfile := tests.NewMockFile()
	mockerror := errors.New("Test Error")

	directio_set := false

	// Mock openFile
	defer tests.Patch(&openFile,
		func(name string, flag int, perm os.FileMode) (Filer, error) {
			directio_set = false
			if (flag & syscall.O_DIRECT) == syscall.O_DIRECT {
				directio_set = true
			}

			return mockfile, mockerror
		}).Restore()

	// Call
	err := asu.Open("filename")

	// Check results
	tests.Assert(t, directio_set == true)
	tests.Assert(t, err == mockerror)

	// Now try without directio set
	usedirectio = false
	asu = NewAsu(usedirectio)

	// Check results
	err = asu.Open("filename")
	tests.Assert(t, directio_set == false)
	tests.Assert(t, err == mockerror)
}

func TestAsuOpenSeek(t *testing.T) {
	usedirectio := true
	asu := NewAsu(usedirectio)
	mockfile := tests.NewMockFile()

	seeklen := int64(0)
	mockerror := errors.New("Test Error")
	seekerror := error(nil)
	mockfile.MockSeek = func(offset int64, whence int) (int64, error) {
		return seeklen, seekerror
	}

	// Mock openFile
	defer tests.Patch(&openFile,
		func(name string, flag int, perm os.FileMode) (Filer, error) {
			return mockfile, nil
		}).Restore()

	// Seek will return len of 0
	err := asu.Open("filename")
	tests.Assert(t, err != nil)
	tests.Assert(t, len(asu.fps) == 0)

	// Seek will return error
	seekerror = mockerror
	err = asu.Open("filename")
	tests.Assert(t, err != nil)
	tests.Assert(t, err == mockerror)
	tests.Assert(t, len(asu.fps) == 0)

	// Seek will return correct data
	seeklen = int64(4 * KB * 100)
	asublocks := uint32(seeklen / (4 * KB))
	seekerror = nil
	err = asu.Open("filename")
	tests.Assert(t, err == nil)
	tests.Assert(t, len(asu.fps) == 1)
	tests.Assert(t, asu.len == asublocks)

	// Now add a larger file, but it should
	// only add the min() size of the files opened
	seeklen = int64(4 * KB * 110)
	asublocks += asublocks
	err = asu.Open("filename")
	tests.Assert(t, err == nil)
	tests.Assert(t, len(asu.fps) == 2)
	tests.Assert(t, asu.len == asublocks)

	// Now add a smaller file, but it should
	// only add the min() size of the files opened
	seeklen = int64(4 * KB * 50)
	asublocks = 50 * 3
	err = asu.Open("filename")
	tests.Assert(t, err == nil)
	tests.Assert(t, len(asu.fps) == 3)
	tests.Assert(t, asu.len == asublocks)

}

func TestAsuIoAt(t *testing.T) {

	usedirectio := true
	asu := NewAsu(usedirectio)

	// Setup Head
	head := tests.NewMockFile()
	head.MockSeek = func(offset int64, whence int) (int64, error) {
		return 10 * KB, nil
	}

	var (
		head_check_p_len  int
		head_check_offset int64
		head_called       bool
	)
	head.MockReadAt = func(p []byte, off int64) (n int, err error) {
		tests.Assert(t, len(p) == head_check_p_len)
		tests.Assert(t, head_check_offset == off)
		head_called = true

		return len(p), nil
	}
	head.MockWriteAt = func(p []byte, off int64) (n int, err error) {
		tests.Assert(t, len(p) == head_check_p_len)
		tests.Assert(t, head_check_offset == off)
		head_called = true

		return len(p), nil
	}

	// Setup Tail
	tail := tests.NewMockFile()
	tail.MockSeek = func(offset int64, whence int) (int64, error) {
		return 8 * KB, nil
	}

	var (
		tail_check_p_len  int
		tail_check_offset int64
		tail_called       bool
	)
	tail.MockReadAt = func(p []byte, off int64) (n int, err error) {
		tests.Assert(t, len(p) == tail_check_p_len)
		tests.Assert(t, tail_check_offset == off)
		tail_called = true

		return len(p), nil
	}
	tail.MockWriteAt = func(p []byte, off int64) (n int, err error) {
		tests.Assert(t, len(p) == tail_check_p_len)
		tests.Assert(t, tail_check_offset == off)
		tail_called = true

		return len(p), nil
	}

	// Mock openFile
	defer tests.Patch(&openFile,
		func(name string, flag int, perm os.FileMode) (Filer, error) {
			if name == "head" {
				return head, nil
			} else {
				return tail, nil
			}
		}).Restore()

	// Open files
	err := asu.Open("head")
	tests.Assert(t, err == nil)
	err = asu.Open("tail")
	tests.Assert(t, err == nil)

	// Write small, it should not over flow into file2
	small := make([]byte, 512)
	head_check_offset = int64(4*KB - len(small))
	head_check_p_len = len(small)
	tail_called = false
	head_called = false
	n, err := asu.WriteAt(small, int64(4*KB-len(small)))
	tests.Assert(t, n == len(small))
	tests.Assert(t, err == nil)
	tests.Assert(t, head_called == true)
	tests.Assert(t, tail_called == false)

	// Write large, should go across files
	large := make([]byte, 10*KB)
	head_check_offset = 4 * KB
	head_check_p_len = 4 * KB
	tail_check_offset = 0
	tail_check_p_len = 6 * KB
	tail_called = false
	head_called = false
	n, err = asu.WriteAt(large, 4*KB)
	tests.Assert(t, n == len(large))
	tests.Assert(t, err == nil)
	tests.Assert(t, head_called == true)
	tests.Assert(t, tail_called == true)

	// Repeat with ReadAt
	// Read small, it should not over flow into file2
	head_check_offset = int64(4*KB - len(small))
	head_check_p_len = len(small)
	tail_called = false
	head_called = false
	n, err = asu.ReadAt(small, int64(4*KB-len(small)))
	tests.Assert(t, n == len(small))
	tests.Assert(t, err == nil)
	tests.Assert(t, head_called == true)
	tests.Assert(t, tail_called == false)

	// Write large, should go across files
	head_check_offset = 4 * KB
	head_check_p_len = 4 * KB
	tail_check_offset = 0
	tail_check_p_len = 6 * KB
	tail_called = false
	head_called = false
	n, err = asu.ReadAt(large, 4*KB)
	tests.Assert(t, n == len(large))
	tests.Assert(t, err == nil)
	tests.Assert(t, head_called == true)
	tests.Assert(t, tail_called == true)
}
