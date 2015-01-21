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

type MockFile struct {
	called      bool
	calls       int
	mockclose   func() error
	mockseek    func(offset int64, whence int) (int64, error)
	mockreadat  func(p []byte, off int64) (n int, err error)
	mockwriteat func(p []byte, off int64) (n int, err error)
}

func NewMockFile() *MockFile {
	m := &MockFile{}
	m.mockclose = func() error { return nil }

	m.mockseek = func(offset int64, whence int) (int64, error) {
		return 0, nil
	}

	m.mockreadat = func(p []byte, off int64) (n int, err error) {
		return len(p), nil
	}

	m.mockwriteat = func(p []byte, off int64) (n int, err error) {
		return len(p), nil
	}

	return m
}

func (m *MockFile) Close() error {
	return m.mockclose()
}

func (m *MockFile) Seek(offset int64, whence int) (int64, error) {
	return m.mockseek(offset, whence)

}

func (m *MockFile) WriteAt(p []byte, off int64) (n int, err error) {
	return m.mockwriteat(p, off)

}

func (m *MockFile) ReadAt(p []byte, off int64) (n int, err error) {
	return m.mockreadat(p, off)
}

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
	mockfile := NewMockFile()
	mockerror := errors.New("Test Error")

	directio_set := false

	openFile = func(name string, flag int, perm os.FileMode) (Filer, error) {
		directio_set = false
		if (flag & syscall.O_DIRECT) == syscall.O_DIRECT {
			directio_set = true
		}

		return mockfile, mockerror
	}

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
	mockfile := NewMockFile()

	seeklen := int64(0)
	mockerror := errors.New("Test Error")
	seekerror := error(nil)
	mockfile.mockseek = func(offset int64, whence int) (int64, error) {
		return seeklen, seekerror
	}

	openFile = func(name string, flag int, perm os.FileMode) (Filer, error) {
		return mockfile, nil
	}

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
