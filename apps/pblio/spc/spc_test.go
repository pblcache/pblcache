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
	"github.com/lpabon/goioworkload/spc1"
	"github.com/pblcache/pblcache/cache"
	"github.com/pblcache/pblcache/tests"
	"os"
	"sync"
	"testing"
	"time"
)

func TestNewSpcInfo(t *testing.T) {

	var cache *cache.CacheMap

	usedirectio := false
	blocksize := 4 * KB

	s := NewSpcInfo(cache, usedirectio, blocksize)
	tests.Assert(t, s.pblcache == cache)
	tests.Assert(t, s.blocksize == blocksize)
	tests.Assert(t, len(s.asus) == 3)
	tests.Assert(t, nil != s.asus[ASU1])
	tests.Assert(t, nil != s.asus[ASU2])
	tests.Assert(t, nil != s.asus[ASU3])
	tests.Assert(t, usedirectio == s.asus[ASU1].usedirectio)
	tests.Assert(t, usedirectio == s.asus[ASU2].usedirectio)
	tests.Assert(t, usedirectio == s.asus[ASU3].usedirectio)
}

func TestSpcOpen(t *testing.T) {

	// initialize
	var cache *cache.CacheMap
	usedirectio := false
	blocksize := 4 * KB
	s := NewSpcInfo(cache, usedirectio, blocksize)

	// Get a test file
	tmpfile := tests.Tempfile()

	// No file exists
	err := s.Open(1, tmpfile)
	tests.Assert(t, err != nil)

	// Create the file and open it
	err = tests.CreateFile(tmpfile, 16*4*KB)
	tests.Assert(t, err == nil)
	defer os.Remove(tmpfile)

	// Now open, and it should work
	err = s.Open(1, tmpfile)
	tests.Assert(t, err == nil)
}

func TestSpcAdjustAsuSizes(t *testing.T) {

	// initialize
	var cache *cache.CacheMap
	usedirectio := false
	blocksize := 4 * KB
	s := NewSpcInfo(cache, usedirectio, blocksize)

	// Setup some fake data
	s.asus[ASU1].len = 100
	s.asus[ASU2].len = 200
	s.asus[ASU3].len = 50
	err := s.adjustAsuSizes()

	// asu1 must be equal to asu2
	tests.Assert(t, err == nil)
	tests.Assert(t, s.asus[ASU1].len == 100)
	tests.Assert(t, s.asus[ASU2].len == 100)
	tests.Assert(t, s.asus[ASU3].len == 22)

	// Setup some fake data
	s.asus[ASU1].len = 200
	s.asus[ASU2].len = 100
	s.asus[ASU3].len = 50
	err = s.adjustAsuSizes()

	// asu1 must be equal to asu2
	tests.Assert(t, err == nil)
	tests.Assert(t, s.asus[ASU1].len == 100)
	tests.Assert(t, s.asus[ASU2].len == 100)
	tests.Assert(t, s.asus[ASU3].len == 22)

	// Setup some fake data
	s.asus[ASU1].len = 100
	s.asus[ASU2].len = 100
	s.asus[ASU3].len = 5
	err = s.adjustAsuSizes()

	// asu3 will error since it is not large enough
	tests.Assert(t, err != nil)
	tests.Assert(t, s.asus[ASU1].len == 100)
	tests.Assert(t, s.asus[ASU2].len == 100)
	tests.Assert(t, s.asus[ASU3].len == 5)

}

// Returns size in GB
func TestSpcSize(t *testing.T) {

	// initialize
	var cache *cache.CacheMap
	usedirectio := false
	blocksize := 4 * KB
	s := NewSpcInfo(cache, usedirectio, blocksize)

	// Set fake len
	s.asus[ASU1].len = 40 * GB / (4 * KB)

	// Check size
	size := s.Size(1)
	tests.Assert(t, size == 40)

}

func TestSpc1Init(t *testing.T) {

	// initialize
	var cache *cache.CacheMap
	usedirectio := false
	blocksize := 4 * KB
	s := NewSpcInfo(cache, usedirectio, blocksize)

	// Set fake len
	s.asus[ASU1].len = 5500
	s.asus[ASU2].len = 4500

	// Set asu3 to a value that is too small
	s.asus[ASU3].len = 1

	bsu := 50
	contexts := 1
	err := s.Spc1Init(bsu, contexts)

	// It should not accept a value of asu3
	// because it is too small
	tests.Assert(t, err != nil)

	// Set fake len
	s.asus[ASU1].len = 5500
	s.asus[ASU2].len = 4500
	s.asus[ASU3].len = 1000
	err = s.Spc1Init(bsu, contexts)

	// Now it should succeed
	tests.Assert(t, err == nil)

	// Check that the sizes where adjusted
	tests.Assert(t, s.asus[ASU1].len == 4500)
	tests.Assert(t, s.asus[ASU2].len == 4500)
	tests.Assert(t, s.asus[ASU3].len == 1000)

	// Check spc1 was initialized
	io := spc1.NewSpc1Io(1)
	err = io.Generate()
	tests.Assert(t, err == nil)
}

func TestSpcContext(t *testing.T) {

	// initialize
	var cache *cache.CacheMap
	usedirectio := false
	blocksize := 4 * KB
	s := NewSpcInfo(cache, usedirectio, blocksize)

	// Setup Mockfile
	mockfile := tests.NewMockFile()
	seeklen := int64(4 * 1024 * 1024 * 1024)
	mockfile.MockSeek = func(offset int64, whence int) (int64, error) {
		return seeklen, nil
	}

	// Mock openfile
	defer tests.Patch(&openFile,
		func(name string, flag int, perm os.FileMode) (Filer, error) {
			return mockfile, nil
		}).Restore()

	// Open files
	err := s.Open(1, "asu1file")
	tests.Assert(t, err == nil)
	err = s.Open(2, "asu2file")
	tests.Assert(t, err == nil)
	err = s.Open(3, "asu3file")
	tests.Assert(t, err == nil)

	// Initialize
	bsu := 50
	contexts := 1
	err = s.Spc1Init(bsu, contexts)
	tests.Assert(t, err == nil)

	// Setup channel for Context() subroutines
	// to send stats back
	iotime := make(chan *IoStats)
	runlen := 3
	teststart := time.Now()

	// Create context goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	quit := make(chan struct{})
	go s.Context(&wg, iotime, quit, runlen, contexts)

	// Create a go routine to get stats
	// from channel
	var iostatwg sync.WaitGroup
	iostatwg.Add(1)
	go func() {
		defer iostatwg.Done()

		for iostat := range iotime {
			if iostat == nil {
				t.Error("iostat is nil")
			}
			if iostat.Io == nil {
				t.Errorf("iostat return nil Io")
			}
			if iostat.Start.Before(teststart) {
				t.Errorf("iostat returned a time in the past")
			}
		}
	}()

	// Wait here for Context() to finish
	wg.Wait()
	end := time.Now()

	// Shutdown iotime channel reader
	close(iotime)
	iostatwg.Wait()

	// These are quite big, but just in case a test framework
	// is very busy
	tests.Assert(t, end.Sub(teststart).Seconds() < 10)
	tests.Assert(t, end.Sub(teststart).Seconds() > 1)

	// Cleanup
	s.Close()

}

func TestSpcContextQuit(t *testing.T) {

	// initialize
	var cache *cache.CacheMap
	usedirectio := false
	blocksize := 4 * KB
	s := NewSpcInfo(cache, usedirectio, blocksize)

	// Setup Mockfile
	mockfile := tests.NewMockFile()
	seeklen := int64(4 * 1024 * 1024 * 1024)
	mockfile.MockSeek = func(offset int64, whence int) (int64, error) {
		return seeklen, nil
	}

	// Mock openfile
	defer tests.Patch(&openFile,
		func(name string, flag int, perm os.FileMode) (Filer, error) {
			return mockfile, nil
		}).Restore()

	// Open files
	err := s.Open(1, "asu1file")
	tests.Assert(t, err == nil)
	err = s.Open(2, "asu2file")
	tests.Assert(t, err == nil)
	err = s.Open(3, "asu3file")
	tests.Assert(t, err == nil)

	// Initialize
	bsu := 50
	contexts := 1
	err = s.Spc1Init(bsu, contexts)
	tests.Assert(t, err == nil)

	// Setup channel for Context() subroutines
	// to send stats back
	iotime := make(chan *IoStats)

	// 60 secs, but we will send a quit signal
	runlen := 60

	teststart := time.Now()

	// Create context goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	quit := make(chan struct{})
	go s.Context(&wg, iotime, quit, runlen, contexts)

	// Create a go routine to get stats
	// from channel
	var iostatwg sync.WaitGroup
	iostatwg.Add(1)
	go func() {
		defer iostatwg.Done()

		for iostat := range iotime {
			if iostat == nil {
				t.Error("iostat is nil")
			}
			if iostat.Io == nil {
				t.Errorf("iostat return nil Io")
			}
			if iostat.Start.Before(teststart) {
				t.Errorf("iostat returned a time in the past")
			}
		}
	}()

	// Wait a bit
	time.Sleep(time.Second)

	// Send the quit signal
	close(quit)

	// Wait here for Context() to finish
	wg.Wait()
	end := time.Now()

	// Shutdown iotime channel reader
	close(iotime)
	iostatwg.Wait()

	// These are quite big, but just in case a test framework
	// is very busy
	tests.Assert(t, end.Sub(teststart).Seconds() < 5)
	tests.Assert(t, end.Sub(teststart).Seconds() > 1)

	// Cleanup
	s.Close()

}
