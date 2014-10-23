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

package main

import (
	"flag"
	"fmt"
	zipf "github.com/lpabon/zipfworkload"
	"os"
	"sync"
	"syscall"
	"time"
	/*
		"github.com/pblcache/pblcache/cache"
		"github.com/pblcache/pblcache/message"
	*/)

const (
	GENERATORS = 32
)

var (
	filename, cachefilename       string
	runtime, cachesize, blocksize int
)

func init() {
	flag.StringVar(&filename, "filename", "", "\n\tStorage back end file to read and write")
	flag.StringVar(&cachefilename, "cache", "", "\n\tCache file name")
	flag.IntVar(&cachesize, "cachesize", 8, "\n\tCache size in GB")
	flag.IntVar(&runtime, "runtime", 300, "\n\tRuntime in seconds")
	flag.IntVar(&blocksize, "blocksize", 4, "\n\tCache block size in KB")
}

func main() {
	flag.Parse()

	if filename == "" {
		fmt.Print("filename must be set\n")
		return
	}
	if cachefilename == "" {
		fmt.Print("cache file name must be set\n")
		return
	}

	fp, err := os.OpenFile(filename, syscall.O_DIRECT|os.O_RDWR|os.O_EXCL, os.ModePerm)
	if err != nil {
		fmt.Println(err)
		return
	}

	filestat, err := fp.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}

	filesize := uint64(filestat.Size())

	blocksize_bytes := uint64(blocksize * 1024)
	fileblocks := uint64(filesize / blocksize_bytes)

	var wg sync.WaitGroup
	for gen := 0; gen < GENERATORS; gen++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			z := zipf.NewZipfWorkload(fileblocks, 65 /* read % */)
			stop := time.After(time.Second * time.Duration(runtime))
			buffer := make([]byte, blocksize*1024)

			for {
				select {
				case <-stop:
					return
				default:
					block, _ := z.ZipfGenerate()
					fp.ReadAt(buffer, int64(block*blocksize_bytes))
					fmt.Printf("%v:", block)
				}
			}
		}()
	}

	wg.Wait()
}
