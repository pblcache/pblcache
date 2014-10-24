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
	"github.com/pblcache/pblcache/cache"
	"github.com/pblcache/pblcache/message"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	GENERATORS = 128
	KB         = 1024
	MB         = 1024 * KB
	GB         = 1024 * MB
)

var (
	filename, cachefilename       string
	runtime, cachesize, blocksize int
	usedirectio                   bool
)

func init() {
	flag.StringVar(&filename, "filename", "", "\n\tStorage back end file to read and write")
	flag.StringVar(&cachefilename, "cache", "", "\n\tCache file name")
	flag.IntVar(&cachesize, "cachesize", 8, "\n\tCache size in GB")
	flag.IntVar(&runtime, "runtime", 300, "\n\tRuntime in seconds")
	flag.IntVar(&blocksize, "blocksize", 4, "\n\tCache block size in KB")
	flag.BoolVar(&usedirectio, "directio", true, "\n\tUse O_DIRECT")
}

func cache_sput(c *cache.Cache,
	offset uint64,
	buffer []byte) error {

	if c != nil {
		here := make(chan *message.Message)
		msg := message.NewMsgPut()
		msg.RetChan = here
		iopkt := msg.IoPkt()
		iopkt.Offset = offset
		iopkt.Buffer = buffer
		c.Msgchan <- msg
		<-here

		//fmt.Print("p")
		return msg.Err
	} else {
		return nil
	}
}

func cache_sinval(c *cache.Cache,
	offset uint64) error {

	if c != nil {
		here := make(chan *message.Message)
		msg := message.NewMsgInvalidate()
		msg.RetChan = here
		iopkt := msg.IoPkt()
		iopkt.Offset = offset
		c.Msgchan <- msg
		<-here

		//fmt.Print("i")
		return msg.Err
	} else {
		return nil
	}
}

func cache_sget(c *cache.Cache, offset uint64, buffer []byte) error {

	if c != nil {
		here := make(chan *message.Message)
		msg := message.NewMsgGet()
		msg.RetChan = here
		iopkt := msg.IoPkt()
		iopkt.Offset = offset
		iopkt.Buffer = buffer
		c.Msgchan <- msg
		<-here

		//fmt.Print("g")
		return msg.Err
	} else {
		return cache.ErrNotFound
	}
}

func main() {
	flag.Parse()

	if filename == "" {
		fmt.Print("filename must be set\n")
		return
	}

	// Open file
	var fp *os.File
	var err error
	if usedirectio {
		fp, err = os.OpenFile(filename, syscall.O_DIRECT|os.O_RDWR|os.O_EXCL, os.ModePerm)
	} else {
		fp, err = os.OpenFile(filename, os.O_RDWR|os.O_EXCL, os.ModePerm)
	}
	if err != nil {
		fmt.Println(err)
		return
	}

	// Get file size
	filestat, err := fp.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}
	filesize := uint64(filestat.Size())

	// Setup number of blocks
	blocksize_bytes := uint64(blocksize * KB)
	fileblocks := uint64(filesize / blocksize_bytes)
	mbs := make(chan int, 32)

	// Open cache
	var c *cache.Cache
	var log *cache.Log
	var logblocks uint64
	if cachefilename != "" {
		fmt.Printf("Using %s as the cache\n", cachefilename)
		log, logblocks = cache.NewLog(cachefilename,
			uint64(cachesize*GB)/blocksize_bytes,
			blocksize_bytes,
			(512*KB)/blocksize_bytes,
			uint64(cachesize*GB)/1000)
		//n := message.NewNullTerminator()
		//n.Start()
		c = cache.NewCache(logblocks, log.Msgchan)
	} else {
		fmt.Println("No cache set")
	}

	// Start timer
	start := time.Now()

	// Start IO generators
	var wg sync.WaitGroup
	for gen := 0; gen < GENERATORS; gen++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			z := zipf.NewZipfWorkload(fileblocks, 65 /* read % */)
			stop := time.After(time.Second * time.Duration(runtime))
			buffer := make([]byte, blocksize_bytes)

			for {
				select {
				case <-stop:
					return
				default:
					block, isread := z.ZipfGenerate()
					offset := block * blocksize_bytes
					if isread {
						// Check cache
						err := cache_sget(c, offset, buffer)
						if err != nil {
							// Not in cache, get from storage
							fp.ReadAt(buffer, int64(offset))

							// Now we store in cache
							cache_sput(c, offset, buffer)
						}
					} else {
						cache_sinval(c, offset)
						fp.WriteAt(buffer, int64(offset))
						cache_sput(c, offset, buffer)
					}
					mbs <- 1
				}
			}
		}()
	}

	var mbswg sync.WaitGroup
	mbswg.Add(1)
	go func() {
		defer mbswg.Done()
		var num_blocks int

		for ioblocks := range mbs {
			num_blocks += ioblocks
		}
		total_duration := time.Now().Sub(start)

		fmt.Printf("Bandwidth: %.1f MB/s\n",
			((float64(blocksize_bytes)*
				float64(num_blocks))/(1024.0*1024.0))/
				float64(total_duration.Seconds()))
		fmt.Printf("Seconds: %.2f\n", total_duration.Seconds())
		fmt.Printf("IO blocks: %d\n", num_blocks)
	}()

	wg.Wait()
	close(mbs)
	mbswg.Wait()

	if c != nil {
		c.Close()
		log.Close()
		fmt.Print(c)
		fmt.Print(log)
	}
}
