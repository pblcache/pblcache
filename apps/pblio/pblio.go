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
	"github.com/lpabon/tm"
	"github.com/pblcache/pblcache/apps/pblio/spc"
	"github.com/pblcache/pblcache/cache"
	"os"
	"runtime/pprof"
	"sync"
	"time"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
)

var (
	asu1, asu2, asu3        string
	cachefilename           string
	runlen, cachesize       int
	blocksize, contexts     int
	bsu                     int
	usedirectio, cpuprofile bool
)

func init() {
	flag.StringVar(&asu1, "asu1", "", "\n\tASU1 - Data Store")
	flag.StringVar(&asu2, "asu2", "", "\n\tASU2 - User Store")
	flag.StringVar(&asu3, "asu3", "", "\n\tLog")
	flag.StringVar(&cachefilename, "cache", "", "\n\tCache file name")
	flag.IntVar(&bsu, "bsu", 50, "\n\tNumber of BSUs (Business Scaling Units)."+
		"\n\tEach BSU requires 50 IOPs from the back end storage")
	flag.IntVar(&cachesize, "cachesize", 8, "\n\tCache size in GB")
	flag.IntVar(&runlen, "runlen", 300, "\n\tBenchmark run time length in seconds")
	flag.IntVar(&blocksize, "blocksize", 4, "\n\tCache block size in KB")
	flag.IntVar(&contexts, "contexts", 1, "\n\tNumber of contexts.  Each context runs its own SPC1 generator"+
		"\n\tEach context also has 8 streams.  Four(4) streams for ASU1, three(3)"+
		"\n\tfor ASU2, and one for ASU3. Values are set in spc1.c:188")
	flag.BoolVar(&usedirectio, "directio", true, "\n\tUse O_DIRECT on ASU files")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "\n\tCreate a Go cpu profile for analysis")
}

func main() {
	flag.Parse()

	if asu1 == "" ||
		asu2 == "" ||
		asu3 == "" {
		fmt.Print("ASU files must be set\n")
		return
	}

	// Setup number of blocks
	blocksize_bytes := uint64(blocksize * KB)

	// Open cache
	var c *cache.Cache
	var log *cache.Log
	var logblocks uint64

	// Determine if we need to use the cache
	if cachefilename != "" {
		fmt.Printf("Using %s as the cache\n", cachefilename)

		// Create log
		log, logblocks = cache.NewLog(cachefilename,
			uint64(cachesize*GB)/blocksize_bytes,
			blocksize_bytes,
			(512*KB)/blocksize_bytes,
			0, // buffer cache has been removed for now
		)

		// Connect cache metadata with log
		c = cache.NewCache(logblocks, blocksize_bytes, log.Msgchan)
	} else {
		fmt.Println("No cache set")
	}

	// Initialize spc1info
	spcinfo := spc.NewSpcInfo(c, usedirectio, blocksize)

	// Open asus
	var err error
	err = spcinfo.Open(1, asu1)
	if err != nil {
		fmt.Print(err)
		return
	}
	err = spcinfo.Open(2, asu2)
	if err != nil {
		fmt.Print(err)
		return
	}
	err = spcinfo.Open(3, asu3)
	if err != nil {
		fmt.Print(err)
		return
	}

	// Start cpu profiling
	if cpuprofile {
		f, _ := os.Create("cpuprofile")
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Initialize Spc1 workload
	spcinfo.Spc1Init(bsu, contexts)

	// This channel will be used for the io to return
	// the latency
	iotime := make(chan time.Duration, 64)

	// Spawn contexts coroutines
	var wg sync.WaitGroup
	stop := time.After(time.Second * time.Duration(runlen))
	for context := 1; context <= contexts; context++ {
		wg.Add(1)
		go spcinfo.Context(&wg, iotime, stop, context)
	}

	// This goroutine will be used to collect the data
	// from the io routines and print out to the console
	// every few seconds
	var outputwg sync.WaitGroup
	outputwg.Add(1)
	go func() {
		defer outputwg.Done()

		ios := uint64(0)
		start := time.Now()
		latency_mean := tm.TimeDuration{}
		print_iops := time.After(time.Second * 2)

		for latency := range iotime {

			// Save the number of ios being sent
			// and their latency
			ios++
			latency_mean.Add(latency)

			// Do this every few seconds
			select {
			case <-print_iops:
				end := time.Now()
				iops := float64(ios) / end.Sub(start).Seconds()
				fmt.Printf("ios:%v IOPS:%.2f Latency:%.4f ms"+
					"                                   \r",
					ios, iops, latency_mean.MeanTimeUsecs()/1000)

				// Clear the latency
				latency_mean = tm.TimeDuration{}

				// Set the timer for the next time
				print_iops = time.After(time.Second * 2)
			default:
			}
		}

		end := time.Now()
		iops := float64(ios) / end.Sub(start).Seconds()
		fmt.Printf("ios:%v IOPS:%.2f Latency:%.4f ms",
			ios, iops, latency_mean.MeanTimeUsecs()/1000)

		fmt.Print("\n")
	}()

	// Wait here for all the context goroutines to finish
	wg.Wait()

	// Now we can close the output goroutine
	close(iotime)
	outputwg.Wait()

	// Print cache stats
	if c != nil {
		c.Close()
		log.Close()
		fmt.Print(c)
		fmt.Print(log)
	} else {
		fmt.Println("No cache stats")
	}

}
