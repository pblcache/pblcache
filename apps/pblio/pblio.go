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
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/pblcache/pblcache/apps/pblio/spc"
	"github.com/pblcache/pblcache/cache"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"time"
)

// JSON Stats
type PblioStats struct {
	Timestamp int64             `json:"time"`
	Spc       *spc.SpcStats     `json:"spc"`
	Cache     *cache.CacheStats `json:"cache,omitempty"`
	Log       *cache.LogStats   `json:"log,omitempty"`
}

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
)

var (
	asu1, asu2, asu3         string
	cachefilename, pbliodata string
	runlen, cachesize        int
	blocksize, contexts      int
	bsu, dataperiod          int
	usedirectio, cpuprofile  bool
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
	flag.BoolVar(&usedirectio, "directio", true, "\n\tUse O_DIRECT on ASU files")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "\n\tCreate a Go cpu profile for analysis")
	flag.StringVar(&pbliodata, "data", "pblio.data", "\n\tStats file in CSV format")
	flag.IntVar(&dataperiod, "dataperiod", 5, "\n\tNumber of seconds per data collected and saved in the csv file")
}

func main() {
	// Gather command line arguments
	flag.Parse()

	// According to spc.h, this needs to be set to
	// contexts = (b+99)/100 for SPC workload generator
	// conformance
	contexts = int((bsu + 99) / 100)

	if asu1 == "" ||
		asu2 == "" ||
		asu3 == "" {
		fmt.Print("ASU files must be set\n")
		return
	}

	// Open stats file
	fp, err := os.Create(pbliodata)
	if err != nil {
		fmt.Print(err)
		return
	}
	metrics := bufio.NewWriter(fp)
	defer fp.Close()

	// Setup number of blocks
	blocksize_bytes := uint64(blocksize * KB)

	// Open cache
	var c *cache.Cache
	var log *cache.Log
	var logblocks uint64

	// Show banner
	fmt.Println("-----")
	fmt.Println("pblio")
	fmt.Println("-----")

	// Determine if we need to use the cache
	if cachefilename != "" {
		// Create log
		log, logblocks = cache.NewLog(cachefilename,
			uint64(cachesize*GB)/blocksize_bytes,
			blocksize_bytes,
			(512*KB)/blocksize_bytes,
			0, // buffer cache has been removed for now
		)

		// Connect cache metadata with log
		c = cache.NewCache(logblocks, blocksize_bytes, log.Msgchan)
		fmt.Printf("Cache   : %s\n"+
			"C Size  : %.2f GB\n",
			cachefilename,
			float64(logblocks*blocksize_bytes)/GB)
	} else {
		fmt.Println("Cache   : None")
	}

	// Initialize spc1info
	spcinfo := spc.NewSpcInfo(c, usedirectio, blocksize)

	// Open asus
	for _, v := range strings.Split(asu1, ",") {
		err = spcinfo.Open(1, v)
		if err != nil {
			fmt.Print(err)
			return
		}
	}
	for _, v := range strings.Split(asu2, ",") {
		err = spcinfo.Open(2, v)
		if err != nil {
			fmt.Print(err)
			return
		}
	}
	for _, v := range strings.Split(asu3, ",") {
		err = spcinfo.Open(3, v)
		if err != nil {
			fmt.Print(err)
			return
		}
	}
	defer spcinfo.Close()

	// Start cpu profiling
	if cpuprofile {
		f, _ := os.Create("cpuprofile")
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Initialize Spc1 workload
	err = spcinfo.Spc1Init(bsu, contexts)
	if err != nil {
		fmt.Print(err)
		return
	}

	// This channel will be used for the io to return
	// the latency
	iotime := make(chan *spc.IoStats, 1024)

	// Before starting, let's print out the sizes
	// and test information
	fmt.Printf("ASU1    : %.2f GB\n"+
		"ASU2    : %.2f GB\n"+
		"ASU3    : %.2f GB\n"+
		"BSUs    : %v\n"+
		"Contexts: %v\n"+
		"Run time: %v s\n",
		spcinfo.Size(1),
		spcinfo.Size(2),
		spcinfo.Size(3),
		bsu,
		contexts,
		runlen)
	fmt.Println("-----")

	// Spawn contexts coroutines
	var wg sync.WaitGroup
	for context := 0; context < contexts; context++ {
		wg.Add(1)
		go spcinfo.Context(&wg, iotime, runlen, context)
	}

	// Used to collect all the stats
	spcstats := spc.NewSpcStats()
	prev_spcstats := spcstats.Copy()
	pbliostats := &PblioStats{}
	pbliostats.Spc = spcstats

	// This goroutine will be used to collect the data
	// from the io routines and print out to the console
	// every few seconds
	var outputwg sync.WaitGroup
	outputwg.Add(1)
	go func() {
		defer outputwg.Done()

		start := time.Now()
		totaltime := start
		totalios := uint64(0)
		print_iops := time.After(time.Second * time.Duration(dataperiod))

		for iostat := range iotime {

			// Save stats
			spcstats.Collect(iostat)

			// Do this every few seconds
			select {
			case <-print_iops:
				end := time.Now()
				ios := spcstats.IosDelta(prev_spcstats)
				totalios += ios
				iops := float64(ios) / end.Sub(start).Seconds()
				fmt.Printf("ios:%v IOPS:%.2f Latency:%.4f ms"+
					"                                   \r",
					ios, iops, spcstats.MeanLatencyDeltaUsecs(prev_spcstats)/1000)

				// Get stats from the cache
				if c != nil {
					pbliostats.Cache = c.Stats()
					pbliostats.Log = log.Stats()
				}

				// Save stats
				pbliostats.Timestamp = time.Now().Unix()
				jsonstats, err := json.Marshal(pbliostats)
				if err != nil {
					fmt.Println(err)
				} else {
					metrics.WriteString(string(jsonstats) + "\n")
				}

				// Reset counters
				start = time.Now()
				prev_spcstats = spcstats.Copy()

				// Set the timer for the next time
				print_iops = time.After(time.Second * time.Duration(dataperiod))
			default:
			}
		}

		end := time.Now()
		iops := float64(totalios) / end.Sub(totaltime).Seconds()

		// Print final info
		fmt.Printf("\tAvg IOPS:%.2f  Avg Latency:%.4f ms"+
			"                        \n",
			iops, spcstats.MeanLatencyUsecs()/1000)

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
	}
	metrics.Flush()

}
