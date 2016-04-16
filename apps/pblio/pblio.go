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
	"os/signal"
	"runtime"
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
	PBLIO_VERSION            = "(dev)"
	asu1, asu2, asu3         string
	cachefilename, pbliodata string
	runlen                   int
	blocksize, contexts      int
	bsu, dataperiod          int
	usedirectio, cpuprofile  bool
	cachesavefile            string
	showVersion              bool
	mock_reads, mock_writes  bool
)

func init() {
	flag.StringVar(&asu1, "asu1", "", "ASU1 - Data Store")
	flag.StringVar(&asu2, "asu2", "", "ASU2 - User Store")
	flag.StringVar(&asu3, "asu3", "", "Log")
	flag.StringVar(&cachefilename, "cache", "", "Cache file name")
	flag.StringVar(&cachesavefile, "cachemeta", "cache.pbl", "Persistent cache metadata location")
	flag.IntVar(&bsu, "bsu", 50, "Number of BSUs (Business Scaling Units)."+
		"\n\tEach BSU requires 50 IOPs from the back end storage")
	flag.IntVar(&runlen, "runlen", 300, "Benchmark run time length in seconds")
	flag.IntVar(&blocksize, "blocksize", 4, "Cache block size in KB")
	flag.BoolVar(&usedirectio, "directio", true, "Use O_DIRECT on ASU files")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "Create a Go cpu profile for analysis")
	flag.StringVar(&pbliodata, "data", "pblio.data", "Stats file in JSON format")
	flag.IntVar(&dataperiod, "dataperiod", 60, "Number of seconds per data collected and saved in the json file")
	flag.BoolVar(&showVersion, "version", false, "Version")
	flag.BoolVar(&mock_reads, "mock_reads", false, "Mock reads to storage backend")
	flag.BoolVar(&mock_writes, "mock_writes", false, "Mock writes to storage backend")
}

func printVersion() {
	fmt.Printf("pblio %v\n", PBLIO_VERSION)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	// Gather command line arguments
	flag.Parse()

	printVersion()
	if showVersion {
		return
	}

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
	blocksize_bytes := uint32(blocksize * KB)

	// Open cache
	var c *cache.CacheMap
	var log *cache.Log
	var logblocks uint32

	// Show banner
	fmt.Println("-----")
	fmt.Println("pblio")
	fmt.Println("-----")

	// Determine if we need to use the cache
	if cachefilename != "" {
		// Create log
		log, logblocks, err = cache.NewLog(cachefilename,
			blocksize_bytes,
			(512*KB)/blocksize_bytes,
			0,    // buffer cache has been removed for now
			true, // Use DirectIO to SSD
		)
		if err != nil {
			fmt.Println(err)
			return
		}

		// Connect cache metadata with log
		c = cache.NewCacheMap(logblocks, blocksize_bytes, log.Msgchan)
		cache_state := "New"
		if _, err = os.Stat(cachesavefile); err == nil {
			err = c.Load(cachesavefile, log)
			if err != nil {
				fmt.Printf("Unable to load metadata: %s", err)
				return
			}
			cache_state = "Loaded"
		}

		// Start log goroutines
		log.Start()

		// Print banner
		fmt.Printf("Cache   : %s (%s)\n"+
			"C Size  : %.2f GB\n",
			cachefilename, cache_state,
			float64(logblocks)/GB*float64(blocksize_bytes))
	} else {
		fmt.Println("Cache   : None")
	}

	// Initialize spc1info
	spcinfo := spc.NewSpcInfo(c, usedirectio, mock_reads, mock_writes, blocksize)

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
	iotime := make(chan *spc.IoStats, 2048)

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

	// Shutdown on signal
	quit := make(chan struct{})
	signalch := make(chan os.Signal, 1)
	signal.Notify(signalch, os.Interrupt)
	go func() {
		select {
		case <-signalch:
			close(quit)
			return
		}
	}()

	// Spawn contexts coroutines
	var wg sync.WaitGroup
	for context := 0; context < contexts; context++ {
		wg.Add(1)
		go spcinfo.Context(&wg, iotime, quit, runlen, context)
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
			totalios += 1

			// Do this every few seconds
			select {
			case <-print_iops:
				end := time.Now()
				ios := spcstats.IosDelta(prev_spcstats)
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
		fmt.Printf("Avg IOPS:%.2f  Avg Latency:%.4f ms"+
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
		err = c.Save(cachesavefile, log)
		if err != nil {
			fmt.Printf("Unable to save metadata: %s\n", err)
			os.Remove(cachesavefile)
		}
		fmt.Print(c)
		fmt.Print(log)
	}
	metrics.Flush()

}
