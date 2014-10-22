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

package cache

import (
	"fmt"
	"github.com/lpabon/tm"
	"sync"
)

type CacheStats struct {
	readhits, writehits      int
	reads, writes            int
	evictions, invalidations int
	insertions               int
	treads                   *tm.TimeDuration
	twrites                  *tm.TimeDuration
	lock                     sync.Mutex
}

func NewCacheStats() *CacheStats {
	c := &CacheStats{}
	c.treads = &tm.TimeDuration{}
	c.twrites = &tm.TimeDuration{}
	return c
}

func (c *CacheStats) readHitRateDelta(prev *CacheStats) float64 {
	reads := c.reads - prev.reads
	readhits := c.readhits - prev.readhits
	if reads == 0 {
		return 0.0
	} else {
		return float64(readhits) / float64(reads)
	}
}

func (c *CacheStats) writeHitRateDelta(prev *CacheStats) float64 {
	writes := c.writes - prev.writes
	writehits := c.writehits - prev.writehits
	if writes == 0 {
		return 0.0
	} else {
		return float64(writehits) / float64(writes)
	}
}

func (c *CacheStats) readHitRate() float64 {
	if c.reads == 0 {
		return 0.0
	} else {
		return float64(c.readhits) / float64(c.reads)
	}
}

func (c *CacheStats) writeHitRate() float64 {
	if c.writes == 0 {
		return 0.0
	} else {
		return float64(c.writehits) / float64(c.writes)
	}

}

func (c *CacheStats) Copy() *CacheStats {
	c.lock.Lock()
	defer c.lock.Unlock()

	statscopy := &CacheStats{}
	*statscopy = *c

	statscopy.treads = c.treads.Copy()
	statscopy.twrites = c.twrites.Copy()

	return statscopy
}

func (c *CacheStats) ReadHit() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.readhits++
}

func (c *CacheStats) WriteHit() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.writehits++
}

func (c *CacheStats) Read() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.reads++
}

func (c *CacheStats) Write() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.writes++
}

func (c *CacheStats) Eviction() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.evictions++
}

func (c *CacheStats) Invalidation() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.invalidations++
}

func (c *CacheStats) Insertion() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.insertions++
}

func (c *CacheStats) String() string {
	c.lock.Lock()
	defer c.lock.Unlock()

	return fmt.Sprintf(
		"Read Hit Rate: %.4f\n"+
			"Write Hit Rate: %.4f\n"+
			"Read hits: %d\n"+
			"Write hits: %d\n"+
			"Reads: %d\n"+
			"Writes: %d\n"+
			"Insertions: %d\n"+
			"Evictions: %d\n"+
			"Invalidations: %d\n"+
			"Mean Read Latency: %.2f usecs\n"+
			"Mean Write Latency: %.2f usecs\n",
		c.readHitRate(),
		c.writeHitRate(),
		c.readhits,
		c.writehits,
		c.reads,
		c.writes,
		c.insertions,
		c.evictions,
		c.invalidations,
		c.treads.MeanTimeUsecs(),
		c.twrites.MeanTimeUsecs())
}

func (c *CacheStats) Dump() string {
	c.lock.Lock()
	defer c.lock.Unlock()

	return fmt.Sprintf(
		"%v,"+ // Read Hit Rate 1
			"%v,"+ // Write Hit Rate 2
			"%d,"+ // Read Hits 3
			"%d,"+ // Write Hits 4
			"%d,"+ // Reads 6
			"%d,"+ // Writes 7
			"%d,"+ // Insertions 9
			"%d,"+ // Evictions 10
			"%d,"+ // Invalidations 11
			"%v,"+ // Mean Reads 12
			"%v\n", // Mean Writes 13
		c.readHitRate(),
		c.writeHitRate(),
		c.readhits,
		c.writehits,
		c.reads,
		c.writes,
		c.insertions,
		c.evictions,
		c.invalidations,
		c.treads.MeanTimeUsecs(),
		c.twrites.MeanTimeUsecs())
}

func (c *CacheStats) DumpDelta(prev *CacheStats) string {
	c.lock.Lock()
	defer c.lock.Unlock()

	return fmt.Sprintf(
		"%v,"+ // Read Hit Rate 1
			"%v,"+ // Write Hit Rate 2
			"%d,"+ // Read Hits 3
			"%d,"+ // Write Hits 4
			"%d,"+ // Reads 6
			"%d,"+ // Writes 7
			"%d,"+ // Insertions 9
			"%d,"+ // Evictions 10
			"%d,"+ // Invalidations 11
			"%v,"+ // Mean Reads 12
			"%v\n", // Mean Writes 13
		c.readHitRateDelta(prev),
		c.writeHitRateDelta(prev),
		c.readhits-prev.readhits,
		c.writehits-prev.writehits,
		c.reads-prev.reads,
		c.writes-prev.writes,
		c.insertions-prev.insertions,
		c.evictions-prev.evictions,
		c.invalidations-prev.invalidations,
		c.treads.DeltaMeanTimeUsecs(prev.treads),
		c.twrites.DeltaMeanTimeUsecs(prev.twrites))
}
