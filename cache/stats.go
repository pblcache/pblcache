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
	"sync"
)

type CacheStats struct {
	Readhits       uint64 `json:"readhits"`
	Invalidatehits uint64 `json:"invalidatehits"`
	Reads          uint64 `json:"reads"`
	Evictions      uint64 `json:"evictions"`
	Invalidations  uint64 `json:"invalidations"`
	Insertions     uint64 `json:"insertions"`
}

func (c *CacheStats) ReadHitRateDelta(prev *CacheStats) float64 {
	Reads := c.Reads - prev.Reads
	Readhits := c.Readhits - prev.Readhits
	if Reads == 0 {
		return 0.0
	} else {
		return float64(Readhits) / float64(Reads)
	}
}

func (c *CacheStats) InvalidateHitRateDelta(prev *CacheStats) float64 {
	Invalidations := c.Invalidations - prev.Invalidations
	Invalidatehits := c.Invalidatehits - prev.Invalidatehits
	if Invalidations == 0 {
		return 0.0
	} else {
		return float64(Invalidatehits) / float64(Invalidations)
	}
}

func (c *CacheStats) ReadHitRate() float64 {
	if c.Reads == 0 {
		return 0.0
	} else {
		return float64(c.Readhits) / float64(c.Reads)
	}
}

func (c *CacheStats) InvalidateHitRate() float64 {
	if c.Invalidations == 0 {
		return 0.0
	} else {
		return float64(c.Invalidatehits) / float64(c.Invalidations)
	}

}

func (c *CacheStats) String() string {

	return fmt.Sprintf(
		"Read Hit Rate: %.4f\n"+
			"Invalidate Hit Rate: %.4f\n"+
			"Read hits: %d\n"+
			"Invalidate hits: %d\n"+
			"Reads: %d\n"+
			"Insertions: %d\n"+
			"Evictions: %d\n"+
			"Invalidations: %d\n",
		c.ReadHitRate(),
		c.InvalidateHitRate(),
		c.Readhits,
		c.Invalidatehits,
		c.Reads,
		c.Insertions,
		c.Evictions,
		c.Invalidations)
}

func (c *CacheStats) Csv() string {

	return fmt.Sprintf(
		"%v,"+ // Read Hit Rate 1
			"%v,"+ // Invalidate Hit Rate 2
			"%d,"+ // Read Hits 3
			"%d,"+ // Invalidation Hits 4
			"%d,"+ // Reads 5
			"%d,"+ // Insertions 6
			"%d,"+ // Evictions 7
			"%d", // Invalidations 8
		c.ReadHitRate(),
		c.InvalidateHitRate(),
		c.Readhits,
		c.Invalidatehits,
		c.Reads,
		c.Insertions,
		c.Evictions,
		c.Invalidations)
}

func (c *CacheStats) CsvDelta(prev *CacheStats) string {

	return fmt.Sprintf(
		"%v,"+ // Read Hit Rate 1
			"%v,"+ // Invalidate Hit Rate 2
			"%d,"+ // Read Hits 3
			"%d,"+ // Invalide Hits 4
			"%d,"+ // Reads 5
			"%d,"+ // Insertions 6
			"%d,"+ // Evictions 7
			"%d", // Invalidations 8
		c.ReadHitRateDelta(prev),
		c.InvalidateHitRateDelta(prev),
		c.Readhits-prev.Readhits,
		c.Invalidatehits-prev.Invalidatehits,
		c.Reads-prev.Reads,
		c.Insertions-prev.Insertions,
		c.Evictions-prev.Evictions,
		c.Invalidations-prev.Invalidations)
}

type cachestats struct {
	readhits       uint64
	invalidatehits uint64
	reads          uint64
	insertions     uint64
	evictions      uint64
	invalidations  uint64
	lock           sync.Mutex
}

func (c *cachestats) stats() *CacheStats {
	stats := c.copy()

	return &CacheStats{
		Readhits:       stats.readhits,
		Invalidatehits: stats.invalidatehits,
		Reads:          stats.reads,
		Evictions:      stats.evictions,
		Invalidations:  stats.invalidations,
		Insertions:     stats.insertions,
	}
}

func (c *cachestats) clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.readhits = 0
	c.invalidatehits = 0
	c.reads = 0
	c.insertions = 0
	c.evictions = 0
	c.invalidations = 0
}

func (c *cachestats) copy() *cachestats {
	c.lock.Lock()
	defer c.lock.Unlock()

	statscopy := &cachestats{}
	*statscopy = *c

	return statscopy
}

func (c *cachestats) readHit() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.readhits++
}

func (c *cachestats) invalidateHit() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.invalidatehits++
}

func (c *cachestats) read() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.reads++
}

func (c *cachestats) eviction() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.evictions++
}

func (c *cachestats) invalidation() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.invalidations++
}

func (c *cachestats) insertion() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.insertions++
}
