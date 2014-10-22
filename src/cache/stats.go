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
	readhits       uint64
	invalidatehits uint64
	reads          uint64
	evictions      uint64
	invalidations  uint64
	insertions     uint64
	lock           sync.Mutex
}

func NewCacheStats() *CacheStats {
	return &CacheStats{}
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

func (c *CacheStats) invalidateHitRateDelta(prev *CacheStats) float64 {
	invalidations := c.invalidations - prev.invalidations
	invalidatehits := c.invalidatehits - prev.invalidatehits
	if invalidations == 0 {
		return 0.0
	} else {
		return float64(invalidatehits) / float64(invalidations)
	}
}

func (c *CacheStats) readHitRate() float64 {
	if c.reads == 0 {
		return 0.0
	} else {
		return float64(c.readhits) / float64(c.reads)
	}
}

func (c *CacheStats) invalidateHitRate() float64 {
	if c.invalidations == 0 {
		return 0.0
	} else {
		return float64(c.invalidatehits) / float64(c.invalidations)
	}

}

func (c *CacheStats) Copy() *CacheStats {
	c.lock.Lock()
	defer c.lock.Unlock()

	statscopy := &CacheStats{}
	*statscopy = *c

	return statscopy
}

func (c *CacheStats) ReadHit() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.readhits++
}

func (c *CacheStats) InvalidateHit() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.invalidatehits++
}

func (c *CacheStats) Read() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.reads++
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
			"Invalidate Hit Rate: %.4f\n"+
			"Read hits: %d\n"+
			"Invalidate hits: %d\n"+
			"Reads: %d\n"+
			"Insertions: %d\n"+
			"Evictions: %d\n"+
			"Invalidations: %d\n",
		c.readHitRate(),
		c.invalidateHitRate(),
		c.readhits,
		c.invalidatehits,
		c.reads,
		c.insertions,
		c.evictions,
		c.invalidations)
}

func (c *CacheStats) Dump() string {
	c.lock.Lock()
	defer c.lock.Unlock()

	return fmt.Sprintf(
		"%v,"+ // Read Hit Rate 1
			"%v,"+ // Invalidate Hit Rate 2
			"%d,"+ // Read Hits 3
			"%d,"+ // Invalidation Hits 4
			"%d,"+ // Reads 5
			"%d,"+ // Insertions 6
			"%d,"+ // Evictions 7
			"%d\n", // Invalidations 8
		c.readHitRate(),
		c.invalidateHitRate(),
		c.readhits,
		c.invalidatehits,
		c.reads,
		c.insertions,
		c.evictions,
		c.invalidations)
}

func (c *CacheStats) DumpDelta(prev *CacheStats) string {
	c.lock.Lock()
	defer c.lock.Unlock()

	return fmt.Sprintf(
		"%v,"+ // Read Hit Rate 1
			"%v,"+ // Invalidate Hit Rate 2
			"%d,"+ // Read Hits 3
			"%d,"+ // Invalide Hits 4
			"%d,"+ // Reads 5
			"%d,"+ // Insertions 6
			"%d,"+ // Evictions 7
			"%d\n", // Invalidations 8
		c.readHitRateDelta(prev),
		c.invalidateHitRateDelta(prev),
		c.readhits-prev.readhits,
		c.invalidatehits-prev.invalidatehits,
		c.reads-prev.reads,
		c.insertions-prev.insertions,
		c.evictions-prev.evictions,
		c.invalidations-prev.invalidations)
}
