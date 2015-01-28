#!/usr/bin/env python

#
# Copyright (c) 2014 The pblcache Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import rrdtool

# Default input is every 5 secs
Period=5

# Convert from pblio.data -> pblio.csv
fp = open('pblio.data', 'r')
line = fp.readline()
jsondata = json.loads(line)
fp.close()

# Setup info
data_sources = ['DS:tlat_d:COUNTER:600:0:U',
				'DS:tlat_c:COUNTER:600:0:U',
				'DS:cache_hits:COUNTER:600:0:U',
				'DS:cache_reads:COUNTER:600:0:U',
				]

# Create db
rrdtool.create('pblio.rrd',
	'--start', "%d" % (jsondata['time']-Period),
	'--step', '%d' % (Period),
	data_sources,
	'RRA:AVERAGE:0.5:120:2600',
	'RRA:AVERAGE:0.5:360:2600')

# Open JSON data file
fp = open('pblio.data', 'r')

# Cover data
start_time=jsondata['time']
rrdtime = start_time
end_time=0
for line in fp.readlines():
	stat = json.loads(line)

	# Calculations
	tlat_d = int(stat['spc']['total']['latency']['duration']/1000)
	tlat_c = stat['spc']['total']['latency']['count']

	# Get cache
	try:
		cache_hits = stat['cache']['readhits']
		cache_reads = stat['cache']['reads']
	except:
		cache_hits = 0
		cache_reads = 0

	# Enter into rrd
	rrdtool.update('pblio.rrd',
		("%d:" % rrdtime) +
		("%d:" % tlat_d)+
		("%d:" % tlat_c)+
		("%d:" % cache_hits)+
		("%d" % cache_reads))
	rrdtime += Period

	# Save the end time for graphs
	end_time = stat['time']

fp.close()

# Graph Total Latency
rrdtool.graph('tlat.png',
	'--start', '%d' % start_time,
	'--end', '%d' % end_time,
	'-w 800',
	'-h 400',
	'--vertical-label=Time (ms)',
	'DEF:duration=pblio.rrd:tlat_d:AVERAGE',
	'DEF:count=pblio.rrd:tlat_c:AVERAGE',
	'CDEF:latms=duration,count,/,1000,/',
	'LINE2:latms#FF0000:Total Latency')

# Graph Read Hit Rate
rrdtool.graph('readhit.png',
	'--start', '%d' % start_time,
	'--end', '%d' % end_time,
	'-w 800',
	'-h 400',
	'--vertical-label=Percentage',
	'DEF:hits=pblio.rrd:cache_hits:AVERAGE',
	'DEF:reads=pblio.rrd:cache_reads:AVERAGE',
	'CDEF:readhit=hits,reads,/,100,*',
	'LINE2:readhit#FF0000:Cache Read Hit')

print "Start Time = %d" % (start_time)
print "End Time = %d" % (end_time)
