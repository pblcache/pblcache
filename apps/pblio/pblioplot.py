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
Period=60

# Convert from pblio.data -> pblio.csv
fp = open('pblio.data', 'r')
line = fp.readline()
jsondata = json.loads(line)
fp.close()

# Setup info
data_sources = ['DS:tlat_d:COUNTER:600:0:U',
				'DS:tlat_c:COUNTER:600:0:U',
				'DS:treadlat_d:COUNTER:600:0:U',
				'DS:treadlat_c:COUNTER:600:0:U',
				'DS:twritelat_d:COUNTER:600:0:U',
				'DS:twritelat_c:COUNTER:600:0:U',
				'DS:cache_hits:COUNTER:600:0:U',
				'DS:cache_reads:COUNTER:600:0:U',
				'DS:cache_ihits:COUNTER:600:0:U',
				'DS:cache_invals:COUNTER:600:0:U',
				'DS:reads:COUNTER:600:0:U',
				'DS:total:COUNTER:600:0:U',
				]

# Create db
rrdtool.create('pblio.rrd',
	'--start', "%d" % (jsondata['time']-Period),
	'--step', '%d' % (Period),
	data_sources,
	'RRA:LAST:0.5:1:2600',
	'RRA:LAST:0.5:5:2600',
	'RRA:LAST:0.5:10:2600',
	'RRA:LAST:0.5:20:2600')

# Open JSON data file
fp = open('pblio.data', 'r')
csv = open('pblio.csv', 'w')

# Cover data
start_time=jsondata['time']
end_time=0
for line in fp.readlines():
	stat = json.loads(line)

	# Calculations
	tlat_d = int(stat['spc']['total']['latency']['duration']/1000)
	tlat_c = stat['spc']['total']['latency']['count']

	treadlat_d = int(stat['spc']['read']['latency']['duration']/1000)
	treadlat_c = stat['spc']['read']['latency']['count']

	twritelat_d = int(stat['spc']['write']['latency']['duration']/1000)
	twritelat_c = stat['spc']['write']['latency']['count']

	reads = stat['spc']['asu'][0]['read']['blocks'] + stat['spc']['asu'][1]['read']['blocks']
	total = stat['spc']['asu'][0]['total']['blocks'] + stat['spc']['asu'][1]['total']['blocks']

	# Get cache
	try:
		cache_hits = stat['cache']['readhits']
		cache_reads = stat['cache']['reads']
		cache_ihits = stat['cache']['invalidatehits']
		cache_invals = stat['cache']['invalidations']
	except:
		cache_hits = 0
		cache_reads = 0
		cache_ihits = 0
		cache_invals = 0

	# Enter into rrd
	rrdtool.update('pblio.rrd',
		("%d:" % stat['time']) +
		("%d:" % tlat_d)+
		("%d:" % tlat_c)+
		("%d:" % treadlat_d)+
		("%d:" % treadlat_c)+
		("%d:" % twritelat_d)+
		("%d:" % twritelat_c)+
		("%d:" % cache_hits)+
		("%d:" % cache_reads)+
		("%d:" % cache_ihits)+
		("%d:" % cache_invals)+
		("%d:" % reads)+
		("%d" % total))

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
	'DEF:duration=pblio.rrd:tlat_d:LAST',
	'DEF:count=pblio.rrd:tlat_c:LAST',
	'DEF:readduration=pblio.rrd:treadlat_d:LAST',
	'DEF:readcount=pblio.rrd:treadlat_c:LAST',
	'DEF:writeduration=pblio.rrd:twritelat_d:LAST',
	'DEF:writecount=pblio.rrd:twritelat_c:LAST',
	'CDEF:latms=duration,count,/,1000,/',
	'CDEF:readlatms=readduration,readcount,/,1000,/',
	'CDEF:writelatms=writeduration,writecount,/,1000,/',
	'LINE2:latms#FF0000:Total Latency',
	'LINE2:readlatms#00FF00:Read Total Latency',
	'LINE2:writelatms#0000FF:Write Total Latency')

# Graph Read Hit Rate
rrdtool.graph('readhit.png',
	'--start', '%d' % start_time,
	'--end', '%d' % end_time,
	'-w 800',
	'-h 400',
	'--vertical-label=Percentage',
	'DEF:hits=pblio.rrd:cache_hits:LAST',
	'DEF:reads=pblio.rrd:cache_reads:LAST',
	'DEF:ihits=pblio.rrd:cache_ihits:LAST',
	'DEF:invals=pblio.rrd:cache_invals:LAST',
	'CDEF:readhit=hits,reads,/,100,*',
	'CDEF:invalhit=ihits,invals,/,100,*',
	'LINE2:invalhit#00FF00:Invalidate Hit',
	'LINE2:readhit#FF0000:Cache Read Hit')

# Graph Read Percentage
rrdtool.graph('readp.png',
	'--start', '%d' % start_time,
	'--end', '%d' % end_time,
	'-w 800',
	'-h 400',
	'--vertical-label=Read Percentage',
	'DEF:total=pblio.rrd:total:LAST',
	'DEF:reads=pblio.rrd:reads:LAST',
	'CDEF:readp=reads,total,/,100,*',
	'LINE2:readp#FF0000:Read Percentage')

print "Start Time = %d" % (start_time)
print "End Time = %d" % (end_time)
