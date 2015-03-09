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
import os
import sys

if len(sys.argv) == 1:
    print "%s <pblio stat file>" % sys.argv[0]
    sys.exit(1)

pbliodata = sys.argv[1]
if not os.path.exists(pbliodata):
    print "File %s does not exist" % pbliodata
    sys.exit(1)

# Default input is every 5 secs
Period=60

# Convert from pblio.data -> pblio.csv
fp = open(pbliodata, 'r')
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
                'DS:cache_insertions:COUNTER:600:0:U',
                'DS:cache_evictions:COUNTER:600:0:U',
                'DS:cache_hits:COUNTER:600:0:U',
                'DS:cache_reads:COUNTER:600:0:U',
                'DS:cache_ihits:COUNTER:600:0:U',
                'DS:cache_invals:COUNTER:600:0:U',
                'DS:reads:COUNTER:600:0:U',
                'DS:total:COUNTER:600:0:U',
                'DS:asu1_rl_d:COUNTER:600:0:U',
                'DS:asu1_rl_c:COUNTER:600:0:U',
                'DS:asu2_rl_d:COUNTER:600:0:U',
                'DS:asu2_rl_c:COUNTER:600:0:U',
                ]

# Create db
rrdtool.create('pblio.rrd',
    '--start', "%d" % (jsondata['time']-Period),
    '--step', '%d' % (Period),
    data_sources,
    'RRA:LAST:0.5:1:2600')

# Open JSON data file
fp = open(pbliodata, 'r')

# Cover data
start_time=jsondata['time']
end_time=0
cache_items=0
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

    asu1_rl_d = int(stat['spc']['asu'][0]['read']['latency']['duration']/1000)
    asu1_rl_c = stat['spc']['asu'][0]['read']['latency']['count']

    asu2_rl_d = int(stat['spc']['asu'][1]['read']['latency']['duration']/1000)
    asu2_rl_c = stat['spc']['asu'][1]['read']['latency']['count']

    # Get cache
    try:
        cache_hits = stat['cache']['readhits']
        cache_reads = stat['cache']['reads']
        cache_ihits = stat['cache']['invalidatehits']
        cache_invals = stat['cache']['invalidations']
        cache_insertions = stat['cache']['insertions']
        cache_evictions = stat['cache']['evictions']
    except:
        cache_hits = 0
        cache_reads = 0
        cache_ihits = 0
        cache_invals = 0
        cache_insertions = 0
        cache_evictions = 0

    # Enter into rrd
    rrdtool.update('pblio.rrd',
        ("%d:" % stat['time']) +
        ("%d:" % tlat_d)+
        ("%d:" % tlat_c)+
        ("%d:" % treadlat_d)+
        ("%d:" % treadlat_c)+
        ("%d:" % twritelat_d)+
        ("%d:" % twritelat_c)+
        ("%d:" % cache_insertions)+
        ("%d:" % cache_evictions)+
        ("%d:" % cache_hits)+
        ("%d:" % cache_reads)+
        ("%d:" % cache_ihits)+
        ("%d:" % cache_invals)+
        ("%d:" % reads)+
        ("%d:" % total)+
        ("%d:" % asu1_rl_d)+
        ("%d:" % asu1_rl_c)+
        ("%d:" % asu2_rl_d)+
        ("%d" % asu2_rl_c))

    # Save the end time for graphs
    end_time = stat['time']

fp.close()

# Graph Total Latency
rrdtool.graph('tlat.png',
    '--start', '%d' % start_time,
    '--end', '%d' % end_time,
    '-w 800',
    '-h 400',
    '--title=Total Latency',
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

# Graph ASU1 and ASU2 Read Latency
rrdtool.graph('asu_read_latency.png',
    '--start', '%d' % start_time,
    '--end', '%d' % end_time,
    '-w 800',
    '-h 400',
    '--title=ASU Read Latency',
    '--vertical-label=Time (ms)',
    'DEF:asu1d=pblio.rrd:asu1_rl_d:LAST',
    'DEF:asu1c=pblio.rrd:asu1_rl_c:LAST',
    'DEF:asu2d=pblio.rrd:asu2_rl_d:LAST',
    'DEF:asu2c=pblio.rrd:asu2_rl_c:LAST',
    'CDEF:asu1l=asu1d,asu1c,/,1000,/',
    'CDEF:asu2l=asu2d,asu2c,/,1000,/',
    'LINE2:asu1l#FF0000:ASU1 Read Latency',
    'LINE2:asu2l#00FF00:ASU2 Read Latency')

# Graph Read Hit Rate
rrdtool.graph('readhit.png',
    '--start', '%d' % start_time,
    '--end', '%d' % end_time,
    '-w 800',
    '-h 400',
    '--title=Cache Read Hit Percentage',
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
    '--title=Pblio Read Percentage',
    '--vertical-label=Percentage',
    'DEF:total=pblio.rrd:total:LAST',
    'DEF:reads=pblio.rrd:reads:LAST',
    'CDEF:readp=reads,total,/,100,*',
    'LINE2:readp#FF0000:Read Percentage')

# Graph cache I/O
rrdtool.graph('insertions.png',
    '--start', '%d' % start_time,
    '--end', '%d' % end_time,
    '-w 800',
    '-h 400',
    '--title=Insertions, Evictions, and Invalidations',
    '--vertical-label=Number of Blocks',
    'DEF:evictions=pblio.rrd:cache_evictions:LAST',
    'DEF:insertions=pblio.rrd:cache_insertions:LAST',
    'DEF:invalidatehits=pblio.rrd:cache_ihits:LAST',
    'LINE2:insertions#0FFF00:Insertions',
    'LINE2:invalidatehits#000FFF:Invalidation Hits',
    'LINE2:evictions#FFF000:Evictions')

# Storage IOPS Reduction
rrdtool.graph('iops_reduction.png',
    '--start', '%d' % start_time,
    '--end', '%d' % end_time,
    '-w 800',
    '-h 400',
    '--title=Storage System Total IOPS Reduction',
    '--vertical-label=Percentage',
    'DEF:writes=pblio.rrd:cache_invals:LAST',
    'DEF:reads=pblio.rrd:cache_reads:LAST',
    'DEF:readhits=pblio.rrd:cache_hits:LAST',
    'CDEF:reduction=readhits,reads,writes,+,/,100,*',
    'LINE2:reduction#FF0000:Total IOPS Reduction Percentage')

print "Start Time = %d" % (start_time)
print "End Time = %d" % (end_time)
