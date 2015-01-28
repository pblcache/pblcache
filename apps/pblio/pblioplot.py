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
In_Period=5

# Default output here is set to 30min
Out_Period=30*60

# Convert from pblio.data -> pblio.csv
fp = open('pblio.data', 'r')
line = fp.readline()
jsondata = json.loads(line)
fp.close()

# Setup info
data_sources = ['DS:tlat_duration:COUNTER:600:0:U',
				'DS:tlat_counter:COUNTER:600:0:U']

# Create db
rrdtool.create('pblio.rrd',
	'--start', "%d" % (jsondata['time']),
	'--step', '5',
	data_sources,
	'RRA:AVERAGE:0.5:6:300',
	'RRA:AVERAGE:0.5:360:1200',
	'RRA:AVERAGE:0.5:1440:1200')

# Cover data
rrdtime=In_Period+jsondata['time']
rrd = None

fp = open('pblio.data', 'r')
for line in fp.readlines():
	stat = json.loads(line)
	rrdtool.update('pblio.rrd',
		("%d:" % rrdtime) +
		("%d:" % stat['spc']['total']['latency']['duration']) +
		("%d" % stat['spc']['total']['latency']['count']))
	rrdtime+=In_Period

fp.close()

