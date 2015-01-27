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

# Make a new table by averaging data over larger period of time
# Note: It would be great to convert this to RRDtool :-)

# Default input is every 5 secs
In_Period=5

# Default output here is set to 30min
Out_Period=30*60

# Convert from pblio.data -> pblio.csv
fp = open('pblio.data', 'r')
out = open('pblio.csv', 'w')

# Cover data
ROWS=(Out_Period/In_Period) # For 5 sec rows, this is 30m
row=ROWS
out_row=1
for line in fp.readlines():
	csv = line.split(',')
	csv = map(float, csv)
	if row == ROWS:
		com = csv
		row -= 1
		continue

	com = [i+j for i,j in zip(com, csv)]
	row-=1
	if row == 0:
		avg = map(lambda x: x/ROWS, com)
		avg[0] = (In_Period*out_row*ROWS)/60
		out.write(','.join(map(str, avg))+ '\n')
		row = ROWS
		out_row += 1

out.close()
fp.close()



