#!/usr/bin/env gnuplot

set terminal png
set datafile separator ","
set key right bottom

set output "pblio_ios.png"
plot "pblio.data" using 1:2 every 5 title "Number of IOs"

set output "pblio_iops.png"
plot "pblio.data" using 1:3 every 5 title "Avg. IOs per second"

set output "pblio_latency.png"
plot "pblio.data" using 1:4 every 5 title "Avg. Latency"

set output "pblio_readhitrate.png"
plot "pblio.data" using 1:5 every 5 title "Read Hit Rate"

set output "pblio_invalhitrate.png"
plot "pblio.data" using 1:6 every 5 title "Invalidation Hit Rate"

set output "pblio_reads.png"
plot "pblio.data" using 1:9 every 5 title "Reads", \
	 "pblio.data" using 1:7 every 5 title "Read Hits"

set output "pblio_inval.png"
plot "pblio.data" using 1:10 every 5 title "Insertions", \
     "pblio.data" using 1:8 every 5 title "Invalidation Hits", \
	 "pblio.data" using 1:12 every 5 title "Invalidations"

set output "pblio_evictions.png"
plot "pblio.data" using 1:11 every 5 title "Evictions"
