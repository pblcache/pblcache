#!/usr/bin/env gnuplot

# Generates graphs for IO generated using Pblcache

# 1: Time
# 2: IOPS

# TOTAL --
# 3: Read Ios
# 4: Read Bytes Transferred
# 5: Read MB/s
# 6: Read Latency
# 7: Write Ios
# 8: Write Bytes Transferred
# 9: Write MB/s
# 10: Write Latency
# 11: Ios
# 12: Bytes Transferred
# 13: MB/s
# 14: Latency

# ASU1 -
# 15: Read Ios
# 16: Read Bytes Transferred
# 17: Read MB/s
# 18: Read Latency
# 19: Write Ios
# 20: Write Bytes Transferred
# 21: Write MB/s
# 22: Write Latency
# 23: Ios
# 24: Bytes Transferred
# 25: MB/s
# 26: Latency

# ASU2 -
# 27 .. 38

# ASU3 -
# 39 .. 50

# Cache Stats
# 51: Read Hit Rate
# 52: Invalidation Hit Rate
# 53: Read Hits
# 54: Invalidation Hits
# 55: Reads
# 56: Insertions
# 57: Evictions
# 58: Invalidation

set terminal png
set datafile separator ","
set key right top
set xlabel "Time (min)"
set style data linesp

# -------- TOTAL
set output "pblio_iops.png"
set ylabel "IOPS"
plot "pblio.csv" using 1:2  title ""

## Reads
set output "pblio_total_read_ios.png"
set ylabel "Total Read IOs"
plot "pblio.csv" using 1:3  title ""

set output "pblio_total_read_bytes.png"
set ylabel "Total Read Bytes Transferred"
plot "pblio.csv" using 1:4  title ""

set output "pblio_total_read_mbs.png"
set ylabel "Total Read MB/s"
plot "pblio.csv" using 1:5  title ""

set output "pblio_total_read_latency.png"
set ylabel "Total Read Latency (usecs)"
plot "pblio.csv" using 1:6  title ""

## Writes
set output "pblio_total_write_ios.png"
set ylabel "Total Write IOs"
plot "pblio.csv" using 1:7  title ""

set output "pblio_total_write_bytes.png"
set ylabel "Total Write Bytes Transferred"
plot "pblio.csv" using 1:8  title ""

set output "pblio_total_write_mbs.png"
set ylabel "Total Write MB/s"
plot "pblio.csv" using 1:9  title ""

set output "pblio_total_write_latency.png"
set ylabel "Total Write Latency (usecs)"
plot "pblio.csv" using 1:10  title ""

## Total IO
set output "pblio_total_ios.png"
set ylabel "Total IOs"
plot "pblio.csv" using 1:11  title ""

set output "pblio_total_bytes.png"
set ylabel "Total Bytes Transferred"
plot "pblio.csv" using 1:12  title ""

set output "pblio_total_mbs.png"
set ylabel "Total MB/s"
plot "pblio.csv" using 1:13  title ""

set output "pblio_total_latency.png"
set ylabel "Total Latency (usecs)"
plot "pblio.csv" using 1:14  title ""


# -------- ASU1
## Reads
set output "pblio_asu1_read_ios.png"
set ylabel "ASU1 Read IOs"
plot "pblio.csv" using 1:15  title ""

set output "pblio_asu1_read_bytes.png"
set ylabel "ASU1 Read Bytes Transferred"
plot "pblio.csv" using 1:16  title ""

set output "pblio_asu1_read_mbs.png"
set ylabel "ASU1 Read MB/s"
plot "pblio.csv" using 1:17  title ""

set output "pblio_asu1_read_latency.png"
set ylabel "ASU1 Read Latency (usecs)"
plot "pblio.csv" using 1:18  title ""

## Writes
set output "pblio_asu1_write_ios.png"
set ylabel "ASU1 Write IOs"
plot "pblio.csv" using 1:19  title ""

set output "pblio_asu1_write_bytes.png"
set ylabel "ASU1 Write Bytes Transferred"
plot "pblio.csv" using 1:20  title ""

set output "pblio_asu1_write_mbs.png"
set ylabel "ASU1 Write MB/s"
plot "pblio.csv" using 1:21  title ""

set output "pblio_asu1_write_latency.png"
set ylabel "ASU1 Write Latency (usecs)"
plot "pblio.csv" using 1:22  title ""

## ASU1 IO
set output "pblio_asu1_ios.png"
set ylabel "ASU1 IOs"
plot "pblio.csv" using 1:23  title ""

set output "pblio_asu1_bytes.png"
set ylabel "ASU1 Bytes Transferred"
plot "pblio.csv" using 1:24  title ""

set output "pblio_asu1_mbs.png"
set ylabel "ASU1 MB/s"
plot "pblio.csv" using 1:25  title ""

set output "pblio_asu1_latency.png"
set ylabel "ASU1 Latency (usecs)"
plot "pblio.csv" using 1:26  title ""



# -------- ASU2
## Reads
set output "pblio_asu2_read_ios.png"
set ylabel "ASU2 Read IOs"
plot "pblio.csv" using 1:27  title ""

set output "pblio_asu2_read_bytes.png"
set ylabel "ASU2 Read Bytes Transferred"
plot "pblio.csv" using 1:28  title ""

set output "pblio_asu2_read_mbs.png"
set ylabel "ASU2 Read MB/s"
plot "pblio.csv" using 1:29  title ""

set output "pblio_asu2_read_latency.png"
set ylabel "ASU2 Read Latency (usecs)"
plot "pblio.csv" using 1:30  title ""

## Writes
set output "pblio_asu2_write_ios.png"
set ylabel "ASU2 Write IOs"
plot "pblio.csv" using 1:31  title ""

set output "pblio_asu2_write_bytes.png"
set ylabel "ASU2 Write Bytes Transferred"
plot "pblio.csv" using 1:32  title ""

set output "pblio_asu2_write_mbs.png"
set ylabel "ASU2 Write MB/s"
plot "pblio.csv" using 1:33  title ""

set output "pblio_asu2_write_latency.png"
set ylabel "ASU2 Write Latency (usecs)"
plot "pblio.csv" using 1:34  title ""

## ASU2 IO
set output "pblio_asu2_ios.png"
set ylabel "ASU2 IOs"
plot "pblio.csv" using 1:35  title ""

set output "pblio_asu2_bytes.png"
set ylabel "ASU2 Bytes Transferred"
plot "pblio.csv" using 1:36  title ""

set output "pblio_asu2_mbs.png"
set ylabel "ASU2 MB/s"
plot "pblio.csv" using 1:37  title ""

set output "pblio_asu2_latency.png"
set ylabel "ASU2 Latency (usecs)"
plot "pblio.csv" using 1:38  title ""


# -------- ASU3
## Reads
set output "pblio_asu3_read_ios.png"
set ylabel "ASU3 Read IOs"
plot "pblio.csv" using 1:39  title ""

set output "pblio_asu3_read_bytes.png"
set ylabel "ASU3 Read Bytes Transferred"
plot "pblio.csv" using 1:40  title ""

set output "pblio_asu3_read_mbs.png"
set ylabel "ASU3 Read MB/s"
plot "pblio.csv" using 1:41  title ""

set output "pblio_asu3_read_latency.png"
set ylabel "ASU3 Read Latency (usecs)"
plot "pblio.csv" using 1:42  title ""

## Writes
set output "pblio_asu3_write_ios.png"
set ylabel "ASU3 Write IOs"
plot "pblio.csv" using 1:43  title ""

set output "pblio_asu3_write_bytes.png"
set ylabel "ASU3 Write Bytes Transferred"
plot "pblio.csv" using 1:44  title ""

set output "pblio_asu3_write_mbs.png"
set ylabel "ASU3 Write MB/s"
plot "pblio.csv" using 1:45  title ""

set output "pblio_asu3_write_latency.png"
set ylabel "ASU3 Write Latency (usecs)"
plot "pblio.csv" using 1:46  title ""

## ASU3 IO
set output "pblio_asu3_ios.png"
set ylabel "ASU3 IOs"
plot "pblio.csv" using 1:47  title ""

set output "pblio_asu3_bytes.png"
set ylabel "ASU3 Bytes Transferred"
plot "pblio.csv" using 1:48  title ""

set output "pblio_asu3_mbs.png"
set ylabel "ASU3 MB/s"
plot "pblio.csv" using 1:49  title ""

set output "pblio_asu3_latency.png"
set ylabel "ASU3 Latency (usecs)"
plot "pblio.csv" using 1:50  title ""


# --------- Cache
set output "pblio_readhitrate.png"
set ylabel "Read Hit Rate"
plot "pblio.csv" using 1:51  title ""

set output "pblio_invalhitrate.png"
set ylabel "Invalidation Hit Rate"
plot "pblio.csv" using 1:52  title ""

set output "pblio_reads.png"
unset ylabel
plot "pblio.csv" using 1:55  title "Reads", \
	 "pblio.csv" using 1:53  title "Read Hits"

set output "pblio_insertions.png"
set ylabel "Insertions"
plot "pblio.csv" using 1:56  title ""

set output "pblio_inval.png"
unset ylabel
plot "pblio.csv" using 1:54  title "Invalidation Hits", \
	 "pblio.csv" using 1:58  title "Invalidations"

set output "pblio_evictions.png"
set ylabel "Evictions"
plot "pblio.csv" using 1:57  title ""

