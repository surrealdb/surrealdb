#!/bin/bash

PROFILE_D=profiling/profile.d
PROFILE_OUT=target/profiling/profile.out
PROFILE_TXT=target/profiling/profile.txt
PROFILE_AVG_CPU_TIME=target/profiling/profile_avg_cpu_time.txt
PROFILE_TOTAL_CPU_TIME=target/profiling/profile_total_cpu_time.txt

# Get the PID from Surreal
PID=$(pgrep surreal)
if [ -z "$PID" ]; then
  echo "Surreal not found."
  exit 1
fi

# Prepare directory and folders
mkdir -p target/profiling
rm -f $PROFILE_TXT
sudo rm -f $PROFILE_OUT

# Start tracing
echo "Tracing... Hit Ctrl-C to end."
sudo dtrace -s "$PROFILE_D" -p "$PID" -o "$PROFILE_OUT"

# Demangle
awk '{
    cmd = "rustfilt \"" $1 "\""
    cmd | getline decoded
    close(cmd)
    $1 = decoded
    print
}' $PROFILE_OUT > $PROFILE_TXT

# Sorted by AVG CPU TIME
awk '{print $2, $6, $1}' $PROFILE_TXT | sort -k1,1nr > $PROFILE_AVG_CPU_TIME
# Sorted by TOTAL CPU TIME
awk '{print $3, $6, $1}' $PROFILE_TXT | sort -k1,1nr > $PROFILE_TOTAL_CPU_TIME
