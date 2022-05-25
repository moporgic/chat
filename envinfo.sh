#!/bin/bash
# envinfo.sh: display the current environment
# verions 2022-05-26 by moporgic

# host name
echo "Host: $(hostname)"
# OS name and version
osinfo=$(uname -o 2>/dev/null | sed "s|GNU/||")
osinfo+=" $(uname -r | sed -E 's/[^0-9.]+.+$//g')"
if [[ $OSTYPE =~ cygwin|msys ]]; then
	ver=($(cmd /c ver 2>/dev/null | tr "[\r\n]" " "))
	(( ${#ver[@]} )) && osinfo+=" (Windows ${ver[-1]})"
fi
echo "OS: $osinfo"
# CPU model
cpuinfo=$(grep -m1 name /proc/cpuinfo | sed -E 's/.+:|\(\S+\)|CPU|[0-9]+-Core.+|@.+//g' | xargs)
nodes=$(lscpu | grep 'NUMA node(s)' | cut -d: -f2 | xargs)
if (( ${nodes:-1} != 1 )); then
	for (( i=0; i<${nodes:-1}; i++ )); do
		echo "CPU $i: $cpuinfo ($(taskset -c $(lscpu | grep 'NUMA node'$i' CPU' | cut -d: -f2) nproc)x)"
	done
else
	echo "CPU: $cpuinfo ($(nproc --all)x)"
fi
# CPU affinity
if [ "$(nproc)" != "$(nproc --all)" ]; then
	echo "CPU $(taskset -pc $$ | cut -d' ' -f4-) ($(nproc)x)"
fi
# GPU model
nvidia-smi -L 2>/dev/null | sed -E "s/ \(UUID:.+$//g" | while IFS= read GPU; do echo "$GPU"; done
# memory info
size=($(head -n1 /proc/meminfo))
echo "RAM: $(printf "%.1fG" $(<<< "${size[1]}/1024/1024" bc -l))"
