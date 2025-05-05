#!/bin/bash

timestamp() {
  date +"%T.%N"
}
folder=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
if [ ! -f /opt/tinkerforge/pi1 ]; then
    touch /opt/tinkerforge/pi1
    echo "$(timestamp) Scheduling 192.168.0.143" >> $folder/cluster_scheduling.log
    echo "192.168.0.143"
elif [ -f /opt/tinkerforge/pi1 ] && [ ! -f /opt/tinkerforge/pi2 ]; then
    touch /opt/tinkerforge/pi2
    echo "$(timestamp) Scheduling 192.168.0.59" >> $folder/cluster_scheduling.log
    echo "192.168.0.59"
elif [ -f /opt/tinkerforge/pi2 ] && [ ! -f /opt/tinkerforge/pi3 ]; then
    touch /opt/tinkerforge/pi3
    echo "$(timestamp) Scheduling 192.168.0.178" >> $folder/cluster_scheduling.log
    echo "192.168.0.178"
#elif [ -f /opt/tinkerforge/pi3 ] && [ ! -f /opt/tinkerforge/pi4 ]; then
#    touch /opt/tinkerforge/pi4
#    echo "Scheduling pi4"
else
    echo "All nodes are already scheduled to start."
fi