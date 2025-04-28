#!/bin/bash

timestamp() {
  date +"%T.%N"
}
folder=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
if [ -f /opt/tinkerforge/pi4 ]; then
    rm /opt/tinkerforge/pi4
    echo "$(timestamp) Stopping 192.168.0.144" >> $folder/cluster_scheduling.log
    echo "192.168.0.144" # stopping pi4
elif [ -f /opt/tinkerforge/pi3 ]; then
    rm /opt/tinkerforge/pi3
    echo "$(timestamp) Stopping 192.168.0.178" >> $folder/cluster_scheduling.log
    echo "192.168.0.178" # stopping pi3
elif [ -f /opt/tinkerforge/pi2 ]; then
    rm /opt/tinkerforge/pi2
    echo "$(timestamp) Stopping 192.168.0.59" >> $folder/cluster_scheduling.log
    echo "192.168.0.59" # stopping pi2
elif [ -f /opt/tinkerforge/pi1 ]; then
    rm /opt/tinkerforge/pi1
    echo "$(timestamp) Stopping 192.168.0.143" >> $folder/cluster_scheduling.log
    echo "192.168.0.143" # stopping pi1
fi
