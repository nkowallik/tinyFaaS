#!/bin/bash
if [ ! -f /opt/tinkerforge/pi1 ]; then
    touch /opt/tinkerforge/pi1
    echo "Scheduling pi1"
elif [ -f /opt/tinkerforge/pi1 ] && [ ! -f /opt/tinkerforge/pi2 ]; then
    touch /opt/tinkerforge/pi2
    echo "Scheduling pi2"
elif [ -f /opt/tinkerforge/pi2 ] && [ ! -f /opt/tinkerforge/pi3 ]; then
    touch /opt/tinkerforge/pi3
    echo "Scheduling pi3"
else
    echo "All nodes are already scheduled to start."
fi