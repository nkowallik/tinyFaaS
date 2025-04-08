#!/bin/bash

if [ -f /opt/tinkerforge/pi3 ]; then
    rm /opt/tinkerforge/pi3
    echo "Stopping pi3"
elif [ -f /opt/tinkerforge/pi2 ]; then
    rm /opt/tinkerforge/pi2
    echo "Stopping pi2"
elif [ -f /opt/tinkerforge/pi1 ]; then
    rm /opt/tinkerforge/pi1
    echo "Stopping pi1"
else
    echo "No other nodes are running."
fi
