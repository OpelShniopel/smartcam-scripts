#!/bin/bash

TARGET_DEV="/dev/ttyACM0"
SYMLINK_NAME="zoom_control"
RULE_FILE="/etc/udev/rules.d/99-stepper-controllers.rules"

# Check if the device exists right now
if [ ! -e "$TARGET_DEV" ]; then
    echo "Error: $TARGET_DEV not found. Please ensure the board is plugged in."
    exit 1
fi

echo "Extracting hardware ID for $TARGET_DEV..."

# Grab the unique attributes
ID_VENDOR=$(udevadm info -a -n "$TARGET_DEV" | grep '{idVendor}' | head -n1 | awk -F'==' '{print $2}' | tr -d '"')
ID_PRODUCT=$(udevadm info -a -n "$TARGET_DEV" | grep '{idProduct}' | head -n1 | awk -F'==' '{print $2}' | tr -d '"')
SERIAL=$(udevadm info -a -n "$TARGET_DEV" | grep '{serial}' | head -n1 | awk -F'==' '{print $2}' | tr -d '"')

if [ -z "$SERIAL" ]; then
    echo "Error: Could not find a unique serial number for this device."
    exit 1
fi

# Create the specific rule. 

RULE="SUBSYSTEM==\"tty\", ATTRS{idVendor}==\"$ID_VENDOR\", ATTRS{idProduct}==\"$ID_PRODUCT\", ATTRS{serial}==\"$SERIAL\", SYMLINK+=\"$SYMLINK_NAME\""

echo "Writing rule to $RULE_FILE..."
echo "$RULE" | sudo tee $RULE_FILE > /dev/null

echo "Reloading udev..."
sudo udevadm control --reload-rules
sudo udevadm trigger

echo "Done! This specific board will now always be available at /dev/$SYMLINK_NAME"