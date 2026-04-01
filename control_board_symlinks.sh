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

# Construct the rule
RULE="ACTION==\"add\", SUBSYSTEM==\"tty\", ENV{ID_VENDOR_ID}==\"$ID_VENDOR\", ENV{ID_MODEL_ID}==\"$ID_PRODUCT\", ENV{ID_SERIAL_SHORT}==\"$SERIAL\", SYMLINK+=\"$SYMLINK_NAME\""

# Check if this serial already exists in the file to avoid duplicates
if grep -q "$SERIAL" "$RULE_FILE" 2>/dev/null; then
    echo "Notice: A rule for serial $SERIAL already exists in $RULE_FILE. Skipping append."
else
    echo "Appending rule to $RULE_FILE..."
    # The -a flag appends instead of overwriting
    echo "$RULE" | sudo tee -a $RULE_FILE > /dev/null
fi

echo "Reloading udev..."
sudo udevadm control --reload-rules
sudo udevadm trigger

echo "Done! The board with serial $SERIAL is mapped to /dev/$SYMLINK_NAME"