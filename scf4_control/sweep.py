import cv2
import os
import serial
import sys
import scf4_control.scf4_tools as scf4_tools
import time
import threading
import scf4_control.camera as camera
import csv
import numpy as np
from scipy.interpolate import interp1d

CHB_MOVE    = 7
CHA_MOVE    = 6
CHB_PI      = 4
CHA_PI      = 3

CSV_FILE = "zoom_focus_table.csv"

# ──────────────────────────────────────────────
# Tune these
# ──────────────────────────────────────────────
SWEEP_SPEED = 500  # motor speed during sweep
SWEEP_DELAY = 0.001  # seconds between commands (lower = faster)
SWEEP_STEP  = 10     # motor units skipped per command — this is the main speed lever
                     # 1 = ~770 commands (slowest), 50 = ~15 commands (fast),
                     # try 10-100 depending on how smooth vs fast you want


# ──────────────────────────────────────────────
# Serial / controller setup
# ──────────────────────────────────────────────
ser = serial.Serial()
ser.port     = 'COM8'
ser.baudrate = 115200
ser.timeout  = 5

print("Open COM port:", ser.port)
ser.open()
ser.flushInput()
ser.flushOutput()


# ──────────────────────────────────────────────
# Camera setup
# ──────────────────────────────────────────────
c = camera.Cam()
print("Starting cam")
c.start()

print("Waiting for camera")
while c.fps == 0:
    time.sleep(0.1)
print("Cam is operational")


# ──────────────────────────────────────────────
# Controller initialisation
# ──────────────────────────────────────────────
c.set_cam_text("Prepare")
print("Read controller version strings")
scf4_tools.send_command(ser, "$S", echo=True)

print("Initialize controller")
scf4_tools.send_command(ser, "$B2", echo=True)

print("Set motion to forced mode")
scf4_tools.send_command(ser, "M231 A", echo=True)

print("Set stepping mode")
scf4_tools.send_command(ser, "M243 C6", echo=True)

print("Set normal move")
scf4_tools.send_command(ser, "M230", echo=True)

print("Set to rel movement mode")
scf4_tools.send_command(ser, "G91", echo=True)

print("Energize PI leds")
scf4_tools.send_command(ser, "M238", echo=True)

print("Set motor power")
scf4_tools.send_command(ser, "M234 A190 B190 C190 D90", echo=True)

print("Set motor sleep power")
scf4_tools.send_command(ser, "M235 A120 B120 C120", echo=True)

print("Set motor drive speed")
scf4_tools.send_command(ser, "M240 A600 B600 C600", echo=True)

print("Set PI low/high detection voltage")
scf4_tools.send_command(ser, "M232 A400 B400 C400 E700 F700 G700", echo=True)

print("Filter = VIS")
scf4_tools.send_command(ser, "M7", echo=True)


# ──────────────────────────────────────────────
# Home axis A (zoom)
# ──────────────────────────────────────────────
c.set_cam_text("Homing A")
print()
print("Home axis A")
print("Get status")
status_str = scf4_tools.send_command(ser, "!1")
status = scf4_tools.parse_status(status_str)
print(status_str)

if status[CHA_PI] == 1:
    print("Dir 1")
    scf4_tools.send_command(ser, "G91")
    scf4_tools.send_command(ser, "M231 A")
    scf4_tools.send_command(ser, "G0 A-100")
    scf4_tools.wait_homing(ser, status[CHA_PI], CHA_PI)
else:
    print("Dir 2")
    scf4_tools.send_command(ser, "G91")
    scf4_tools.send_command(ser, "M231 A")
    scf4_tools.send_command(ser, "G0 A+100")
    scf4_tools.wait_homing(ser, status[CHA_PI], CHA_PI)

print("Motor normal mode")
scf4_tools.send_command(ser, "M230 A")
scf4_tools.send_command(ser, "G0 A+200")
scf4_tools.wait_homing(ser, 1, CHA_MOVE)

print("Motor forced mode")
scf4_tools.send_command(ser, "G91")
scf4_tools.send_command(ser, "M231 A")
scf4_tools.send_command(ser, "G0 A-100")
scf4_tools.wait_homing(ser, status[CHA_PI], CHA_PI)

print("Set current coordinate as middle")
scf4_tools.send_command(ser, "G92 A32000")
scf4_tools.send_command(ser, "M230 A")
scf4_tools.send_command(ser, "G90")


# ──────────────────────────────────────────────
# Home axis B (focus)
# ──────────────────────────────────────────────
c.set_cam_text("Homing B")
print()
print("Home axis B")
print("Get status")
status_str = scf4_tools.send_command(ser, "!1")
status = scf4_tools.parse_status(status_str)
print(status_str)

if status[CHB_PI] == 0:
    print("Dir 1")
    scf4_tools.send_command(ser, "G91")
    scf4_tools.send_command(ser, "M231 B")
    scf4_tools.send_command(ser, "G0 B+100")
    scf4_tools.wait_homing(ser, status[CHB_PI], CHB_PI)
else:
    print("Dir 2")
    scf4_tools.send_command(ser, "G91")
    scf4_tools.send_command(ser, "M231 B")
    scf4_tools.send_command(ser, "G0 B-100")
    scf4_tools.wait_homing(ser, status[CHB_PI], CHB_PI)

print("Motor normal mode")
scf4_tools.send_command(ser, "M230 B")
scf4_tools.send_command(ser, "G0 B-200")
scf4_tools.wait_homing(ser, 1, CHB_MOVE)

print("Motor forced mode")
scf4_tools.send_command(ser, "G91")
scf4_tools.send_command(ser, "M231 B")
scf4_tools.send_command(ser, "G0 B+100")
scf4_tools.wait_homing(ser, status[CHB_PI], CHB_PI)

print("Set current coordinate as middle")
scf4_tools.send_command(ser, "G92 B32000")
scf4_tools.send_command(ser, "M230 B")
scf4_tools.send_command(ser, "G90")


# ──────────────────────────────────────────────
# Load CSV and interpolate
# ──────────────────────────────────────────────
print()
print(f"Loading {CSV_FILE}")
zoom_pts = []
focus_pts = []
with open(CSV_FILE, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        zoom_pts.append(int(row["zoom_pos"]))
        focus_pts.append(int(row["focus_pos"]))

print(f"  {len(zoom_pts)} points loaded")

# Build cubic interpolator, one command every SWEEP_STEP motor units
zoom_min, zoom_max = min(zoom_pts), max(zoom_pts)
focus_interp = interp1d(zoom_pts, focus_pts, kind='cubic')
zoom_dense = np.arange(zoom_min, zoom_max + 1, SWEEP_STEP)
focus_dense = focus_interp(zoom_dense).astype(int)

# wide -> tele (zoom in)
sweep_table = list(zip(zoom_dense.tolist(), focus_dense.tolist()))
print(f"  {len(sweep_table)} steps (SWEEP_STEP={SWEEP_STEP})")


# ──────────────────────────────────────────────
# Move to start position
# ──────────────────────────────────────────────
print("Set motor drive speed")
scf4_tools.send_command(ser, f"M240 A{SWEEP_SPEED} B{SWEEP_SPEED} C{SWEEP_SPEED}", echo=True)

c.set_cam_text("Moving to start position")
zoom_start, focus_start = sweep_table[0]
scf4_tools.send_command(ser, f"G0 A{zoom_start}")
scf4_tools.wait_homing(ser, 1, CHA_MOVE)

scf4_tools.send_command(ser, f"G0 B{focus_start}")
scf4_tools.wait_homing(ser, 1, CHB_MOVE)

print("Done - starting sweep")
time.sleep(1)


# ──────────────────────────────────────────────
# Sweep - fast-fire, no per-step wait
# ──────────────────────────────────────────────
for i, (zoom_pos, focus_pos) in enumerate(sweep_table):
    c.set_cam_text(f"zoom={zoom_pos}  focus={focus_pos}  ({i+1}/{len(sweep_table)})")
    scf4_tools.send_command(ser, f"G0 A{zoom_pos} B{focus_pos}")
    time.sleep(SWEEP_DELAY)

c.set_cam_text("Sweep done - sleeping 10s")
time.sleep(10)

c.stop()