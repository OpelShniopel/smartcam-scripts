import cv2
import os
import serial
import sys
import scf4_tools
import time
import threading
import camera
import json

# --- Axis status bit indices ---
CHC_MOVE    = 8
CHB_MOVE    = 7
CHA_MOVE    = 6
CHC_PI      = 5
CHB_PI      = 4
CHA_PI      = 3
CHC_POS     = 2
CHB_POS     = 1
CHA_POS     = 0

# --- Zoom (A axis) calibration range ---
# 32000 = home/wide end. Adjust ZOOM_MAX to your true 3x position.
ZOOM_MIN          = 30000
ZOOM_MAX          = 42000
ZOOM_STEPS        = 100        # motor steps between calibration points

# --- Focus (B axis) sweep range (from working autofocus script) ---
FOCUS_SWEEP_MIN   = 32000
FOCUS_SWEEP_MAX   = 37000
FOCUS_SWEEP_SPEED = 5000      # slower during sweep for more samples
FOCUS_NORMAL_SPEED = 600
FOCUS_OFFSET      = -100      # empirical camera/motor frame offset

OUTPUT_JSON = "zoom_focus_table.json"


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

# Enable focus tracker on centre ROI so focus_val is always live
c.focus_tracker(True, int(1920 / 4), int(1080 / 4), size=100)

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

print("Get bus voltage")
adc = scf4_tools.send_command(ser, "M247", echo=True)
adc = float(adc.split("=")[1])
volts = adc / 4096.0 * 3.3 / 0.5
print("  V(bus)=", round(volts, 2), "V")


# ──────────────────────────────────────────────
# Home axis A (zoom) — exact logic from working script
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
# Home axis B (focus) — exact logic from working script
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

print()
print("Get status")
status_str = scf4_tools.send_command(ser, "!1")
status = scf4_tools.parse_status(status_str)
print(status_str)
print("Both axes homed")


# ──────────────────────────────────────────────
# Helper: find best focus at current zoom position
# ──────────────────────────────────────────────
def find_best_focus(zoom_pos):
    """
    Sweep focus axis across [FOCUS_SWEEP_MIN, FOCUS_SWEEP_MAX],
    sample focus_val throughout, return motor position with peak sharpness.
    """
    focus_table = []

    scf4_tools.send_command(ser, f"G0 B{FOCUS_SWEEP_MIN}")
    scf4_tools.wait_homing(ser, 1, CHB_MOVE)
    time.sleep(0.05)

    scf4_tools.send_command(ser, f"M240 B{FOCUS_SWEEP_SPEED}")
    scf4_tools.send_command(ser, f"G0 B{FOCUS_SWEEP_MAX}")

    for i in range(10000):
        status_str = scf4_tools.send_command(ser, "!1")
        status = scf4_tools.parse_status(status_str)
        focus_table.append([status[CHB_POS], c.focus_val])
        time.sleep(0.01)
        if status[CHB_MOVE] != 1:
            break

    time.sleep(0.1)
    scf4_tools.send_command(ser, f"M240 B{FOCUS_NORMAL_SPEED}")

    if not focus_table:
        print(f"  [zoom={zoom_pos}] WARNING: empty focus table")
        return None, None

    best_pos, best_val = max(focus_table, key=lambda f: f[1])
    best_pos += FOCUS_OFFSET

    print(f"  [zoom={zoom_pos}] best focus pos={best_pos}  val={best_val:.2f}")
    return int(best_pos), float(best_val)


# ──────────────────────────────────────────────
# Build zoom-to-focus calibration table
# ──────────────────────────────────────────────
c.set_cam_text("Building zoom-focus table...")
print()
print("=== Building zoom-focus table ===")
print(f"Zoom range: {ZOOM_MIN} - {ZOOM_MAX}, step={ZOOM_STEPS}")

zoom_focus_table = {}
zoom_positions = list(range(ZOOM_MIN, ZOOM_MAX + 1, ZOOM_STEPS))
total = len(zoom_positions)

for idx, zoom_pos in enumerate(zoom_positions):
    c.set_cam_text(f"Calibrating {idx+1}/{total}  zoom={zoom_pos}")
    print(f"\nStep {idx+1}/{total} - moving zoom to {zoom_pos}")

    scf4_tools.send_command(ser, f"G0 A{zoom_pos}")
    scf4_tools.wait_homing(ser, 1, CHA_MOVE)
    time.sleep(0.1)

    best_pos, best_val = find_best_focus(zoom_pos)

    if best_pos is not None:
        zoom_focus_table[str(zoom_pos)] = {
            "zoom_pos":  zoom_pos,
            "focus_pos": best_pos,
            "focus_val": round(best_val, 2),
        }
        # Park focus at best position before moving zoom
        scf4_tools.send_command(ser, f"G0 B{best_pos}")
        scf4_tools.wait_homing(ser, 1, CHB_MOVE)


# ──────────────────────────────────────────────
# Save table to JSON
# ──────────────────────────────────────────────
with open(OUTPUT_JSON, "w") as f:
    json.dump(zoom_focus_table, f, indent=2)

print(f"\n=== Calibration complete - {len(zoom_focus_table)} entries saved to {OUTPUT_JSON} ===")
c.set_cam_text(f"Done - {len(zoom_focus_table)} zoom points saved")
time.sleep(1)


# ──────────────────────────────────────────────
# Interactive loop: click to jump to calibrated focus
# ──────────────────────────────────────────────
def lookup_focus(zoom_pos):
    if not zoom_focus_table:
        return None
    keys = [int(k) for k in zoom_focus_table.keys()]
    nearest = min(keys, key=lambda k: abs(k - zoom_pos))
    return zoom_focus_table[str(nearest)]["focus_pos"]


print("\nEntering interactive mode - click the image to jump to calibrated focus.")

while True:
    status_str = scf4_tools.send_command(ser, "!1")
    status = scf4_tools.parse_status(status_str)
    current_zoom = status[CHA_POS]

    c.set_cam_text(f"Click to autofocus  |  zoom={current_zoom}")
    time.sleep(0.1)

    if c.mouse_clicked:
        c.mouse_clicked = False

        focus_pos = lookup_focus(current_zoom)
        if focus_pos is not None:
            c.set_cam_text(f"Moving to focus pos {focus_pos} (table lookup)")
            print(f"Click - zoom={current_zoom}, table focus pos={focus_pos}")
            scf4_tools.send_command(ser, f"G0 B{focus_pos}")
            scf4_tools.wait_homing(ser, 1, CHB_MOVE)
        else:
            # Fallback: live sweep if table is empty
            c.set_cam_text("No table - sweeping for focus...")
            best_pos, _ = find_best_focus(current_zoom)
            if best_pos:
                scf4_tools.send_command(ser, f"G0 B{best_pos}")
                scf4_tools.wait_homing(ser, 1, CHB_MOVE)

    if not c.running:
        break

print("Stopping camera")
c.stop()