"""
focus_map_manual.py
Manual zoom-focus calibration recorder.

Controls:
  Arrow Up / Arrow Down   — move focus by FOCUS_FINE steps
  [ / ]                   — move focus by FOCUS_COARSE steps
  Enter                   — save current zoom+focus and advance to next zoom step
  s                       — skip this zoom step without saving
  q                       — quit (saves CSV of recorded points so far)
"""

import csv
import serial
import sys
import termios
import time
import tty

import lens_helpers

# --- CONFIGURATION ---
SERIAL_PORT_Z  = "/dev/zoom_control"
ZOOM_SPEED     = 1000
FOCUS_SPEED    = 1200
OUTPUT_CSV     = "zoom_focus_table_new.csv"

ZOOM_MIN_STEPS   = 30000
ZOOM_MAX_STEPS   = 41000
ZOOM_STEP        = 100     # Motor steps between calibration points

FOCUS_FINE       = 10      # Steps per arrow key press
FOCUS_COARSE     = 200     # Steps per +/- press

FOCUS_START      = 35000   # Initial focus position for each zoom step
FOCUS_MIN        = 32000
FOCUS_MAX        = 37000


# ── Raw keyboard input ─────────────────────────────────────────────────────────

def read_key():
    """Read one keypress from stdin. Handles arrow escape sequences."""
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
        if ch == "\x1b":
            ch2 = sys.stdin.read(1)   # expect '['
            ch3 = sys.stdin.read(1)   # 'A'/'B' for arrows, digit for others
            seq = ch + ch2 + ch3
            if ch3.isdigit():         # multi-byte sequence (e.g. PgUp) — drain the '~'
                sys.stdin.read(1)
            return seq
        return ch
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


KEY_UP    = "\x1b[A"
KEY_DOWN  = "\x1b[B"


# ── Lens helpers ───────────────────────────────────────────────────────────────

def move_focus(ser, pos):
    lens_helpers.send_command(ser, f"G0 B{pos}")
    lens_helpers.wait_homing(ser, 1, lens_helpers.CHB_MOVE, timeout_sec=10.0)


def move_zoom(ser, pos):
    lens_helpers.send_command(ser, f"G0 A{pos}")
    lens_helpers.wait_homing(ser, 1, lens_helpers.CHA_MOVE, timeout_sec=15.0)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("Opening serial port...")
    ser = serial.Serial(SERIAL_PORT_Z, 115200, timeout=2)
    time.sleep(1.5)

    print("Initializing lens board...")
    lens_helpers.init_lens_board(ser, ZOOM_SPEED, FOCUS_SPEED)

    if not lens_helpers.verify_command(ser, "G90"):
        print("WARNING: Lens board did not respond to G90.")

    print("Calibrating lens...")
    lens_helpers.calibrate_lens(ser, ZOOM_SPEED, FOCUS_SPEED)

    zoom_positions = list(range(ZOOM_MIN_STEPS, ZOOM_MAX_STEPS + 1, ZOOM_STEP))
    if zoom_positions[-1] != ZOOM_MAX_STEPS:
        zoom_positions.append(ZOOM_MAX_STEPS)

    recorded = []

    print(f"\n{len(zoom_positions)} zoom positions  ({ZOOM_MIN_STEPS}–{ZOOM_MAX_STEPS}, step={ZOOM_STEP})")
    print("Controls:  Up/Down = fine  |  [/] = coarse  |  Enter = save  |  s = skip  |  q = quit\n")

    focus_pos = FOCUS_START

    for idx, zoom in enumerate(zoom_positions):
        print(f"\n[{idx+1}/{len(zoom_positions)}] zoom={zoom}  moving...")
        move_zoom(ser, zoom)
        move_focus(ser, focus_pos)
        sys.stdout.write(f"  focus={focus_pos}  (Up/Down ±{FOCUS_FINE}  [/] ±{FOCUS_COARSE}  Enter=save  s=skip)\n")
        sys.stdout.flush()

        while True:
            key = read_key()

            if key == KEY_UP:
                focus_pos = min(FOCUS_MAX, focus_pos + FOCUS_FINE)
                move_focus(ser, focus_pos)
                sys.stdout.write(f"\r  focus={focus_pos}    ")
                sys.stdout.flush()

            elif key == KEY_DOWN:
                focus_pos = max(FOCUS_MIN, focus_pos - FOCUS_FINE)
                move_focus(ser, focus_pos)
                sys.stdout.write(f"\r  focus={focus_pos}    ")
                sys.stdout.flush()

            elif key == "]":
                focus_pos = min(FOCUS_MAX, focus_pos + FOCUS_COARSE)
                move_focus(ser, focus_pos)
                sys.stdout.write(f"\r  focus={focus_pos}    ")
                sys.stdout.flush()

            elif key == "[":
                focus_pos = max(FOCUS_MIN, focus_pos - FOCUS_COARSE)
                move_focus(ser, focus_pos)
                sys.stdout.write(f"\r  focus={focus_pos}    ")
                sys.stdout.flush()

            elif key in ("\r", "\n"):
                recorded.append((zoom, focus_pos))
                sys.stdout.write(f"\n  Saved zoom={zoom}  focus={focus_pos}  ({len(recorded)} points)\n")
                sys.stdout.flush()
                break

            elif key.lower() == "s":
                sys.stdout.write(f"\n  Skipped zoom={zoom}\n")
                sys.stdout.flush()
                break

            elif key.lower() == "q":
                sys.stdout.write("\nQuitting.\n")
                sys.stdout.flush()
                _save_csv(recorded, OUTPUT_CSV)
                ser.close()
                return

    _save_csv(recorded, OUTPUT_CSV)
    ser.close()


def _save_csv(recorded, path):
    if not recorded:
        print("No points recorded — CSV not written.")
        return
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["zoom_pos", "focus_pos"])
        for zoom, focus in recorded:
            writer.writerow([zoom, focus])
    print(f"Saved {len(recorded)} points to {path}")


if __name__ == "__main__":
    main()
