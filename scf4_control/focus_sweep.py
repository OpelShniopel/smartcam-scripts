import serial
import time
import lens_helpers

# --- CONFIGURATION ---
SERIAL_PORT_Z = "/dev/zoom_control"
ZOOM_SPEED    = 1000
FOCUS_SPEED   = 1200
CSV_FILE      = "zoom_focus_table.csv"
FOCUS_BIAS    = -1040

ZOOM_MIN_STEPS = 30000
ZOOM_MAX_STEPS = 41000

# Steps per command — smaller = smoother focus tracking, more serial traffic
SWEEP_STEP = 100

# Delay between commands in seconds — controls sweep speed
# 0.08 s * (11000 steps / 100 step) = ~8 s for full range
CMD_INTERVAL = 0.5


def sweep(ser, focus_interp, start, end, step):
    direction = 1 if end > start else -1
    pos = start
    while (direction == 1 and pos <= end) or (direction == -1 and pos >= end):
        focus = int(focus_interp(pos)) + FOCUS_BIAS
        ser.write(f"G0 A{pos} B{focus}\r\n".encode())
        ser.readline()  # consume board response
        print(f"zoom={pos:6d}  focus={focus:6d}")
        time.sleep(CMD_INTERVAL)
        pos += direction * SWEEP_STEP
    # Ensure we land exactly on the endpoint
    focus = int(focus_interp(end)) + FOCUS_BIAS
    ser.write(f"G0 A{end} B{focus}\r\n".encode())
    ser.readline()


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

    focus_interp = lens_helpers.load_focus_interpolator(CSV_FILE)

    print(f"\nSweep in  — zoom {ZOOM_MIN_STEPS} → {ZOOM_MAX_STEPS}")
    sweep(ser, focus_interp, ZOOM_MIN_STEPS, ZOOM_MAX_STEPS, SWEEP_STEP)

    # Wait at tele end so you can inspect focus before sweeping back
    time.sleep(1.0)

    print(f"\nSweep out — zoom {ZOOM_MAX_STEPS} → {ZOOM_MIN_STEPS}")
    sweep(ser, focus_interp, ZOOM_MAX_STEPS, ZOOM_MIN_STEPS, SWEEP_STEP)

    lens_helpers.wait_homing(ser, 1, lens_helpers.CHA_MOVE, timeout_sec=10.0)
    lens_helpers.wait_homing(ser, 1, lens_helpers.CHB_MOVE, timeout_sec=10.0)
    print("Done.")
    ser.close()


if __name__ == "__main__":
    main()
