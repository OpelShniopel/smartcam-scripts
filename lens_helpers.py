import csv
import time
from scipy.interpolate import CubicSpline

# Status field indices for the Kurokesu SCF4 board (!1 response)
CHA_PI   = 3
CHB_PI   = 4
CHA_MOVE = 6
CHB_MOVE = 7


# ── Serial communication ───────────────────────────────────────────────────────

def send_command(ser, cmd):
    """Write a command to the board and return the response line."""
    ser.write(bytes(cmd + "\r\n", 'utf8'))
    return ser.readline().decode('utf-8').strip()

def parse_status(ser):
    """Query board status (!1) and return a parsed list of ints."""
    raw = send_command(ser, "!1")
    return [int(v.strip()) for v in raw.split(",")]

def wait_homing_and_stop(ser, initial_val, axis_idx, axis_letter, timeout_sec=5.0):
    """Poll until PI triggers, then immediately send stop to minimize overshoot."""
    start = time.time()
    while time.time() - start < timeout_sec:
        status = parse_status(ser)
        if status[axis_idx] != initial_val:
            # PI just triggered — stop the motor immediately
            send_command(ser, f"M230 {axis_letter}")
            send_command(ser, "G0 {axis_letter}0".format(axis_letter=axis_letter))
            break
        time.sleep(0.005)  # tighter poll: 5ms instead of 10ms
    else:
        print(f"TIMEOUT: status index {axis_idx} did not change within {timeout_sec}s")
    time.sleep(0.05)

def wait_homing(ser, initial_val, axis_idx, timeout_sec=10.0):
    """Poll until status[axis_idx] differs from initial_val."""
    start = time.time()
    while time.time() - start < timeout_sec:
        status = parse_status(ser)
        if status[axis_idx] != initial_val:
            break
        time.sleep(0.01)
    else:
        print(f"TIMEOUT: status index {axis_idx} did not change within {timeout_sec}s")
    time.sleep(0.1)

def verify_command(ser, cmd):
    """Send a command and return True if the board replies with 'ok'."""
    try:
        ser.reset_input_buffer()
        ser.write(f"{cmd}\n".encode())
        time.sleep(0.1)
        response = ser.readline().decode().strip().lower()
        return "ok" in response
    except Exception as e:
        print(f"Communication error during command verification: {e}")
        return False


# ── Board initialisation ───────────────────────────────────────────────────────

def init_lens_board(ser, zoom_speed, focus_speed):
    """
    Send the standard startup command sequence to the Kurokesu SCF4 board.
    Call after opening the serial port (allow ~1.5 s for STM32 boot first).
    """
    init_cmds = [
        "$B2",                                          # Boot/Handshake
        "M243 C6",                                      # Stepping mode
        "M230",                                         # Normal move mode
        "G91",                                          # Relative movement
        "M238",                                         # Energize PI LEDs
        "M234 A190 B190 C190 D90",                      # Motor power (running)
        "M235 A120 B120 C120",                          # Motor power (sleep/hold)
        f"M240 A{zoom_speed} B{focus_speed} C600",      # Drive speeds
        "M232 A400 B400 C400 E700 F700 G700",           # PI voltage thresholds
    ]
    for cmd in init_cmds:
        ser.write(bytes(cmd + '\r\n', 'utf8'))
        time.sleep(0.05)


# ── Homing / calibration ───────────────────────────────────────────────────────

def calibrate_lens(ser, zoom_speed=1000, focus_speed=3000):
    print("--- Starting Lens Calibration ---")
    send_command(ser, "M238")           # Energize PI LEDs
    send_command(ser, "G91")            # Relative mode
    
    # Fast homing speed — strictly staying at 600 as you documented
    send_command(ser, "M240 A600 B600") 

    # ── Axis A (Zoom) ──────────────────────────────
    print("Homing Axis A (zoom)")
    status = parse_status(ser)

    # 1. Seek
    send_command(ser, "G91")
    send_command(ser, "M231 A")
    send_command(ser, "G0 A-100" if status[CHA_PI] == 1 else "G0 A+100")
    wait_homing(ser, status[CHA_PI], CHA_PI)

    # 2. Back off
    send_command(ser, "M230 A")
    send_command(ser, "G0 A+200")
    wait_homing(ser, 1, CHA_MOVE)

    # REFRESH STATUS: This is the ONLY change. It prevents the stale variable 
    # from instantly satisfying the next wait_homing loop and causing an offset.
    status = parse_status(ser)

    # 3. Re-approach
    send_command(ser, "G91")
    send_command(ser, "M231 A")
    send_command(ser, "G0 A-100")
    wait_homing_and_stop(ser, status[CHA_PI], CHA_PI, "A")

    send_command(ser, "G92 A32000")
    send_command(ser, "M230 A")
    send_command(ser, "G90")

    # ── Axis B (Focus) ─────────────────────────────
    print("Homing Axis B (focus)")
    status = parse_status(ser)

    # 1. Seek
    send_command(ser, "G91")
    send_command(ser, "M231 B")
    send_command(ser, "G0 B+100" if status[CHB_PI] == 0 else "G0 B-100")
    wait_homing(ser, status[CHB_PI], CHB_PI)

    # 2. Back off
    send_command(ser, "M230 B")
    send_command(ser, "G0 B-200")
    wait_homing(ser, 1, CHB_MOVE)

    # REFRESH STATUS: Again, just refreshing the state. No speed changes.
    status = parse_status(ser)
    print(f"  [CAL] B PI state before re-approach: {status[4]}")

    send_command(ser, "G91")
    send_command(ser, "M231 B")
    send_command(ser, "G0 B+100")
    wait_homing_and_stop(ser, status[CHB_PI], CHB_PI, "B")

    status = parse_status(ser)
    print(f"  [CAL] B PI state after re-approach: {status[4]}")
    print(f"  [CAL] B position at home: {status[1]}")

    send_command(ser, "G92 B32000")
    send_command(ser, "M230 B")
    send_command(ser, "G90")

    status = parse_status(ser)
    print(f"  [CAL] B position after G92: {status[1]}")

    send_command(ser, f"M240 A{zoom_speed} B{focus_speed} C600")
    print("--- Calibration Complete ---")


# ── Zoom/focus interpolation ───────────────────────────────────────────────────

def load_focus_interpolator(csv_file):
    """
    Load zoom/focus pairs from a CSV and return a cubic interpolation function.
    The returned function accepts a zoom position and returns the matching focus position.
    """
    zoom_pts, focus_pts = [], []
    with open(csv_file, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            zoom_pts.append(int(row["zoom_pos"]))
            focus_pts.append(int(row["focus_pos"]))

    print(f"Loaded {len(zoom_pts)} zoom/focus points from {csv_file}")
    return CubicSpline(zoom_pts, focus_pts, extrapolate=True)
