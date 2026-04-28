import serial
import time

DEBUG = False

# --- CONFIGURATION ---
SERIAL_PORT_P = "/dev/pan_control_esp32"
BAUD_RATE     = 460800

ENABLE_HOMING = True

# Stationary camera frame dimensions (must match inference output)
STATIONARY_FRAME_W  = 1280
STATIONARY_CENTER_X = STATIONARY_FRAME_W / 2

# Rogue detection (in stationary-cam pixel space)
MAX_ERROR_JUMP = 200

# Coasting
MAX_COAST_FRAMES = 30
COAST_ERROR_PX   = 60   # px — ghost error to keep gentle momentum during coast

def open_serial_with_retry(port_path, baud, retries=5, delay=0.5):
    last_exc = None
    for attempt in range(retries):
        try:
            s = serial.Serial(port_path, baud, timeout=1)
            s.set_buffer_size(rx=8192, tx=8192)
            time.sleep(0.1)
            s.reset_input_buffer()
            return s
        except serial.SerialException as e:
            last_exc = e
            print(f"Port {port_path} not ready (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay * (attempt + 1))
    raise last_exc

class PanController:
    def __init__(self):
        self.jogging        = False
        self.last_error_x   = 0.0
        self.lost_frames    = 0
        self.last_direction = 0
        self.rogue_patience = 0

        try:
            self.ser_p = open_serial_with_retry(SERIAL_PORT_P, BAUD_RATE)
            time.sleep(0.8)
            self.ser_p.reset_input_buffer()
            print(f"SUCCESS: Connected to Pan Motor on {SERIAL_PORT_P}")

            if ENABLE_HOMING:
                self._do_homing()
            else:
                print("Homing skipped. Zeroing position.")
                self.ser_p.write(b"Z\n")

            print("Controller Ready.")

        except Exception as e:
            print(f"WARNING: Pan Motor serial port not found. ({e})")
            self.ser_p = None

    # ------------------------------------------------------------------
    def _do_homing(self):
        if not self.ser_p:
            return
        print("Homing pan motor…")
        self.ser_p.write(b"H\n")
        deadline = time.time() + 20.0
        while time.time() < deadline:
            raw = self.ser_p.readline().decode("utf-8", errors="ignore").strip()
            if not raw:
                continue
            print(f"  [HOMING] {raw}")
            if raw == "OK":
                print("Homing complete.")
                return
            if raw.startswith("ERR"):
                print(f"Homing error: {raw}")
                return
        print("WARNING: Homing timed out.")

    # ------------------------------------------------------------------
    def send_command(self, error_x, speed_scale=1.0):
        """Send pixel error to ESP32. Speed curve runs onboard."""
        if not self.ser_p:
            return
        scale_pct = max(1, min(100, int(speed_scale * 100)))
        if scale_pct == 100:
            self.ser_p.write(f"X{int(error_x)}\n".encode())
        else:
            self.ser_p.write(f"X{int(error_x)},{scale_pct}\n".encode())
        self.jogging = True

    # ------------------------------------------------------------------
    def process_detection(self, detections, speed_scale=1.0):
        """Detections come from the stationary camera.
        Sends raw pixel error from stationary-cam centre to the ESP32.
        The ESP32 (tmc_control_stationary) maps this to a target step
        position and closes the loop internally."""
        ball = next((d for d in detections if d["class"] == "BALL"), None)

        if not ball:
            self.lost_frames += 1
            if self.lost_frames > MAX_COAST_FRAMES:
                self._stop_jog()
                return
            if self.jogging and self.lost_frames <= 8:
                ghost_error = COAST_ERROR_PX * self.last_direction
                self.send_command(ghost_error, speed_scale=speed_scale)
            return

        # Error in stationary-cam pixel space — sent as-is, ESP32 handles mapping
        error_x = ball["center_x"] - STATIONARY_CENTER_X

        # Rogue jump rejection
        if self.lost_frames < 1 and self.rogue_patience < 3 and self.last_error_x != 0.0:
            if abs(error_x - self.last_error_x) > MAX_ERROR_JUMP:
                self.rogue_patience += 1
                return

        self.rogue_patience = 0
        self.lost_frames    = 0

        if DEBUG:
            print(f"[DET] stationary_err={error_x:.1f}")

        self.send_command(error_x, speed_scale=speed_scale)
        self.last_direction = 1 if error_x > 0 else -1
        self.last_error_x   = error_x
        self.jogging        = True

    # ------------------------------------------------------------------
    def _stop_jog(self):
        if not self.ser_p:
            return
        self.ser_p.write(b"L\n")
        self.jogging = False

    # ------------------------------------------------------------------
    def return_home(self):
        if not self.ser_p:
            return
        self.ser_p.write(b"S\n")
        time.sleep(0.05)
        self.ser_p.write(b"G0\n")
