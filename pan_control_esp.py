import serial
import time

DEBUG = True

# --- CONFIGURATION ---
SERIAL_PORT_P = "/dev/pan_control_esp32"   # Update to your ESP32 port (e.g. /dev/ttyUSB0)
BAUD_RATE     = 921600

# Enable homing on startup — requires limit switch to be wired to GPIO 27.
# Set False until the limit switch is connected.
ENABLE_HOMING = True

# Frame dimensions (must match inference output)
FRAME_W  = 1280
FRAME_H  = 720
CENTER_X = FRAME_W / 2

# --- PHYSICAL CONSTANTS ---
# 5000 steps = 40°  →  125 steps/degree
STEPS_PER_DEG  = 125
DEGREES_PER_UNIT = 0.5              # legacy unit: 1 unit = 0.5°
STEPS_PER_UNIT   = STEPS_PER_DEG * DEGREES_PER_UNIT   # 62.5

# --- CONTROL LIMITS (steps) ---
PAN_MAX_STEPS =  int(90 * STEPS_PER_DEG)   #  11250  (+90°)
PAN_MIN_STEPS = -int(35 * STEPS_PER_DEG)   #  -4375  (-35°)

# --- CONTROL TUNING ---
DEADZONE_PAN   = 50                              # pixels
MIN_PAN_SPS    = 500                             # steps/sec floor  (~0.2°/sec... ~4°/sec)
MAX_PAN_SPS    = 5000                            # steps/sec ceiling (~40°/sec)
SPEED_FACTOR   = 3.5                             # exponent of speed curve (higher = more exponential)
MAX_ERROR_JUMP = 200                             # pixels — rogue-detection threshold


class PanController:
    def __init__(self):
        self.current_pan_pos  = 0      # tracked position in steps
        self.jogging          = False
        self.last_error_x     = 0.0
        self.lost_frames      = 0
        self.max_coast_frames = 30
        self.last_speed_sps   = 0
        self.last_direction   = 0
        self.rogue_patience   = 0
        self.frame_count      = 0
        self.sync_every_n_frames = 15

        try:
            # Low timeout so position queries don't block the pipeline
            self.ser_p = serial.Serial(SERIAL_PORT_P, BAUD_RATE, timeout=0.05)
            time.sleep(0.8)   # wait for ESP32 boot message
            self.ser_p.reset_input_buffer()
            print(f"SUCCESS: Connected to Pan Motor on {SERIAL_PORT_P}")

            if ENABLE_HOMING:
                self._do_homing()
            else:
                print("Homing skipped (ENABLE_HOMING=False). Zeroing position.")
                self.ser_p.write(b"Z\n")
                self.current_pan_pos = 0

            print(f"Controller Ready. Initial position: {self.current_pan_pos} steps")

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
                self.current_pan_pos = 0
                return
            if raw.startswith("ERR"):
                print(f"Homing error: {raw}")
                return
        print("WARNING: Homing timed out.")

    # ------------------------------------------------------------------
    def _get_position(self):
        """Query step position from ESP32. Returns current_pan_pos on failure."""
        if not self.ser_p:
            return self.current_pan_pos
        try:
            self.ser_p.reset_input_buffer()
            self.ser_p.write(b"?\n")
            raw = self.ser_p.readline().decode("utf-8", errors="ignore").strip()
            if raw.startswith("P"):
                return int(raw[1:])
        except Exception as e:
            DEBUG and print(f"[PAN] Sync error: {e}")
        return self.current_pan_pos

    # ------------------------------------------------------------------
    def send_command(self, error_x, override_speed=None, speed_scale=1.0):
        """Convert pixel error to a velocity command and send to ESP32."""
        if not self.ser_p:
            return

        # --- Speed curve (identical logic to pan_control.py) ---
        ball_velocity   = abs(error_x - self.last_error_x)
        normalized_err  = min(1.0, abs(error_x) / (FRAME_W / 2))
        base_factor     = pow(normalized_err, SPEED_FACTOR)

        speed_multiplier = 1.0
        if ball_velocity > 20:
            speed_multiplier += (ball_velocity / 100.0) * 1.7

        effective_min = max(50, MIN_PAN_SPS * speed_scale)
        speed_sps = effective_min + (MAX_PAN_SPS - effective_min) * base_factor
        speed_sps = min(MAX_PAN_SPS, (override_speed or speed_sps) * speed_multiplier)
        speed_sps = int(speed_sps)
        self.last_speed_sps = speed_sps

        direction = 1 if error_x > 0 else -1

        # Soft limit check (ESP32 also enforces hard limits)
        if direction > 0 and self.current_pan_pos >= PAN_MAX_STEPS:
            return
        if direction < 0 and self.current_pan_pos <= PAN_MIN_STEPS:
            return

        vel = direction * speed_sps
        self.ser_p.write(f"V{vel}\n".encode())
        self.jogging = True

    # ------------------------------------------------------------------
    def process_detection(self, detections, speed_scale=1.0):
        self.frame_count += 1

        # Periodic position sync with hardware
        if self.frame_count % self.sync_every_n_frames == 0:
            hw_pos = self._get_position()
            drift  = abs(hw_pos - self.current_pan_pos)

            if drift > int(5 * STEPS_PER_UNIT):   # >2.5° discrepancy
                DEBUG and print(f"[CRITICAL SYNC] Drift {drift} steps. Resyncing.")
                self.ser_p.write(b"S\n")
                time.sleep(0.02)
                self.ser_p.reset_input_buffer()
                self.current_pan_pos = hw_pos
                return
            elif drift > 0:
                self.current_pan_pos = hw_pos

        ball = next((d for d in detections if d["class"] == "BALL"), None)

        if not ball:
            self.lost_frames += 1
            if self.lost_frames > self.max_coast_frames:
                self._stop_jog()
                return
            # Momentum coasting
            if self.jogging and abs(self.last_speed_sps) > 300:
                coast_speed = abs(self.last_speed_sps) * 0.8
                ghost_error = (DEADZONE_PAN + 10) * self.last_direction
                self.send_command(ghost_error, override_speed=coast_speed,
                                  speed_scale=speed_scale)
            return

        # --- Ball detected ---
        error_x = ball["center_x"] - CENTER_X

        # Rogue jump rejection
        if self.lost_frames < 1 and self.rogue_patience < 3 and self.last_error_x != 0.0:
            if abs(error_x - self.last_error_x) > MAX_ERROR_JUMP:
                self.rogue_patience += 1
                return

        self.rogue_patience = 0
        self.lost_frames    = 0

        # Hard stop on direction reversal
        if (self.last_error_x != 0.0
                and (error_x > 0) != (self.last_error_x > 0)
                and abs(error_x) > 20):
            self._stop_jog()

        if abs(error_x) <= DEADZONE_PAN:
            self._stop_jog()
        else:
            self.send_command(error_x, speed_scale=speed_scale)
            self.last_direction = 1 if error_x > 0 else -1

        self.last_error_x = error_x

    # ------------------------------------------------------------------
    def _stop_jog(self):
        if not self.ser_p or not self.jogging:
            return
        self.ser_p.write(b"S\n")
        time.sleep(0.02)
        self.ser_p.reset_input_buffer()
        self.current_pan_pos = self._get_position()
        self.jogging = False

    # ------------------------------------------------------------------
    def return_home(self):
        if not self.ser_p:
            return
        self.ser_p.write(b"S\n")    # stop any motion
        time.sleep(0.05)
        self.ser_p.write(b"G0\n")   # go to step position 0 (home/center)
        self.current_pan_pos = 0


if __name__ == "__main__":
    ctrl = PanController()
    ctrl.return_home()
