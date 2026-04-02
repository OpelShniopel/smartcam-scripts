import serial
import time
import lens_helpers
import math

DEBUG = False

# --- CONFIGURATION ---
CSV_FILE       = "zoom_focus_table_updated.csv"
SERIAL_PORT_Z  = "/dev/zoom_control"
ZOOM_SPEED     = 1200
FOCUS_SPEED    = 1200

# --- TUNING ---
DEADZONE_ZOOM      = 50       # pixels — no action within this band
FINE_ZONE_ZOOM     = 150      # pixels — proportional steps within this band
ZOOM_STEP          = 350      # fixed step size outside the fine zone
GAIN_ZOOM          = 1.5      # proportional gain inside fine zone (pixels → steps)
TARGET_WIDTH       = 100      # Target ball width in pixels
FOCUS_UPDATE_STEPS = 50       # Update focus axis only when zoom moves this many steps

# --- PRESET POSITION ---
ZOOM_BASE_POS  = 34000
FOCUS_BASE_POS = 34520

# --- LIMITS ---
ZOOM_MAX_STEPS  = 40000
ZOOM_MIN_STEPS  = 30000
FOCUS_MAX_STEPS = 37000
FOCUS_MIN_STEPS = 32000


class ZoomController:
    def __init__(self):
        self.current_zoom_pos = 32000  # set by G92 A32000 at end of calibration
        self.last_focus_update_pos = 32000  # zoom position at last focus update

        try:
            self.ser_z = serial.Serial(SERIAL_PORT_Z, 115200, timeout=1)
            time.sleep(1.5)
            print("Initializing Kurokesu Lens Board...")
            lens_helpers.init_lens_board(self.ser_z, ZOOM_SPEED, FOCUS_SPEED)
            print(f"SUCCESS: Zoom ({ZOOM_SPEED}) and Focus ({FOCUS_SPEED}) initialized.")
        except Exception as e:
            print(f"WARNING: Zoom/Focus Serial port not found. ({e})")
            self.ser_z = None

        self.focus_interp = lens_helpers.load_focus_interpolator(CSV_FILE)

        if self.ser_z and not lens_helpers.verify_command(self.ser_z, "G90"):
            print("CRITICAL: Kurokesu board failed response check.")

        self.calibrate()
        if self.ser_z:
            print("Moving to base position...")
            lens_helpers.send_command(self.ser_z, f"G0 A{ZOOM_BASE_POS}")
            lens_helpers.wait_homing(self.ser_z, 1, lens_helpers.CHA_MOVE)
            lens_helpers.send_command(self.ser_z, f"G0 B{FOCUS_BASE_POS}")
            lens_helpers.wait_homing(self.ser_z, 1, lens_helpers.CHB_MOVE)
            self.current_zoom_pos = ZOOM_BASE_POS
            self.last_focus_update_pos = ZOOM_BASE_POS
            print(f"Base position reached: zoom={ZOOM_BASE_POS}, focus={FOCUS_BASE_POS}")

    def calibrate(self):
        if not self.ser_z:
            print("Calibration skipped: No serial connection.")
            return
        lens_helpers.calibrate_lens(self.ser_z)

    def get_focus_for_zoom(self, zoom_pos):
        return int(self.focus_interp(zoom_pos))

    def send_zoom(self, zoom_steps):
        if not self.ser_z:
            return
        new_zoom_pos = self.current_zoom_pos + zoom_steps
        if ZOOM_MIN_STEPS <= new_zoom_pos <= ZOOM_MAX_STEPS:
            self.current_zoom_pos = new_zoom_pos
            new_focus_pos = self.get_focus_for_zoom(new_zoom_pos)
            if new_focus_pos <= FOCUS_MIN_STEPS or new_focus_pos >= FOCUS_MAX_STEPS:
                new_focus_pos = 4000

            focus_due = abs(new_zoom_pos - self.last_focus_update_pos) >= FOCUS_UPDATE_STEPS
            if focus_due:
                # Update both axes together only when focus needs a meaningful correction
                lens_helpers.send_command(self.ser_z, f"G0 A{int(new_zoom_pos)} B{int(new_focus_pos)}")
                self.last_focus_update_pos = new_zoom_pos
                DEBUG and print(f"[ZOOM] pos={int(new_zoom_pos)}  focus={int(new_focus_pos)}  (focus updated)")
            else:
                # Zoom axis only — keeps A moving at full speed without B as a bottleneck
                lens_helpers.send_command(self.ser_z, f"G0 A{int(new_zoom_pos)}")
                DEBUG and print(f"[ZOOM] pos={int(new_zoom_pos)}  focus=hold")
        else:
            DEBUG and print(f"[ZOOM] Limit! Target {int(new_zoom_pos)} out of range [{ZOOM_MIN_STEPS}, {ZOOM_MAX_STEPS}]")

    def process_detection(self, detections):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        if not ball or ball['width'] <= 0:
            return

        # 1. Calculate the Ratio (How much bigger/smaller is it than target?)
        # Example: 150px / 100px = 1.5
        ratio = ball['width'] / TARGET_WIDTH

        # 2. Use Log to get a symmetrical error signal
        # log(1.0) = 0 (On target)
        # log(2.0) = 0.69 (Too big)
        # log(0.5) = -0.69 (Too small)
        norm_error = math.log(ratio)

        # 3. Apply Deadzone to the normalized error
        # 0.1 means "ignore if within 10% of target size"
        NORM_DEADZONE = 0.1 
        if abs(norm_error) < NORM_DEADZONE:
            return

        # 4. Global Gain
        # Higher value = faster zoom. Try 5000 to 10000.
        ZOOM_K = 1000
        
        zoom_step_command = norm_error * ZOOM_K
        self.send_zoom(zoom_step_command)