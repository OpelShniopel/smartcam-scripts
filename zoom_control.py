import serial
import time
import lens_helpers
import math

DEBUG = False

# --- CONFIGURATION ---
CSV_FILE       = "zoom_focus_table_updated.csv"
SERIAL_PORT_Z  = "/dev/zoom_control"
ZOOM_SPEED     = 1000
FOCUS_SPEED    = 1000

# --- TUNING ---
TARGET_WIDTH  = 100    # Target ball width in pixels
ZOOM_K        = 1000   # Step multiplier: larger = faster zoom response
NORM_DEADZONE = 0.1    # Log-ratio deadzone (~±10% of target width)

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
            # Using G0 for both axes simultaneously
            lens_helpers.send_command(self.ser_z, f"G0 A{ZOOM_BASE_POS} B{FOCUS_BASE_POS}")
            lens_helpers.wait_homing(self.ser_z, 1, lens_helpers.CHA_MOVE)
            lens_helpers.wait_homing(self.ser_z, 1, lens_helpers.CHB_MOVE)
            
            self.current_zoom_pos = ZOOM_BASE_POS
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
        
        # Clamp to physical limits
        new_zoom_pos = max(ZOOM_MIN_STEPS, min(ZOOM_MAX_STEPS, new_zoom_pos))
        
        # Ignore micro-jitters to prevent flooding the serial port
        if abs(new_zoom_pos - self.current_zoom_pos) < 5:
            return

        self.current_zoom_pos = new_zoom_pos
        new_focus_pos = self.get_focus_for_zoom(new_zoom_pos)
        
        # Clamp focus to valid range
        new_focus_pos = max(FOCUS_MIN_STEPS, min(FOCUS_MAX_STEPS, new_focus_pos))

        # Send BOTH axes via G0 every time. 
        # The firmware will start both motors at the exact same time.
        cmd = f"G0 A{int(new_zoom_pos)} B{int(new_focus_pos)}"
        lens_helpers.send_command(self.ser_z, cmd)
        
        DEBUG and print(f"[ZOOM] Sync Move -> A:{int(new_zoom_pos)} B:{int(new_focus_pos)}")

    def process_detection(self, detections):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        if not ball or ball['width'] <= 0:
            return

        ratio = ball['width'] / TARGET_WIDTH
        norm_error = math.log(ratio)

        if abs(norm_error) < NORM_DEADZONE:
            return

        zoom_step_command = norm_error * ZOOM_K
        self.send_zoom(zoom_step_command)