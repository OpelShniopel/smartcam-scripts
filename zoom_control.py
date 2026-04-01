import serial
import time
import lens_helpers

DEBUG = True

# --- CONFIGURATION ---
CSV_FILE       = "zoom_focus_table_updated.csv"
SERIAL_PORT_Z  = "/dev/zoom_control"
ZOOM_SPEED     = 600
FOCUS_SPEED    = 600

# --- TUNING ---
GAIN_ZOOM     = 0.2
DEADZONE_ZOOM = 15
TARGET_WIDTH  = 100      # Target ball width in pixels

# --- LIMITS ---
ZOOM_MAX_STEPS  = 40000
ZOOM_MIN_STEPS  = 30000
FOCUS_MAX_STEPS = 37000
FOCUS_MIN_STEPS = 32000


class ZoomController:
    def __init__(self):
        self.current_zoom_pos = 0

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

    def calibrate(self):
        if not self.ser_z:
            print("Calibration skipped: No serial connection.")
            return
        lens_helpers.calibrate_lens(self.ser_z)

    def get_focus_for_zoom(self, zoom_pos):
        return int(self.focus_interp(zoom_pos))

    def send_zoom(self, zoom_steps, zoom_speed=ZOOM_SPEED):
        if not self.ser_z:
            return
        new_zoom_pos = self.current_zoom_pos + zoom_steps
        if ZOOM_MIN_STEPS <= new_zoom_pos <= ZOOM_MAX_STEPS:
            self.current_zoom_pos = new_zoom_pos
            new_focus_pos = self.get_focus_for_zoom(new_zoom_pos)
            if new_focus_pos <= FOCUS_MIN_STEPS or new_focus_pos >= FOCUS_MAX_STEPS:
                new_focus_pos = 4000
            cmd = f"G1 A{int(new_zoom_pos)} B{int(new_focus_pos)} F{int(zoom_speed)}\n"
            self.ser_z.write(cmd.encode())
            DEBUG and print(f"[ZOOM] pos={int(new_zoom_pos)}  focus={int(new_focus_pos)}  speed={int(zoom_speed)}")
        else:
            DEBUG and print(f"[ZOOM] Limit! Target {int(new_zoom_pos)} out of range [{ZOOM_MIN_STEPS}, {ZOOM_MAX_STEPS}]")

    def process_detection(self, detections):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        if not ball:
            return
        zoom_error = TARGET_WIDTH - ball['width']
        DEBUG and print(f"[ZOOM] width={ball['width']:.0f}  error={zoom_error:+.0f}")
        if abs(zoom_error) > DEADZONE_ZOOM:
            self.send_zoom(zoom_error * GAIN_ZOOM)
        else:
            DEBUG and print(f"[ZOOM] In deadzone")
