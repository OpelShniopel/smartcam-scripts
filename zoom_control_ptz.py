import serial
import time
import lens_helpers
import math

DEBUG = False

# --- CONFIGURATION ---
CSV_FILE       = "zoom_focus_table.csv"
SERIAL_PORT_Z  = "/dev/zoom_control"
ZOOM_SPEED     = 1000
FOCUS_SPEED    = 1200

# --- LIMITS (Synchronized with fixed) ---
ZOOM_BASE_POS  = 34700
ZOOM_RTH_POS   = 34700
ZOOM_MAX_STEPS = 34700   # 1x (widest FOV)
ZOOM_MIN_STEPS = 29500   # max optical zoom (~3x)
MAX_OPTICAL_ZOOM = 3

FOCUS_MAX_STEPS = 33300
FOCUS_MIN_STEPS = 25340

# --- TUNING ---
TARGET_WIDTH            = 100   # Target ball width in pixels
ZOOM_K                  = 1500  # Step multiplier: larger = faster zoom response
NORM_DEADZONE           = 0.15   # Log-ratio deadzone (~±10% of target width)
MAX_ZOOM_STEP           = 800   # Max steps per frame — keeps focus motor from falling behind
VELOCITY_ZOOM_THRESHOLD = 35    # Ball horizontal speed (px/frame) that starts triggering zoom-out
VELOCITY_ZOOM_GAIN      = 6.0   # Zoom-out steps added per px/frame above threshold
FRAME_W                 = 1280  # Camera frame width in pixels
FRAME_H                 = 720
EDGE_MARGIN             = 0.25  # Fraction of frame width from each edge that triggers zoom-out
EDGE_ZOOM_GAIN          = 4.0   # Zoom-out steps per pixel inside the edge margin
MAX_SEGMENT             = 50    # Reduced to 50 to match fixed version's stability

def open_serial_with_retry(port_path, baud, retries=5, delay=0.5):
    last_exc = None
    for attempt in range(retries):
        try:
            s = serial.Serial(port_path, baud, timeout=1)
            time.sleep(0.1)
            s.reset_input_buffer()
            return s
        except serial.SerialException as e:
            last_exc = e
            print(f"Port {port_path} not ready (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay * (attempt + 1))
    raise last_exc

class ZoomController:
    def __init__(self):
        self.current_zoom_pos  = ZOOM_BASE_POS
        self.target_zoom_pos   = ZOOM_BASE_POS
        self.focus_bias        = 0
        self.last_ball_x       = None
        self.last_cmd_time     = 0
        self.cmd_interval      = 0.05

        try:
            self.ser_z = open_serial_with_retry(SERIAL_PORT_Z, 115200)
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
            focus_base = self.get_focus_for_zoom(ZOOM_BASE_POS)
            lens_helpers.send_command(self.ser_z, f"G0 A{ZOOM_BASE_POS}")
            lens_helpers.wait_homing(self.ser_z, 1, lens_helpers.CHA_MOVE)
            lens_helpers.send_command(self.ser_z, f"G0 B{focus_base}")
            lens_helpers.wait_homing(self.ser_z, 1, lens_helpers.CHB_MOVE)
            self.current_zoom_pos = ZOOM_BASE_POS
            print(f"Base position reached: zoom={ZOOM_BASE_POS}, focus={focus_base}")

    def calibrate(self):
        if not self.ser_z:
            print("Calibration skipped: No serial connection.")
            return
        lens_helpers.calibrate_lens(self.ser_z, ZOOM_SPEED, FOCUS_SPEED)

    def get_focus_for_zoom(self, zoom_pos):
        return int(self.focus_interp(zoom_pos)) + self.focus_bias

    def get_pan_speed_factor(self):
        """Returns 1/optical_zoom_ratio: 1.0 at 1x, lower when zoomed in."""
        t = (ZOOM_MAX_STEPS - self.current_zoom_pos) / (ZOOM_MAX_STEPS - ZOOM_MIN_STEPS)
        zoom_ratio = 1.0 + (MAX_OPTICAL_ZOOM - 1.0) * t
        return 1.0 / zoom_ratio

    def _drive_motor(self):
        """Send one incremental step toward target_zoom_pos (rate-limited)."""
        if not self.ser_z:
            return
        now = time.time()
        if now - self.last_cmd_time < self.cmd_interval:
            return
        diff = self.target_zoom_pos - self.current_zoom_pos
        if abs(diff) < 5:
            return
        step = max(-MAX_SEGMENT, min(MAX_SEGMENT, diff * 0.5))
        new_pos = self.current_zoom_pos + step
        new_focus = self.get_focus_for_zoom(new_pos)
        lens_helpers.send_command(self.ser_z, f"G0 A{int(new_pos)} B{int(new_focus)}")
        self.current_zoom_pos = new_pos
        self.last_cmd_time = now

    def return_home(self):
        if not self.ser_z:
            return
        home_focus = self.get_focus_for_zoom(ZOOM_RTH_POS)
        print(f"Zoom returning home: pos={ZOOM_RTH_POS}, focus={home_focus}")
        lens_helpers.send_command(self.ser_z, f"G0 A{ZOOM_RTH_POS} B{home_focus}")
        lens_helpers.wait_homing(self.ser_z, 1, lens_helpers.CHA_MOVE)
        lens_helpers.wait_homing(self.ser_z, 1, lens_helpers.CHB_MOVE)
        self.current_zoom_pos = ZOOM_RTH_POS
        self.target_zoom_pos  = ZOOM_RTH_POS
        print("Zoom home reached.")

    def apply_focus_bias(self, delta):
        self.focus_bias += delta
        if not self.ser_z:
            return
        new_focus = self.get_focus_for_zoom(self.current_zoom_pos)
        lens_helpers.send_command(self.ser_z, f"G0 B{int(new_focus)}")
        self.last_cmd_time = time.time()

    def send_zoom(self, zoom_steps):
        if not self.ser_z:
            return
        self.target_zoom_pos = max(ZOOM_MIN_STEPS, min(ZOOM_MAX_STEPS,
                                                        self.target_zoom_pos + zoom_steps))
        self._drive_motor()

    def process_detection(self, detections):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        if not ball or ball['width'] <= 0:
            self.last_ball_x = None
            self.target_zoom_pos = ZOOM_MAX_STEPS  # widen FOV when ball is lost
            self._drive_motor()
            return

        # Horizontal velocity (px/frame) — reset on lost frames so first reacquire doesn't spike
        ball_velocity_x = abs(ball['center_x'] - self.last_ball_x) if self.last_ball_x is not None else 0.0
        self.last_ball_x = ball['center_x']

        # Size-based zoom: proportional log error, suppressed inside deadzone
        ratio = ball['width'] / TARGET_WIDTH
        norm_error = math.log(ratio)
        zoom_step = 0.0
        if abs(norm_error) >= NORM_DEADZONE:
            zoom_step = max(-MAX_ZOOM_STEP, min(MAX_ZOOM_STEP, norm_error * ZOOM_K))

        # Velocity-based zoom-out: fast horizontal movement → widen FOV
        if ball_velocity_x > VELOCITY_ZOOM_THRESHOLD:
            excess = ball_velocity_x - VELOCITY_ZOOM_THRESHOLD
            velocity_bias = min(MAX_ZOOM_STEP, excess * VELOCITY_ZOOM_GAIN)
            zoom_step += velocity_bias
            DEBUG and print(f"[ZOOM] velocity={ball_velocity_x:.0f}px/f  bias=+{velocity_bias:.0f}")

        # Edge-of-frame zoom-out: ball near horizontal edge → widen FOV to keep it in frame.
        # Overrides any size-based zoom-in — keeping ball in frame takes priority.
        dist_h = min(ball['center_x'], FRAME_W - ball['center_x'])
        dist_v = min(ball['center_y'], FRAME_H - ball['center_y'])
        
        edge_margin_h = FRAME_W * EDGE_MARGIN
        edge_margin_v = FRAME_H * EDGE_MARGIN
        
        edge_bias = 0.0
        
        if dist_h < edge_margin_h:
            edge_bias = max(edge_bias, (edge_margin_h - dist_h) * EDGE_ZOOM_GAIN)
            
        if dist_v < edge_margin_v:
            edge_bias = max(edge_bias, (edge_margin_v - dist_v) * EDGE_ZOOM_GAIN)

        if edge_bias > 0:
            zoom_step = max(0.0, zoom_step) + edge_bias
            DEBUG and print(f"[ZOOM] Edge Alert! bias=+{edge_bias:.0f}")

        self.send_zoom(zoom_step)
