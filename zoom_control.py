import serial
import time
import lens_helpers
import math

DEBUG = False

# --- CONFIGURATION ---
CSV_FILE       = "zoom_focus_table_updated.csv"
SERIAL_PORT_Z  = "/dev/zoom_control"
ZOOM_SPEED     = 1000
FOCUS_SPEED    = 2000

# --- TUNING ---
TARGET_WIDTH            = 100   # Target ball width in pixels
ZOOM_K                  = 2000  # Step multiplier: larger = faster zoom response
NORM_DEADZONE           = 0.1   # Log-ratio deadzone (~±10% of target width)
MAX_ZOOM_STEP           = 200   # Max steps per frame — keeps focus motor from falling behind
FOCUS_UPDATE_STEPS      = 10    # Send focus correction only when zoom drifts this many steps
VELOCITY_ZOOM_THRESHOLD = 60    # Ball horizontal speed (px/frame) that starts triggering zoom-out
VELOCITY_ZOOM_GAIN      = 5.0   # Zoom-out steps added per px/frame above threshold
FRAME_W                 = 1280  # Camera frame width in pixels
EDGE_MARGIN             = 0.25  # Fraction of frame width from each edge that triggers zoom-out
EDGE_ZOOM_GAIN          = 3.0   # Zoom-out steps per pixel inside the edge margin

# --- PRESET POSITION ---
ZOOM_BASE_POS  = 34000
FOCUS_BASE_POS = 34520

# --- LIMITS ---
ZOOM_MAX_STEPS    = 41800
ZOOM_MIN_STEPS    = 30000
MAX_OPTICAL_ZOOM  = 10     # Optical zoom ratio at ZOOM_MIN_STEPS (1x at ZOOM_MAX_STEPS)
FOCUS_MAX_STEPS = 37000
FOCUS_MIN_STEPS = 32000


class ZoomController:
    def __init__(self):
        self.current_zoom_pos = 32000  # set by G92 A32000 at end of calibration
        self.last_focus_update_pos = 32000
        self.last_ball_x = None

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
            focus_base = self.get_focus_for_zoom(ZOOM_BASE_POS)
            lens_helpers.send_command(self.ser_z, f"G0 A{ZOOM_BASE_POS}")
            lens_helpers.wait_homing(self.ser_z, 1, lens_helpers.CHA_MOVE)
            lens_helpers.send_command(self.ser_z, f"G0 B{focus_base}")
            lens_helpers.wait_homing(self.ser_z, 1, lens_helpers.CHB_MOVE)
            self.current_zoom_pos = ZOOM_BASE_POS
            self.last_focus_update_pos = ZOOM_BASE_POS
            print(f"Base position reached: zoom={ZOOM_BASE_POS}, focus={focus_base}")

    def calibrate(self):
        if not self.ser_z:
            print("Calibration skipped: No serial connection.")
            return
        lens_helpers.calibrate_lens(self.ser_z)

    def get_focus_for_zoom(self, zoom_pos):
        return int(self.focus_interp(zoom_pos))

    def get_pan_speed_factor(self):
        """Returns 1/optical_zoom_ratio: 1.0 at 1x (40000 steps), 0.1 at 10x (30000 steps)."""
        t = (ZOOM_MAX_STEPS - self.current_zoom_pos) / (ZOOM_MAX_STEPS - ZOOM_MIN_STEPS)
        zoom_ratio = 1.0 + (MAX_OPTICAL_ZOOM - 1.0) * t
        return 1.0 / zoom_ratio

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
        new_focus_pos = max(FOCUS_MIN_STEPS, min(FOCUS_MAX_STEPS, new_focus_pos))

        focus_due = abs(new_zoom_pos - self.last_focus_update_pos) >= FOCUS_UPDATE_STEPS
        if focus_due:
            lens_helpers.send_command(self.ser_z, f"G0 A{int(new_zoom_pos)} B{int(new_focus_pos)}")
            self.last_focus_update_pos = new_zoom_pos
            DEBUG and print(f"[ZOOM] A={int(new_zoom_pos)} B={int(new_focus_pos)} (focus updated)")
        else:
            lens_helpers.send_command(self.ser_z, f"G0 A{int(new_zoom_pos)}")
            DEBUG and print(f"[ZOOM] A={int(new_zoom_pos)} (focus hold)")

    def process_detection(self, detections):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        if not ball or ball['width'] <= 0:
            self.last_ball_x = None
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

        # Edge-of-frame zoom-out: ball near horizontal edge → widen FOV to keep it in frame
        edge_margin_px = FRAME_W * EDGE_MARGIN
        edge_distance = min(ball['center_x'], FRAME_W - ball['center_x'])
        if edge_distance < edge_margin_px:
            edge_bias = min(MAX_ZOOM_STEP, (edge_margin_px - edge_distance) * EDGE_ZOOM_GAIN)
            zoom_step += edge_bias
            DEBUG and print(f"[ZOOM] edge_dist={edge_distance:.0f}px  bias=+{edge_bias:.0f}")

        if zoom_step != 0.0:
            self.send_zoom(zoom_step)