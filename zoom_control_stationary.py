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

# --- BALL SIZE → ZOOM MAPPING ---
# Ball width in stationary-cam pixels at the extremes of the court.
# BALL_MIN_PX: ball appears this small when at max distance → zoom in fully.
# BALL_MAX_PX: ball appears this large when close → no zoom (1x).
# Measure these by watching the stationary cam feed with the ball at each extreme.
BALL_MIN_PX  = 20    # far ball  → ZOOM_MIN_STEPS (max optical zoom)
BALL_MAX_PX  = 120   # near ball → ZOOM_MAX_STEPS (1x, widest FOV)

# Shape of the zoom curve. 1.0 = linear. >1 = more aggressive zoom for distant balls.
ZOOM_CURVE   = 1.5

# --- ZOOM-OUT BIAS ---
# Combines position and velocity into one zoom-aware signal.
#
# pan_fov_half = STATIONARY_FRAME_HALF / zoom_ratio
#   = how many stationary-cam pixels the pan cam can see on each side of its centre.
#   At 1x zoom: 640 px.  At 8x zoom: 80 px.
#
# We predict where the ball will be in VELOCITY_HORIZON frames and check whether
# that lands outside (1 - EDGE_MARGIN) of the pan cam's FOV half-width.
# At high zoom this triggers for much smaller offsets from centre, which is correct.
#
# Assumes the pan camera is pointing at the stationary cam centre (step 0).
# This over-triggers when the pan has already rotated, but that only causes
# extra zoom-out which is the safe failure mode.
STATIONARY_CENTER_X = 640.0    # half of FRAME_W — stationary cam optical centre

VELOCITY_HORIZON = 6           # frames ahead to predict ball position
EDGE_MARGIN      = 0.20        # fraction of pan FOV half-width — zoom-out starts here
EDGE_ZOOM_GAIN   = 15.0        # zoom-out steps per stationary-px of overshoot
MAX_EDGE_BIAS    = 5000        # hard cap (~45% of full zoom range)

FRAME_W = 1280   # stationary cam frame width (px)
FRAME_H = 720

# --- LIMITS ---
ZOOM_BASE_POS  = 40000
ZOOM_MAX_STEPS = 41000   # 1x (widest FOV)
ZOOM_MIN_STEPS = 30000   # max optical zoom (~8x)
MAX_OPTICAL_ZOOM = 8

FOCUS_MAX_STEPS = 37000
FOCUS_MIN_STEPS = 32000
FOCUS_BIAS      = -1040

MAX_SEGMENT = 250   # max zoom step per serial command


class ZoomController:
    def __init__(self):
        self.current_zoom_pos  = ZOOM_BASE_POS
        self.target_zoom_pos   = ZOOM_BASE_POS
        self.last_ball_x       = None
        self.last_cmd_time     = 0
        self.cmd_interval      = 0.05

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
            print(f"Base position reached: zoom={ZOOM_BASE_POS}, focus={focus_base}")

    def calibrate(self):
        if not self.ser_z:
            print("Calibration skipped: No serial connection.")
            return
        lens_helpers.calibrate_lens(self.ser_z, ZOOM_SPEED, FOCUS_SPEED)

    def get_focus_for_zoom(self, zoom_pos):
        return int(self.focus_interp(zoom_pos)) + FOCUS_BIAS

    def get_pan_speed_factor(self):
        """Returns 1/optical_zoom_ratio: 1.0 at 1x, lower when zoomed in."""
        t = (ZOOM_MAX_STEPS - self.current_zoom_pos) / (ZOOM_MAX_STEPS - ZOOM_MIN_STEPS)
        zoom_ratio = 1.0 + (MAX_OPTICAL_ZOOM - 1.0) * t
        return 1.0 / zoom_ratio

    def send_zoom(self, zoom_steps):
        if not self.ser_z:
            return

        self.target_zoom_pos += zoom_steps
        self.target_zoom_pos = max(ZOOM_MIN_STEPS, min(ZOOM_MAX_STEPS, self.target_zoom_pos))

        now = time.time()
        if now - self.last_cmd_time < self.cmd_interval:
            return

        diff = self.target_zoom_pos - self.current_zoom_pos
        if abs(diff) < 5:
            return

        step_to_take = max(-MAX_SEGMENT, min(MAX_SEGMENT, diff * 0.5))
        new_zoom_request = self.current_zoom_pos + step_to_take
        new_focus_request = self.get_focus_for_zoom(new_zoom_request)

        lens_helpers.send_command(self.ser_z, f"G0 A{int(new_zoom_request)} B{int(new_focus_request)}")
        self.current_zoom_pos = new_zoom_request
        self.last_cmd_time = now

    def process_detection(self, detections):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        if not ball or ball['width'] <= 0:
            self.last_ball_x = None
            return

        ball_width = ball['width']

        # Direct size → zoom position mapping.
        # Small ball (far) → t near 0 → zoom in (low zoom_pos).
        # Large ball (close) → t near 1 → zoom out (high zoom_pos / 1x).
        t = (ball_width - BALL_MIN_PX) / (BALL_MAX_PX - BALL_MIN_PX)
        t = max(0.0, min(1.0, t))
        t_curved = t ** (1.0 / ZOOM_CURVE)   # invert curve: more zoom for distant balls
        base_zoom_pos = ZOOM_MIN_STEPS + t_curved * (ZOOM_MAX_STEPS - ZOOM_MIN_STEPS)

        ball_x          = ball['center_x']
        ball_velocity_x = abs(ball_x - self.last_ball_x) if self.last_ball_x is not None else 0.0
        self.last_ball_x = ball_x

        # Zoom-out bias — zoom-aware, combines position and velocity.
        #
        # pan_fov_half: how many stationary-cam pixels the pan cam sees on each side.
        #   1x zoom → 640 px,  8x zoom → 80 px.
        #
        # Predict where the ball will be in VELOCITY_HORIZON frames and measure
        # how far that lands outside the safe fraction of the pan cam's FOV.
        # At high zoom the safe zone shrinks, so even small offsets trigger zoom-out.
        zoom_ratio   = 1.0 / self.get_pan_speed_factor()
        pan_fov_half = (FRAME_W / 2.0) / zoom_ratio

        offset_now       = abs(ball_x - STATIONARY_CENTER_X)
        predicted_offset = offset_now + ball_velocity_x * VELOCITY_HORIZON
        safe_zone        = pan_fov_half * (1.0 - EDGE_MARGIN)

        edge_bias = 0.0
        if predicted_offset > safe_zone:
            overshoot = predicted_offset - safe_zone
            edge_bias = min(MAX_EDGE_BIAS, overshoot * EDGE_ZOOM_GAIN)

        desired_zoom_pos = min(ZOOM_MAX_STEPS, base_zoom_pos + edge_bias)

        if DEBUG:
            print(f"[ZOOM] ball_w={ball_width:.0f}  vel={ball_velocity_x:.1f}  "
                  f"offset={offset_now:.0f}  pred={predicted_offset:.0f}  "
                  f"safe={safe_zone:.0f}  edge_bias={edge_bias:.0f}  "
                  f"base={base_zoom_pos:.0f}  desired={desired_zoom_pos:.0f}")

        # Send delta from current target so send_zoom's rate limiter works normally
        zoom_delta = desired_zoom_pos - self.target_zoom_pos
        self.send_zoom(zoom_delta)
