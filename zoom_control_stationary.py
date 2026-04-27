import serial
import time
import lens_helpers
import math

DEBUG = True

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
BALL_MIN_PX  = 50    # far ball  → ZOOM_MIN_STEPS (max optical zoom)
BALL_MAX_PX  = 200   # near ball → ZOOM_MAX_STEPS (1x, widest FOV)

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
STATIONARY_CENTER_X = 640.0    # half of FRAME_W
STATIONARY_CENTER_Y = 360.0    # half of FRAME_H

VELOCITY_HORIZON   = 6         # frames ahead to predict ball position
EDGE_MARGIN        = 0.20      # fraction of FOV half-width inside which we consider safe
VELOCITY_EMA_ALPHA = 0.35      # smoothing factor for ball velocity (0=frozen, 1=raw)

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
        self.last_ball_y       = None
        self.smooth_velocity   = 0.0
        self.smooth_vel_y      = 0.0
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

    def send_zoom(self, zoom_steps):
        if not self.ser_z:
            return
        self.target_zoom_pos = max(ZOOM_MIN_STEPS, min(ZOOM_MAX_STEPS,
                                                        self.target_zoom_pos + zoom_steps))
        self._drive_motor()

    def process_detection(self, detections, pan_error_x=0.0):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        if not ball or ball['width'] <= 0:
            self.last_ball_x     = None
            self.last_ball_y     = None
            self.smooth_velocity = 0.0
            self.smooth_vel_y    = 0.0
            self.target_zoom_pos = ZOOM_MAX_STEPS  # widen FOV when ball is lost
            self._drive_motor()
            return

        ball_width = ball['width']

        # Small ball (far) → t near 0 → zoom in (low zoom_pos = 8x).
        # Large ball (close) → t near 1 → zoom out (high zoom_pos = 1x).
        t = (ball_width - BALL_MIN_PX) / (BALL_MAX_PX - BALL_MIN_PX)
        t = max(0.0, min(1.0, t))
        t_curved = t ** (1.0 / ZOOM_CURVE)
        base_zoom_pos = ZOOM_MIN_STEPS + t_curved * (ZOOM_MAX_STEPS - ZOOM_MIN_STEPS)

        ball_x = ball['center_x']
        ball_y = ball['center_y']
        raw_vel_x = abs(ball_x - self.last_ball_x) if self.last_ball_x is not None else 0.0
        raw_vel_y = abs(ball_y - self.last_ball_y) if self.last_ball_y is not None else 0.0
        self.smooth_velocity = VELOCITY_EMA_ALPHA * raw_vel_x + (1.0 - VELOCITY_EMA_ALPHA) * self.smooth_velocity
        self.smooth_vel_y    = VELOCITY_EMA_ALPHA * raw_vel_y + (1.0 - VELOCITY_EMA_ALPHA) * self.smooth_vel_y
        self.last_ball_x = ball_x
        self.last_ball_y = ball_y

        def _edge_req(offset, center_half):
            """Minimum zoom_pos to keep `offset` inside the safe FOV zone."""
            predicted = offset + self.smooth_velocity * VELOCITY_HORIZON
            if predicted <= 0.0:
                return ZOOM_MIN_STEPS
            max_zr    = center_half * (1.0 - EDGE_MARGIN) / predicted
            req_t     = max(0.0, min(1.0, (max_zr - 1.0) / (MAX_OPTICAL_ZOOM - 1.0)))
            return ZOOM_MAX_STEPS - req_t * (ZOOM_MAX_STEPS - ZOOM_MIN_STEPS)

        # Horizontal: pan tracks left/right, so offset relative to where the pan is pointing.
        # pan_center_x ≈ ball_x when tracking → offset ≈ 0, only fast balls trigger zoom-out.
        pan_center_x  = STATIONARY_CENTER_X + pan_error_x
        horiz_offset  = abs(ball_x - pan_center_x)
        edge_req_h    = _edge_req(horiz_offset, STATIONARY_CENTER_X)

        # Vertical: pan cannot tilt, so offset is absolute from stationary frame centre.
        # Zoom out whenever the ball is near the top or bottom of the stationary frame.
        vert_offset   = abs(ball_y - STATIONARY_CENTER_Y)
        # Use vertical velocity for vertical prediction
        vert_predicted = vert_offset + self.smooth_vel_y * VELOCITY_HORIZON
        if vert_predicted > 0.0:
            max_zr_v  = STATIONARY_CENTER_Y * (1.0 - EDGE_MARGIN) / vert_predicted
            req_t_v   = max(0.0, min(1.0, (max_zr_v - 1.0) / (MAX_OPTICAL_ZOOM - 1.0)))
            edge_req_v = ZOOM_MAX_STEPS - req_t_v * (ZOOM_MAX_STEPS - ZOOM_MIN_STEPS)
        else:
            edge_req_v = ZOOM_MIN_STEPS

        edge_required_pos = max(edge_req_h, edge_req_v)

        # Take the more zoomed-out of ball-size target and edge requirement.
        # Higher zoom_pos = more zoomed out (1x). Lower = more zoomed in (8x).
        desired_zoom_pos = min(ZOOM_MAX_STEPS, max(base_zoom_pos, edge_required_pos))

        if DEBUG:
            print(f"[ZOOM] ball_w={ball_width:.0f}  vel_x={self.smooth_velocity:.1f}  vel_y={self.smooth_vel_y:.1f}  "
                  f"pan_err={pan_error_x:.0f}  h_off={horiz_offset:.0f}  v_off={vert_offset:.0f}  "
                  f"edge_h={edge_req_h:.0f}  edge_v={edge_req_v:.0f}  "
                  f"base={base_zoom_pos:.0f}  desired={desired_zoom_pos:.0f}")

        self.target_zoom_pos = desired_zoom_pos
        self._drive_motor()
