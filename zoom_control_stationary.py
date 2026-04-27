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

# --- BIAS CONSTANTS ---
# Velocity: fast horizontal movement in stationary cam → zoom out to keep ball in pan frame.
VELOCITY_ZOOM_THRESHOLD = 50    # px/frame in stationary cam space
VELOCITY_ZOOM_GAIN      = 5.0   # zoom-out steps added per px/frame above threshold
MAX_VELOCITY_BIAS       = 600   # cap on velocity zoom-out steps

# Edge: ball near edge of pan camera's frame → zoom out.
# Computed in pan-cam coordinates using an EMA estimate of where the pan camera is pointing.
# PAN_TRACKING_ALPHA: EMA smoothing for pan-pointing estimate (lower = more lag = more conservative).
# Represents roughly how fast the pan camera catches up — 0.15 ≈ ~6 frame lag.
EDGE_MARGIN          = 0.20     # fraction of pan cam half-frame that triggers zoom-out
EDGE_ZOOM_GAIN       = 800.0    # zoom-out steps per unit of normalised pan error above margin
MAX_EDGE_BIAS        = 600
PAN_TRACKING_ALPHA   = 0.15     # EMA alpha for pan-pointing estimate

# Stationary cam half-width used to normalise pan error (must match pan_control config)
STATIONARY_FRAME_HALF = 640.0

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
        self.pan_pointing_px   = None   # EMA estimate of pan cam pointing in stationary px
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
            self.last_ball_x     = None
            self.pan_pointing_px = None   # reset so first reacquire doesn't use stale estimate
            return

        ball_width = ball['width']

        # Direct size → zoom position mapping.
        # Small ball (far) → t near 0 → zoom in (low zoom_pos).
        # Large ball (close) → t near 1 → zoom out (high zoom_pos / 1x).
        t = (ball_width - BALL_MIN_PX) / (BALL_MAX_PX - BALL_MIN_PX)
        t = max(0.0, min(1.0, t))
        t_curved = t ** (1.0 / ZOOM_CURVE)   # invert curve: more zoom for distant balls
        base_zoom_pos = ZOOM_MIN_STEPS + t_curved * (ZOOM_MAX_STEPS - ZOOM_MIN_STEPS)

        ball_x = ball['center_x']

        # Velocity bias: fast horizontal movement → zoom out to keep ball in pan cam frame
        ball_velocity_x = abs(ball_x - self.last_ball_x) if self.last_ball_x is not None else 0.0
        self.last_ball_x = ball_x

        velocity_bias = 0.0
        if ball_velocity_x > VELOCITY_ZOOM_THRESHOLD:
            excess = ball_velocity_x - VELOCITY_ZOOM_THRESHOLD
            velocity_bias = min(MAX_VELOCITY_BIAS, excess * VELOCITY_ZOOM_GAIN)
            DEBUG and print(f"[ZOOM] velocity={ball_velocity_x:.0f}px/f  bias=+{velocity_bias:.0f}")

        # Edge bias in pan-camera coordinates.
        #
        # We don't have pan cam detections, so we estimate where the pan camera is
        # currently pointing using an EMA of the ball's stationary cam position.
        # A well-tuned pan controller converges to the ball position over several frames,
        # so the EMA (with appropriate lag) approximates the pan camera's current centre.
        #
        # ball_x - pan_pointing_px = ball's offset from pan centre, in stationary cam pixels.
        # Dividing by (STATIONARY_FRAME_HALF / zoom_ratio) normalises to pan cam coordinates
        # (-1..+1), because at zoom_ratio×, only 1/zoom_ratio of the stationary frame is visible.
        if self.pan_pointing_px is None:
            self.pan_pointing_px = ball_x
        else:
            self.pan_pointing_px = (PAN_TRACKING_ALPHA * ball_x
                                    + (1.0 - PAN_TRACKING_ALPHA) * self.pan_pointing_px)

        zoom_ratio     = 1.0 / self.get_pan_speed_factor()   # 1x at wide, 8x at max zoom
        pan_fov_half   = STATIONARY_FRAME_HALF / zoom_ratio  # stationary px visible either side
        ball_norm      = (ball_x - self.pan_pointing_px) / pan_fov_half if pan_fov_half > 0 else 0.0

        edge_bias = 0.0
        excess_norm = abs(ball_norm) - (1.0 - EDGE_MARGIN)
        if excess_norm > 0:
            edge_bias = min(MAX_EDGE_BIAS, excess_norm * EDGE_ZOOM_GAIN)
            DEBUG and print(f"[ZOOM] pan_norm={ball_norm:.2f}  edge_bias=+{edge_bias:.0f}")

        # Biases push toward wider FOV (zoom out = higher position value)
        desired_zoom_pos = min(ZOOM_MAX_STEPS, base_zoom_pos + velocity_bias + edge_bias)

        if DEBUG:
            print(f"[ZOOM] ball_w={ball_width:.0f}px  t={t:.2f}  base={base_zoom_pos:.0f}  desired={desired_zoom_pos:.0f}  current={self.current_zoom_pos:.0f}")

        # Send delta from current target so send_zoom's rate limiter works normally
        zoom_delta = desired_zoom_pos - self.target_zoom_pos
        self.send_zoom(zoom_delta)
