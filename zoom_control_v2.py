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

# --- TUNING ---
TARGET_WIDTH            = 70   # Target ball width in pixels
ZOOM_K                  = 1500  # Step multiplier: larger = faster zoom response
NORM_DEADZONE           = 0.15   # Log-ratio deadzone (~±10% of target width)
MAX_ZOOM_STEP           = 800   # Max steps per frame — keeps focus motor from falling behind
VELOCITY_ZOOM_THRESHOLD = 35    # Ball horizontal speed (px/frame) that starts triggering zoom-out
VELOCITY_ZOOM_GAIN      = 6.0   # Zoom-out steps added per px/frame above threshold
FRAME_W                 = 1280  # Camera frame width in pixels
FRAME_H                 = 720
EDGE_MARGIN             = 0.25  # Fraction of frame width from each edge that triggers zoom-out
EDGE_ZOOM_GAIN          = 4.0   # Zoom-out steps per pixel inside the edge margin
MAX_SEGMENT             = 250   # Maximum dynamic zoom step segment

# --- PRESET POSITION ---
ZOOM_BASE_POS  = 40000

# --- LIMITS ---
ZOOM_MAX_STEPS    = 41000
ZOOM_MIN_STEPS    = 30000
MAX_OPTICAL_ZOOM  = 8     # Optical zoom ratio at ZOOM_MIN_STEPS (1x at ZOOM_MAX_STEPS)
FOCUS_MAX_STEPS = 37000
FOCUS_MIN_STEPS = 32000
FOCUS_BIAS      = -1040      # Steps added to every CSV lookup — tune if table is systematically off


class ZoomController:
    def __init__(self):
        self.current_zoom_pos = 32000
        self.last_ball_x = None
        self.last_cmd_time = 0
        self.cmd_interval = 0.05
        self.target_zoom_pos = ZOOM_BASE_POS
        self.reported_zoom_pos = ZOOM_BASE_POS # What the motor actually reached

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
        """Returns 1/optical_zoom_ratio: 1.0 at 1x (40000 steps), 0.1 at 10x (30000 steps)."""
        t = (ZOOM_MAX_STEPS - self.current_zoom_pos) / (ZOOM_MAX_STEPS - ZOOM_MIN_STEPS)
        zoom_ratio = 1.0 + (MAX_OPTICAL_ZOOM - 1.0) * t
        return 1.0 / zoom_ratio

    def send_zoom(self, zoom_steps):
        if not self.ser_z: return

        # 1. Update the overall intended target (the "Virtual" position)
        self.target_zoom_pos += zoom_steps
        self.target_zoom_pos = max(ZOOM_MIN_STEPS, min(ZOOM_MAX_STEPS, self.target_zoom_pos))

        # 2. Rate limit the Serial commands
        now = time.time()
        if now - self.last_cmd_time < self.cmd_interval:
            return

        # 3. Calculate a "Micro-Step"
        diff = self.target_zoom_pos - self.current_zoom_pos
        
        if abs(diff) < 5: return

        # DYNAMIC SCALING:
        # Move 50% of the remaining distance per command, 
        # but cap it so we don't break the focus curve too badly.
        # Increase MAX_SEGMENT if your motors can handle it.
          # Increased from 80 for much higher speed
        step_to_take = diff * 0.5 
        step_to_take = max(-MAX_SEGMENT, min(MAX_SEGMENT, step_to_take))

        new_zoom_request = self.current_zoom_pos + step_to_take
        new_focus_request = self.get_focus_for_zoom(new_zoom_request)

        # 4. Send command
        lens_helpers.send_command(self.ser_z, f"G0 A{int(new_zoom_request)} B{int(new_focus_request)}")
        
        self.current_zoom_pos = new_zoom_request
        self.last_cmd_time = now

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

        # Edge-of-frame zoom-out: ball near horizontal edge → widen FOV to keep it in frame.
        # Overrides any size-based zoom-in — keeping ball in frame takes priority.
        # Calculate horizontal edge distance
        dist_h = min(ball['center_x'], FRAME_W - ball['center_x'])
        # Calculate vertical edge distance
        dist_v = min(ball['center_y'], FRAME_H - ball['center_y'])
        
        # We care about the "most dangerous" edge (the smallest distance)
        # Note: We normalize the vertical distance so the margin is proportional to frame height
        edge_margin_h = FRAME_W * EDGE_MARGIN
        edge_margin_v = FRAME_H * EDGE_MARGIN
        
        edge_bias = 0.0
        
        # Check Horizontal Edge
        if dist_h < edge_margin_h:
            edge_bias = max(edge_bias, (edge_margin_h - dist_h) * EDGE_ZOOM_GAIN)
            
        # Check Vertical Edge
        if dist_v < edge_margin_v:
            # Use same gain or a different one if vertical movement is more sensitive
            edge_bias = max(edge_bias, (edge_margin_v - dist_v) * EDGE_ZOOM_GAIN)

        if edge_bias > 0:
            # Suppress zoom-in and apply the edge bias
            zoom_step = max(0.0, zoom_step) + edge_bias
            DEBUG and print(f"[ZOOM] Edge Alert! bias=+{edge_bias:.0f}")

        # 5. Final Send
        self.send_zoom(zoom_step)