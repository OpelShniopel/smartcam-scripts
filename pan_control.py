import serial
import time
import pan_homing

DEBUG = True

# --- CONFIGURATION ---
SERIAL_PORT_P = "/dev/pan_control"
BAUD_RATE = 115200

# Frame dimensions (Ensure this matches your inference output)
FRAME_W = 1280 
FRAME_H = 720
CENTER_X = FRAME_W / 2

# --- PHYSICAL CONSTANTS ---
# 1 unit = 0.5°  |  G0 X180 = 90° right
DEGREES_PER_UNIT = 0.5

# --- CONTROL TUNING ---
DEADZONE_PAN  = 50                              # pixels
MIN_PAN_SPEED = 1000                            # units/min  — ~8.3°/sec
MAX_PAN_SPEED = 5000                            # units/min  — ~41.7°/sec
SPEED_GAIN    = MAX_PAN_SPEED / (FRAME_W / 2)  # ramps linearly from 0 to MAX across half-frame
                                                # = 7.8 units/min per pixel
COMMAND_DT    = 0.04                            # seconds per jog segment (= 1 frame at 25fps)
                                                # at MAX_PAN_SPEED: 6.7 units (3.3°) per step
SPEED_FACTOR = 3.5                              # power of speed curve - lower for linear, higher for more exponential
MAX_ERROR_JUMP = 200                            # pixels — reject rogue detections that jump more than this per frame


# Control limits (1 unit = 0.5°)
PAN_MAX_STEPS =  180    # +90°  right
PAN_MIN_STEPS =  -70    # -35°  left

class PanController:
    def __init__(self):
        self.current_pan_pos = 0.0
        self.jogging = False
        self.last_error_x = 0.0
        self.lost_frames = 0
        self.max_coast_frames = 30 
        self.last_speed = 0
        self.last_direction = 0
        self.rogue_patience = 0
        
        # --- NEW: Sync Logic ---
        self.frame_count = 0
        self.sync_every_n_frames = 20 # Sync with hardware every 0.5s @ 40fps

        try:
            self.ser_p = serial.Serial(SERIAL_PORT_P, BAUD_RATE, timeout=0.02) # Low timeout for speed
            print(f"SUCCESS: Connected to Pan Motor on {SERIAL_PORT_P}")
            pan_homing.auto_home_precision(self.ser_p)
            # Anchor the start position
            self.current_pan_pos = self._get_position()
        except Exception as e:
            print(f"WARNING: Pan Motor Serial port not found. ({e})")
            self.ser_p = None

    def _get_position(self):
        """Query actual motor X position from GRBL status report."""
        if not self.ser_p: return self.current_pan_pos
        try:
            self.ser_p.write(b"?") 
            # Give GRBL a tiny moment to process the real-time command
            time.sleep(0.01) 
            line = self.ser_p.readline().decode('utf-8')
            if "MPos:" in line:
                # Example line: <Idle|MPos:10.000,0.000,0.000|Bf:15,128>
                pos_str = line.split("MPos:")[1].split(",")[0]
                return float(pos_str)
        except Exception as e:
            DEBUG and print(f"[PAN] Sync error: {e}")
        return self.current_pan_pos

    def _stop_jog(self):
        """Cancel any pending jog and sync position from the board."""
        if not self.jogging:
            return
        self.ser_p.write(b"\x85")
        time.sleep(0.05)                            # wait for GRBL to flush and return to Idle
        self.ser_p.reset_input_buffer()             # discard any leftover 'ok' responses
        self.current_pan_pos = self._get_position()
        self.jogging = False
        DEBUG and print(f"[PAN] Stopped at X={self.current_pan_pos:.1f}")

    def send_command(self, error_x, override_speed=None, speed_scale=1.0):
        if not self.ser_p:
            return
        
        # 1. Calculate Ball Velocity (pixels per frame)
        # Positive if moving right, negative if moving left
        ball_velocity = abs(error_x - self.last_error_x)
        self.last_error_x = error_x # Update for next frame

        # 2. Base Speed (The Exponential Curve you already have)
        max_possible_error = FRAME_W / 2
        normalized_error = min(1.0, abs(error_x) / max_possible_error)
        base_factor = pow(normalized_error, SPEED_FACTOR)

        # 3. Sudden Move Boost (The "Turbo")
        # If the ball moved more than 30 pixels since the last frame,
        # we add a multiplier to the speed.
        boost_threshold = 20
        boost_gain = 1.7 # 70% extra speed during sudden moves

        speed_multiplier = 1.0
        if ball_velocity > boost_threshold:
            # Scale boost based on how 'sudden' the move is
            speed_multiplier += (ball_velocity / 100.0) * boost_gain

        # Scale only the minimum speed by zoom level to prevent oscillation near centre.
        # Maximum speed stays full — so tracking is fast when the ball is far away.
        effective_min = max(50, MIN_PAN_SPEED * speed_scale)
        speed = effective_min + (MAX_PAN_SPEED - effective_min) * base_factor
        speed = min(MAX_PAN_SPEED, speed * speed_multiplier)

        if override_speed is not None:
            speed = override_speed
        self.last_speed = speed

        # 4. Look-ahead and Buffer Management (Keep your 3.0x logic)
        step_duration = COMMAND_DT * 4.0 
        step = (speed / 60.0) * step_duration * (1 if error_x > 0 else -1)

        # Apply limits based on the LATEST synced position
        target = max(PAN_MIN_STEPS, min(PAN_MAX_STEPS, self.current_pan_pos + step))
        actual_step = target - self.current_pan_pos

        # Limit Check
        if abs(actual_step) < 0.1: # Changed from 0.2 to be more sensitive
            # If we are being told to move but target=current_pos, we are at software limit
            if abs(self.current_pan_pos - PAN_MAX_STEPS) < 0.5 or abs(self.current_pan_pos - PAN_MIN_STEPS) < 0.5:
                DEBUG and print(f"[PAN] Software Limit Reached at {self.current_pan_pos}")
            return

        cmd = f"$J=G91 X{actual_step:.3f} F{int(speed)}\n"
        self.ser_p.write(cmd.encode())
        
        # Incremental update (will be corrected by periodic sync)
        self.current_pan_pos += actual_step
        self.jogging = True

    def process_detection(self, detections, speed_scale=1.0):
        # 1. INCREMENT FRAME AND SYNC HARDWARE
        self.frame_count += 1
        if self.frame_count % self.sync_every_n_frames == 0:
            hw_pos = self._get_position()
            # If the difference is significant, force a correction
            if abs(hw_pos - self.current_pan_pos) > 0.3:
                DEBUG and print(f"[SYNC] Correcting drift: {self.current_pan_pos:.2f} -> {hw_pos:.2f}")
                self.current_pan_pos = hw_pos

        # 2. FIND BALL
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        
        # --- CASE 1: NO BALL DETECTED ---
        if not ball:
            self.lost_frames += 1
            if self.lost_frames > self.max_coast_frames:
                if self.jogging:
                    self._stop_jog()
                return

            if self.jogging and abs(self.last_speed) > 500:
                coast_speed = abs(self.last_speed) * 0.9 
                self.last_speed = coast_speed * self.last_direction
                ghost_error = (DEADZONE_PAN + 10) * self.last_direction
                self.send_command(ghost_error, override_speed=coast_speed, speed_scale=speed_scale)
            return

        # --- CASE 2: BALL FOUND ---
        error_x = ball['center_x'] - CENTER_X

        # 3. ROGUE JUMP REJECTION
        # We trust the ball if we just found it (lost_frames >= 1)
        if self.lost_frames < 1 and self.rogue_patience < 3 and self.last_error_x != 0.0:
            jump_amount = abs(error_x - self.last_error_x)
            if jump_amount > MAX_ERROR_JUMP:
                DEBUG and print(f"[PAN] Rogue jump rejected: {jump_amount:.0f}px")
                self.rogue_patience += 1
                return
            
        self.rogue_patience = 0
        self.lost_frames = 0 
        
        # 4. DIRECTION REVERSAL
        reversed_dir = self.last_error_x != 0.0 and (error_x > 0) != (self.last_error_x > 0)
        if reversed_dir and abs(error_x) > 20: 
            self._stop_jog()

        # 5. DEADZONE OR TRACK
        if abs(error_x) <= DEADZONE_PAN:
            self._stop_jog()
        else:
            self.send_command(error_x, speed_scale=speed_scale)
            self.last_direction = 1 if error_x > 0 else -1

        self.last_error_x = error_x

    def return_home(self):
        if self.ser_p:
            self.ser_p.write(b"\x85")           # Cancel any pending jog
            self.ser_p.write(b"G90 G0 X0\n")    # Return to home in absolute mode
            self.current_pan_pos = 0

if __name__ == "__main__":
    # Standalone: initialise and home the pan motor, then exit
    ctrl = PanController()
    ctrl.return_home()