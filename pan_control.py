import serial
import time
import lens_helpers
import pan_homing

DEBUG = False

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
COMMAND_DT    = 0.02                            # seconds per jog segment: s = (speed/60) * dt
                                                # at MAX_PAN_SPEED: 3.3 units (1.65°) per step

# Control limits (1 unit = 0.5°)
PAN_MAX_STEPS =  180    # +90°  right
PAN_MIN_STEPS =  -70    # -35°  left

class PanController:
    def __init__(self):
        self.current_pan_pos = 0.0
        self.jogging = False
        self.last_error_x = 0.0
        self.pending_oks = 0
        self.lost_frames = 0
        self.max_coast_frames = 10 # Coast for ~0.2 seconds at 50fps
        self.last_speed = 0
        self.last_direction = 0

        try:
            self.ser_p = serial.Serial(SERIAL_PORT_P, BAUD_RATE, timeout=0.1)
            print(f"SUCCESS: Connected to Pan Motor on {SERIAL_PORT_P}")
            pan_homing.auto_home_precision(self.ser_p)
        except Exception as e:
            print(f"WARNING: Pan Motor Serial port not found. ({e})")
            self.ser_p = None

    def _get_position(self):
        """Query actual motor X position from GRBL status report."""
        try:
            self.ser_p.reset_input_buffer()
            self.ser_p.write(b"?\n")
            line = self.ser_p.readline().decode('utf-8')
            if "MPos:" in line:
                x = float(line.split("MPos:")[1].split(",")[0])
                return x
        except Exception as e:
            DEBUG and print(f"[PAN] Position query failed: {e}")
        return self.current_pan_pos  # fallback if query fails

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

    def send_command(self, error_x, override_speed=None):
        if not self.ser_p:
            return
        
        speed = override_speed if override_speed is not None else speed
        self.last_speed = speed

        # 1. Calculate Ball Velocity (pixels per frame)
        # Positive if moving right, negative if moving left
        ball_velocity = abs(error_x - self.last_error_x)
        self.last_error_x = error_x # Update for next frame

        # 2. Base Speed (The Exponential Curve you already have)
        max_possible_error = FRAME_W / 2
        normalized_error = min(1.0, abs(error_x) / max_possible_error)
        base_factor = pow(normalized_error, 2.0) 

        # 3. Sudden Move Boost (The "Turbo")
        # If the ball moved more than 30 pixels since the last frame, 
        # we add a multiplier to the speed.
        boost_threshold = 20 
        boost_gain = 1.5 # 50% extra speed during sudden moves
        
        speed_multiplier = 1.0
        if ball_velocity > boost_threshold:
            # Scale boost based on how 'sudden' the move is
            speed_multiplier += (ball_velocity / 100.0) * boost_gain

        # Calculate final speed with boost
        speed = MIN_PAN_SPEED + (MAX_PAN_SPEED - MIN_PAN_SPEED) * base_factor
        speed = min(MAX_PAN_SPEED, speed * speed_multiplier)

        # 4. Look-ahead and Buffer Management (Keep your 3.0x logic)
        step_duration = COMMAND_DT * 4.0 
        step = (speed / 60.0) * step_duration * (1 if error_x > 0 else -1)

        target = max(PAN_MIN_STEPS, min(PAN_MAX_STEPS, self.current_pan_pos + step))
        actual_step = target - self.current_pan_pos
        
        # 5. Ignore "Micro-twitches" that the motor can't physically do smoothly
        if abs(actual_step) < 1: 
            return

        cmd = f"$J=G91 X{actual_step:.3f} F{int(speed)}\n"
        self.ser_p.write(cmd.encode())
        
        # Important: Don't update current_pan_pos by the FULL 'step' 
        # only update it by what we expect to cover in ONE COMMAND_DT
        # this keeps the 'target' from drifting too far ahead of reality
        self.current_pan_pos += (speed / 60.0) * COMMAND_DT * (1 if error_x > 0 else -1)
        
        self.jogging = True
        self.pending_oks += 1

    def process_detection(self, detections):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        
        # --- CASE 1: NO BALL DETECTED ---
        if not ball:
            self.lost_frames += 1
            
            # Only stop if we've been blind for too long (e.g., 10 frames)
            if self.lost_frames > self.max_coast_frames:
                if self.jogging:
                    self._stop_jog()
                return

            # ACTIVE COAST: If we were moving fast, keep the momentum going
            if self.jogging and abs(self.last_speed) > 500:
                # Decay the speed (0.9 = 10% slowdown per frame)
                coast_speed = abs(self.last_speed) * 0.9 
                self.last_speed = coast_speed * self.last_direction # Keep the sign
                
                # Calculate a 'ghost' error based on last direction to use send_command
                # We simulate an error just outside the deadzone to keep the loop alive
                ghost_error = (DEADZONE_PAN + 10) * self.last_direction
                self.send_command(ghost_error, override_speed=coast_speed)
            return

        # --- CASE 2: BALL FOUND ---
        self.lost_frames = 0 # Reset the counter
        error_x = ball['center_x'] - CENTER_X
        
        # Handle Direction Reversal
        # Use a small buffer so a 1-pixel flicker doesn't trigger a hard stop
        reversed = self.last_error_x != 0.0 and (error_x > 0) != (self.last_error_x > 0)
        if reversed and abs(error_x) > 20: 
            DEBUG and print(f"[DETECT] Direction reversal, cancelling jog")
            self._stop_jog()

        # Handle Deadzone
        if abs(error_x) <= DEADZONE_PAN:
            self._stop_jog()
        else:
            # Normal tracking
            self.send_command(error_x)
            # Store direction for coasting logic (+1 for Right, -1 for Left)
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