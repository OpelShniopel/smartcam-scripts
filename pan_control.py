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
        self.frame_count = 0
        self.sync_every_n_frames = 15 

        try:
            # timeout=0.1 allows enough time for a full status string to arrive
            self.ser_p = serial.Serial(SERIAL_PORT_P, BAUD_RATE, timeout=0.1) 
            print(f"SUCCESS: Connected to Pan Motor on {SERIAL_PORT_P}")
            
            # Run your homing sequence
            pan_homing.auto_home_precision(self.ser_p)
            
            # Set Work Zero at the home position to align with your PC/Putty tests
            time.sleep(0.5)
            self.ser_p.write(b"G10 L20 P1 X0\n") 
            
            # Initial sync
            self.current_pan_pos = self._get_position()
            print(f"Controller Ready. Initial Position: {self.current_pan_pos}")
        except Exception as e:
            print(f"WARNING: Pan Motor Serial port not found. ({e})")
            self.ser_p = None

    def _get_position(self):
        """Query actual motor Work Position (WPos)."""
        if not self.ser_p: return self.current_pan_pos
        try:
            # 1. Clear the buffer so we don't read old "ok" messages
            self.ser_p.reset_input_buffer()
            # 2. Ask for status
            self.ser_p.write(b"?") 
            time.sleep(0.02) # Shortest reliable wait for GRBL
            # 3. Read the status line
            line = self.ser_p.readline().decode('utf-8', errors='ignore')
            
            if "WPos:" in line:
                # Extracts the X value from <...|WPos:0.000,0.000,0.000|...>
                pos_str = line.split("WPos:")[1].split(",")[0]
                return float(pos_str)
            elif "MPos:" in line:
                # Fallback to MPos if WPos isn't configured
                pos_str = line.split("MPos:")[1].split(",")[0]
                return float(pos_str)
        except Exception as e:
            DEBUG and print(f"[PAN] Sync error: {e}")
        return self.current_pan_pos

    def send_command(self, error_x, override_speed=None, speed_scale=1.0):
        if not self.ser_p: return
        
        # --- Speed Logic ---
        ball_velocity = abs(error_x - self.last_error_x)
        normalized_error = min(1.0, abs(error_x) / (FRAME_W / 2))
        base_factor = pow(normalized_error, SPEED_FACTOR)
        
        speed_multiplier = 1.0
        if ball_velocity > 20:
            speed_multiplier += (ball_velocity / 100.0) * 1.7

        effective_min = max(50, MIN_PAN_SPEED * speed_scale)
        speed = effective_min + (MAX_PAN_SPEED - effective_min) * base_factor
        speed = min(MAX_PAN_SPEED, (override_speed or speed) * speed_multiplier)
        self.last_speed = speed

        # --- Movement Logic ---
        step_duration = COMMAND_DT * 3.0 
        step = (speed / 60.0) * step_duration * (1 if error_x > 0 else -1)
        
        # Calculate intended target
        target = self.current_pan_pos + step
        
        # HARD CLAMP: Ensure target never exceeds limits
        if target > PAN_MAX_STEPS: target = PAN_MAX_STEPS
        if target < PAN_MIN_STEPS: target = PAN_MIN_STEPS
        
        actual_step = target - self.current_pan_pos

        # If we are at the limit and trying to move further out, abort
        if abs(actual_step) < 0.01:
            return

        # Write the Jog command
        cmd = f"$J=G91 X{actual_step:.3f} F{int(speed)}\n"
        self.ser_p.write(cmd.encode())
        
        # Update internal tracker
        self.current_pan_pos += actual_step
        self.jogging = True

    def process_detection(self, detections, speed_scale=1.0):
        self.frame_count += 1
        
       # Periodically anchor the software position to hardware reality
        if self.frame_count % self.sync_every_n_frames == 0:
            hw_pos = self._get_position()
            drift = abs(hw_pos - self.current_pan_pos)
            
            if drift > 5.0:  # If we are off by more than 2.5 degrees
                DEBUG and print(f"[CRITICAL SYNC] Huge drift ({drift:.2f}). Resetting buffer.")
                self.ser_p.write(b"\x85")      # Cancel current jogs immediately
                time.sleep(0.02)
                self.ser_p.reset_input_buffer() # Clear old 'ok' responses
                self.current_pan_pos = hw_pos   # Snap to reality
                return # Skip this frame to let the motor settle
            
            elif drift > 0.2:
                self.current_pan_pos = hw_pos

        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        
        if not ball:
            self.lost_frames += 1
            if self.lost_frames > self.max_coast_frames:
                self._stop_jog()
                return
            # Momentum Coasting
            if self.jogging and abs(self.last_speed) > 500:
                coast_speed = abs(self.last_speed) * 0.8
                ghost_error = (DEADZONE_PAN + 10) * self.last_direction
                self.send_command(ghost_error, override_speed=coast_speed, speed_scale=speed_scale)
            return

        # Standard Detection Logic
        error_x = ball['center_x'] - CENTER_X
        
        # Rogue Jump rejection
        if self.lost_frames < 1 and self.rogue_patience < 3 and self.last_error_x != 0.0:
            if abs(error_x - self.last_error_x) > MAX_ERROR_JUMP:
                self.rogue_patience += 1
                return
            
        self.rogue_patience = 0
        self.lost_frames = 0 
        
        # Hard Stop on Direction Reversal
        if self.last_error_x != 0.0 and (error_x > 0) != (self.last_error_x > 0) and abs(error_x) > 20: 
            self._stop_jog()

        if abs(error_x) <= DEADZONE_PAN:
            self._stop_jog()
        else:
            self.send_command(error_x, speed_scale=speed_scale)
            self.last_direction = 1 if error_x > 0 else -1

        self.last_error_x = error_x

    def _stop_jog(self):
        if not self.ser_p or not self.jogging: return
        self.ser_p.write(b"\x85") # GRBL Jog Cancel
        time.sleep(0.05)
        self.ser_p.reset_input_buffer()
        self.current_pan_pos = self._get_position()
        self.jogging = False

    def return_home(self):
        if self.ser_p:
            self.ser_p.write(b"\x85")           # Cancel any pending jog
            self.ser_p.write(b"G90 G0 X0\n")    # Return to home in absolute mode
            self.current_pan_pos = 0

if __name__ == "__main__":
    # Standalone: initialise and home the pan motor, then exit
    ctrl = PanController()
    ctrl.return_home()