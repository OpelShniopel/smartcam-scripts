import json
import socket
import serial
import time
import sys
import atexit
import signal
import lens_helpers
import pan_homing

# --- CONFIGURATION ---
UNIX_SOCK = "/tmp/pycam.sock"
SERIAL_PORT_P = "/dev/ttyACM0"
BAUD_RATE = 115200
TARGET_CAM = "CAM2"

# Frame dimensions (Ensure this matches your inference output)
FRAME_W = 1280 
FRAME_H = 720
CENTER_X = FRAME_W / 2

# --- CONTROL TUNING (PD Controller) ---
GAIN_P = 0.05       # Proportional: How fast it moves toward the target
GAIN_D = 0.02       # Derivative: The "brakes" to prevent overshoot (Tune this)
DEADZONE_PAN = 50   # Reduced deadzone for smoother micro-adjustments
PAN_SPEED = 3000

# Control limits
PAN_MAX_STEPS = 180
PAN_MIN_STEPS = -70

class PanController:
    def __init__(self):
        self.current_pan_pos = 0.0
        self.prev_error_x = 0.0
        self.last_time = time.time()
        self.last_command_time = 0.0
        self.command_rate_limit = 0.05 # Max 20 commands per second to not choke the serial buffer

        try:
            self.ser_p = serial.Serial(SERIAL_PORT_P, BAUD_RATE, timeout=0.1)
            print(f"SUCCESS: Connected to Pan Motor on {SERIAL_PORT_P}")
            pan_homing.auto_home_precision(self.ser_p)
            self.ser_p.write(b"G90\n") 
        except Exception as e:
            print(f"WARNING: Pan Motor Serial port not found. ({e})")
            self.ser_p = None

    def send_command(self, pan_steps, pan_speed=PAN_SPEED):
        if not self.ser_p:
            return
        
        current_time = time.time()
        # 1. BRAKE CHECK: If the ball reversed direction, 
        # we need to clear the motor buffer immediately.
        # (Assuming '!' is your controller's real-time stop command)
        moving_left = pan_steps < 0
        previously_moving_right = self.prev_error_x > 0
        
        if moving_left and previously_moving_right:
            self.ser_p.write(b"!") # Immediate Feed Hold (Purge Buffer)
            self.ser_p.write(b"~") # Cycle Start (Resume)
            # This "resets" the motor so it doesn't finish the old "Right" move
        
        # 2. RATE LIMIT: Don't send more than 15-20 commands per second.
        # This keeps the buffer from growing longer than ~100ms.
        if current_time - self.last_command_time < 0.06: 
            return

        new_pan_pos = self.current_pan_pos + pan_steps
        new_pan_pos = max(PAN_MIN_STEPS, min(PAN_MAX_STEPS, new_pan_pos))
        
        # 3. THRESHOLD: If the move is less than 1 step/unit, ignore it.
        # This prevents "micro-buffering" that causes choppiness.
        if abs(new_pan_pos - self.current_pan_pos) < 1.0:
            return

        self.current_pan_pos = new_pan_pos
        # Using $J= for GRBL or G1 for standard. 
        # Note: $J= requires absolute if you use G90.
        cmd = f"$J=X{new_pan_pos:.2f} F{int(pan_speed)}\n" 
        self.ser_p.write(cmd.encode())
        self.last_command_time = current_time

    def process_detection(self, detections):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        current_time = time.time()
        dt = current_time - self.last_time
        
        if ball and dt > 0:
            error_x = ball['center_x'] - CENTER_X
            
            if abs(error_x) > DEADZONE_PAN:
                # Calculate Derivative (Rate of change of the error)
                derivative = (error_x - self.prev_error_x) / dt
                
                # PD Control output
                movement = (error_x * GAIN_P) + (derivative * GAIN_D)
                self.send_command(movement)
            
            self.prev_error_x = error_x
        self.last_time = current_time

    def return_home(self):
        if self.ser_p:
            self.ser_p.write(b"G90 G0 X0\n")
            self.current_pan_pos = 0

def socket_listener(controller):
    while True:
        try:
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(UNIX_SOCK)
            print("Connected to socket.")

            buf = ""
            while True:
                chunk = client.recv(4096).decode()
                if not chunk:
                    break
                buf += chunk
                
                latest_msg = None
                
                # Extract all complete JSON messages from the buffer
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    try:
                        latest_msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                
                # ONLY process the absolute newest frame, discard the backlog
                if latest_msg:
                    cam = latest_msg.get("camera", "?")
                    if cam == TARGET_CAM:
                        controller.process_detection(latest_msg.get("detections", []))

        except (ConnectionRefusedError, FileNotFoundError):
            time.sleep(2)
        except Exception as e:
            print(f"Socket Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    motor_ctrl = PanController()
    atexit.register(motor_ctrl.return_home)

    def signal_handler(sig, frame):
        print("\nInterrupt detected! Homing before exit...")
        motor_ctrl.return_home()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    socket_listener(motor_ctrl)