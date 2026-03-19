import json
import socket
import serial
import time
import threading
import sys
import atexit
import signal

# --- CONFIGURATION ---
UNIX_SOCK = "/tmp/smartcam.sock"
SERIAL_PORT_P = "/dev/ttyACM0"      # Pan control serial port
SERIAL_PORT_Z = "/dev/ttyACM1"     # Zoom and Focus control serial port
BAUD_RATE = 115200
TARGET_CAM = "CAM2"                 # Camera to use for control

# Frame dimensions (must match your DeepStream output resolution)
FRAME_W = 1280
FRAME_H = 720
CENTER_X = FRAME_W / 2
CENTER_Y = FRAME_H / 2

# Control Tuning
GAIN_X = 0.05           # Pan sensitivity
GAIN_ZOOM = 0.2         # Zoom sensitivity
DEADZONE_PAN = 100      # Pan deadzone
DEADZONE_ZOOM = 15      # Zoom deadzone 
TARGET_WIDTH = 50       # Target ball widh in pixels
ZOOM_SPEED = 600
FOCUS_SPEED = 600

# Control limits
PAN_MAX_STEPS = 200
PAN_MIN_STEPS = 0
ZOOM_MAX_STEPS = 41800
ZOOM_MIN_STEPS = 22500
FOCUS_MAX_STEPS = 34000
FOCUS_MIN_STEPS = 29000

current_pan_pos = 0
current_zoom_pos = 0

class StepperController:
    def __init__(self):
        # --- PAN MOTOR (Separate Board) ---
        try:
            self.ser_p = serial.Serial(SERIAL_PORT_P, BAUD_RATE, timeout=0.1)
            print(f"SUCCESS: Connected to Pan Motor on {SERIAL_PORT_P}")           
            # Standard G-code init for Pan
            self.ser_p.write(b"G90\r\n") 
        except Exception as e:
            print(f"WARNING: Pan Motor Serial port not found. ({e})")
            self.ser_p = None

        # --- ZOOM & FOCUS (Kurokesu SCF4 Board) ---
        try:
            # Using dsrdtr=False is critical for STM32-based Kurokesu boards on Linux
            self.ser_z = serial.Serial(SERIAL_PORT_Z, 115200, timeout=1)
            time.sleep(1.5) # Wait for STM32 boot
            
            # We use a list with f-strings to plug in your variables
            init_cmds = [
                "$B2",                          # Boot/Handshake
                "M243 C6",                      # Stepping mode
                "M230",                         # Set normal move mode
                "G91",                          # Relative movement (initial)
                "M238",                         # Energize Photo-Interrupter LEDs
                "M234 A190 B190 C190 D90",      # Set Motor Power (Running)
                "M235 A120 B120 C120",          # Set Motor Power (Sleep/Hold)
                f"M240 A{ZOOM_SPEED} B{FOCUS_SPEED} C600", # Set motor drive speeds
                "M232 A400 B400 C400 E700 F700 G700" # PI Voltage thresholds
            ]

            print("Initializing Kurokesu Lens Board...")
            for cmd in init_cmds:
                # Kurokesu requires \r\n and utf8 encoding
                self.ser_z.write(bytes(cmd + '\r\n', 'utf8'))
                # Wait for the board to process each configuration step
                time.sleep(0.05) 
                
            print(f"SUCCESS: Zoom ({ZOOM_SPEED}) and Focus ({FOCUS_SPEED}) initialized.")

        except Exception as e:
            print(f"WARNING: Zoom/Focus Serial port not found. ({e})")
            self.ser_z = None

        # --- CRITICAL VERIFICATION ---
        # We check if the ports are actually talking back
        if self.ser_p and not self.verify_command(self.ser_p, "G90"):
            print("CRITICAL: Pan motor failed response check.")
            # sys.exit(1) # Optional: decide if you want to hard-abort

        if self.ser_z and not self.verify_command(self.ser_z, "G90"):
            print("CRITICAL: Kurokesu board failed response check.")
            # sys.exit(1)

    def calibrate_lens(self):
        """
        Runs the Homing (Seek) routine for Zoom (A) and Focus (B).
        Must be called after ser_z is initialized.
        """
        if not self.ser_z:
            print("Calibration skipped: No serial connection.")
            return

        print("--- Starting Lens Calibration ---")
        
        # 1. Setup: Turn on LEDs and set to Relative Mode for the search
        self.ser_z.write(b"M238\r\n")  # Turn on Optocoupler LEDs
        self.ser_z.write(b"G91\r\n")   # Relative mode
        time.sleep(0.5)

        # List of axes to calibrate (Zoom and Focus)
        for axis in ['A', 'B']:
            print(f"Calibrating Axis {axis}...")
            
            # 2. Start Movement: Forced mode ignores software limits
            self.ser_z.write(f"M231 {axis}\r\n".encode()) 
            
            # Move 'outward' towards the sensor. 
            # 40000 is more than the full length of the lens to ensure we hit the wall.
            self.ser_z.write(f"G0 {axis}40000\r\n".encode()) 

            # 3. Polling Loop: Wait until movement stops (hit the sensor)
            timeout = 10  # 10 second safety timeout
            start_time = time.time()
            
            while (time.time() - start_time) < timeout:
                self.ser_z.write(b"!1\r\n")
                line = self.ser_z.readline().decode('utf-8').strip()
                
                if line and "," in line:
                    status = line.split(",")
                    # Index 6 is Axis A moving, Index 7 is Axis B moving
                    is_moving = int(status[6] if axis == 'A' else status[7])
                    
                    if is_moving == 0:
                        print(f"Axis {axis} reached physical limit.")
                        break
                time.sleep(0.1)

            # 4. Set Reference: Define this physical spot as 32000
            self.ser_z.write(f"G92 {axis}32000\r\n".encode())
            
        # 5. Reset: Turn back on Normal safety mode
        self.ser_z.write(f"M230 {axis}\r\n".encode())

        # 6. Finalize: Shut down LEDs and set to Absolute mode
        self.ser_z.write(b"M239\r\n")
        self.ser_z.write(b"G90\r\n")
        print("--- Calibration Complete: LEDs Powered Down ---")

    def verify_command(self, ser, cmd):
        """Sends a command and waits for 'ok' response from the driver"""
        try:
            ser.reset_input_buffer() # Clear old messages
            ser.write(f"{cmd}\n".encode())
            time.sleep(0.1) # Give the motor a moment to think
            
            response = ser.readline().decode().strip().lower()
            #print(f"Motor Response: {response}") # Helpful for debugging
            
            return "ok" in response
        except Exception as e:
            print(f"Communication error during command verification: {e}")
            return False

    def send_command(self, pan_steps, zoom_steps, pan_speed=10000, zoom_speed=10000):
        global current_pan_pos, current_zoom_pos
        if self.ser_p:
            new_pan_pos = current_pan_pos + pan_steps
            if  new_pan_pos >= PAN_MIN_STEPS and new_pan_pos <= PAN_MAX_STEPS:
                current_pan_pos = new_pan_pos
                # Format: "G1 X10 F10000"
                cmd = f"G1 X{int(new_pan_pos)} F{int(pan_speed)}\n"
                self.ser_p.write(cmd.encode())

        if self.ser_z:
            new_zoom_pos = current_zoom_pos + zoom_steps
            if  new_zoom_pos >= ZOOM_MIN_STEPS and new_zoom_pos <= ZOOM_MAX_STEPS:
                current_zoom_pos = new_zoom_pos
                new_focus_pos = self.get_focus_for_zoom(new_zoom_pos)
                if new_focus_pos <= FOCUS_MIN_STEPS or new_focus_pos >= FOCUS_MAX_STEPS:
                    new_focus_pos = 4000
                # Format: "G1 A10 B100 F10000" (A - zoom, B - focus)
                cmd = f"G1 A{int(new_zoom_pos)} B{int(new_focus_pos)} F{int(zoom_speed)}\n"
                self.ser_z.write(cmd.encode())

    def process_detection(self, detections):
        # Find the basketball (Class ID 1 or label 'BALL')
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        
        if ball:
            # 1. Calculate Pan (Horizontal Error)
            error_x = ball['center_x'] - CENTER_X
            
            # 2. Calculate Zoom (Size Error)
            # If current width < target, zoom_error is positive (Zoom In)
            # If current width > target, zoom_error is negative (Zoom Out)
            current_width = ball['width']
            zoom_error = TARGET_WIDTH - current_width

            pan_move = 0
            zoom_move = 0

            # Apply Pan Logic
            if abs(error_x) > DEADZONE_PAN:
                pan_move = error_x * GAIN_X
                
            # Apply Zoom Logic
            if abs(zoom_error) > DEADZONE_ZOOM:
                zoom_move = zoom_error * GAIN_ZOOM

            if pan_move != 0 or zoom_move != 0:
                self.send_command(pan_move, zoom_move)

    def process_manual_ptz(self, msg):
        """Handles manual overrides from Go backend"""
        pan = msg.get("pan", 0)
        zoom = msg.get("zoom", 0)
        print(f"Manual Override: Pan {pan}, Zoom {zoom}")
        # slow down for manual control
        self.send_command(pan, zoom, 1000, 1000)

    def return_home(self):
        """Returns pan motor to home position"""
        global current_pan_pos, current_zoom_pos
        pan_steps = -current_pan_pos
        self.send_command(pan_steps, 0)

    def get_focus_for_zoom(self, zoom_pos):
        """Approximation of the 'inf' curve from your diagram"""
        # Normalize zoom_pos to a 0.0 - 1.0 scale for calculation
        z = zoom_pos / ZOOM_MAX_STEPS 
        
        if z < 0.6:
            # Linear climb (adjust 400 to match your max focus steps)
            return int(400 * (z / 0.6))
        else:
            # Parabolic drop on the Tele side
            dist_from_peak = (z - 0.6) / 0.4
            return int(400 - (1000 * (dist_from_peak ** 2)))

def socket_listener(controller):
    while True:
        try:
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(UNIX_SOCK)
            print("Connected to SmartCam Socket.")
            
            fileobj = client.makefile('r')
            for line in fileobj:
                data = json.loads(line)
                
                # 1. Handle Auto-Tracking
                if data.get("type") == "detection" and data.get("camera") == TARGET_CAM:
                    controller.process_detection(data.get("detections", []))
                
                # 2. Handle Manual Commands from Go
                elif data.get("type") == "cmd" and data.get("action") == "manual_ptz":
                    controller.process_manual_ptz(data)
                    
        except (ConnectionRefusedError, FileNotFoundError):
            print("Waiting for SmartCam socket...")
            time.sleep(2)
        except Exception as e:
            print(f"Socket Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    motor_ctrl = StepperController()

    atexit.register(motor_ctrl.return_home)

    def signal_handler(sig, frame):
        print("\nInterrupt detected! Homing before exit...")
        motor_ctrl.return_home()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    socket_listener(motor_ctrl)