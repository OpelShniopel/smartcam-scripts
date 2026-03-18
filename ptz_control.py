import json
import socket
import serial
import time
import threading
import sys

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

# Control limits
PAN_MAX_STEPS = 100
PAN_MIN_STEPS = -100
ZOOM_MAX_STEPS = 10
ZOOM_MIN_STEPS = 0

current_pan_pos = 0
current_zoom_pos = 0

class StepperController:
    def __init__(self):
        try:
            self.ser_p = serial.Serial(SERIAL_PORT_P, BAUD_RATE, timeout=0.1)
            print(f"SUCCESS: Connected to Pan Motor on {SERIAL_PORT_P}")           
        except:
            print("WARNING: Pan Motor Serial port not found. Running in simulation mode.")
            self.ser_p = None

        try:
            self.ser_z = serial.Serial(SERIAL_PORT_Z, BAUD_RATE, timeout=0.1)
            print(f"SUCCESS: Connected to Zoom Motor on {SERIAL_PORT_Z}")            
        except:
            print("WARNING: Zoom Motor Serial port not found. Running in simulation mode.")
            self.ser_z = None

        if self.ser_p != None and not self.verify_command(self.ser_p, "G91"):
            print("CRITICAL: Pan motor failed to enter Relative Mode. Aborting.")
            sys.exit(1)

        if self.ser_z != None and not self.verify_command(self.ser_z, "G91"):
            print("CRITICAL: Zoom motor failed to enter Relative Mode. Aborting.")
            sys.exit(1)

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
                cmd = f"G1 X{int(pan_steps)} F{int(pan_speed)}\n"
                self.ser_p.write(cmd.encode())

        if self.ser_z:
            new_zoom_pos = current_zoom_pos + zoom_steps
            if  new_zoom_pos >= ZOOM_MIN_STEPS and new_zoom_pos <= ZOOM_MAX_STEPS:
                current_zoom_pos = new_zoom_pos
                # Format: "G1 A10 F10000" (A - zoom, B - focus)
                cmd = f"G1 A{int(zoom_steps)} F{int(zoom_speed)}\n"
                self.ser_z.write(cmd.encode())

    def process_detection(self, detections):
        # Find the basketball (Class ID 1 or label 'basketball')
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
    socket_listener(motor_ctrl)