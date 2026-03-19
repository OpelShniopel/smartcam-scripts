import json
import socket
import serial
import time
import sys
import atexit
import signal
import lens_helpers

# --- CONFIGURATION ---
CSV_FILE = "zoom_focus_table_updated.csv"
UNIX_SOCK = "/tmp/smartcam.sock"
SERIAL_PORT_P = "/dev/ttyACM0"      # Pan control serial port
SERIAL_PORT_Z = "/dev/ttyACM1"      # Zoom and Focus control serial port
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
TARGET_WIDTH = 50       # Target ball width in pixels
ZOOM_SPEED = 600
FOCUS_SPEED = 600

# Control limits
PAN_MAX_STEPS = 200
PAN_MIN_STEPS = 0
ZOOM_MAX_STEPS = 40000
ZOOM_MIN_STEPS = 30000
FOCUS_MAX_STEPS = 37000
FOCUS_MIN_STEPS = 32000

current_pan_pos = 0
current_zoom_pos = 0


class StepperController:
    def __init__(self):
        # --- PAN MOTOR (Separate Board) ---
        try:
            self.ser_p = serial.Serial(SERIAL_PORT_P, BAUD_RATE, timeout=0.1)
            print(f"SUCCESS: Connected to Pan Motor on {SERIAL_PORT_P}")
            self.ser_p.write(b"G90\r\n")
        except Exception as e:
            print(f"WARNING: Pan Motor Serial port not found. ({e})")
            self.ser_p = None

        # --- ZOOM & FOCUS (Kurokesu SCF4 Board) ---
        try:
            self.ser_z = serial.Serial(SERIAL_PORT_Z, 115200, timeout=1)
            time.sleep(1.5)  # Wait for STM32 boot
            print("Initializing Kurokesu Lens Board...")
            lens_helpers.init_lens_board(self.ser_z, ZOOM_SPEED, FOCUS_SPEED)
            print(f"SUCCESS: Zoom ({ZOOM_SPEED}) and Focus ({FOCUS_SPEED}) initialized.")
        except Exception as e:
            print(f"WARNING: Zoom/Focus Serial port not found. ({e})")
            self.ser_z = None

        # --- ZOOM/FOCUS INTERPOLATION ---
        self.focus_interp = lens_helpers.load_focus_interpolator(CSV_FILE)

        # --- CRITICAL VERIFICATION ---
        if self.ser_p and not lens_helpers.verify_command(self.ser_p, "G90"):
            print("CRITICAL: Pan motor failed response check.")

        if self.ser_z and not lens_helpers.verify_command(self.ser_z, "G90"):
            print("CRITICAL: Kurokesu board failed response check.")

    def calibrate_lens(self):
        if not self.ser_z:
            print("Calibration skipped: No serial connection.")
            return
        lens_helpers.calibrate_lens(self.ser_z)

    def get_focus_for_zoom(self, zoom_pos):
        return int(self.focus_interp(zoom_pos))

    def send_command(self, pan_steps, zoom_steps, pan_speed=500, zoom_speed=500):
        global current_pan_pos, current_zoom_pos
        if self.ser_p:
            new_pan_pos = current_pan_pos + pan_steps
            if PAN_MIN_STEPS <= new_pan_pos <= PAN_MAX_STEPS:
                current_pan_pos = new_pan_pos
                cmd = f"G1 X{int(new_pan_pos)} F{int(pan_speed)}\n"
                self.ser_p.write(cmd.encode())

        if self.ser_z:
            new_zoom_pos = current_zoom_pos + zoom_steps
            if ZOOM_MIN_STEPS <= new_zoom_pos <= ZOOM_MAX_STEPS:
                current_zoom_pos = new_zoom_pos
                new_focus_pos = self.get_focus_for_zoom(new_zoom_pos)
                if new_focus_pos <= FOCUS_MIN_STEPS or new_focus_pos >= FOCUS_MAX_STEPS:
                    new_focus_pos = 4000
                cmd = f"G1 A{int(new_zoom_pos)} B{int(new_focus_pos)} F{int(zoom_speed)}\n"
                self.ser_z.write(cmd.encode())

    def process_detection(self, detections):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        if ball:
            error_x = ball['center_x'] - CENTER_X
            zoom_error = TARGET_WIDTH - ball['width']

            pan_move = 0
            zoom_move = 0

            if abs(error_x) > DEADZONE_PAN:
                pan_move = error_x * GAIN_X

            if abs(zoom_error) > DEADZONE_ZOOM:
                zoom_move = zoom_error * GAIN_ZOOM

            if pan_move != 0 or zoom_move != 0:
                self.send_command(pan_move, zoom_move)

    def process_manual_ptz(self, msg):
        pan = msg.get("pan", 0)
        zoom = msg.get("zoom", 0)
        print(f"Manual Override: Pan {pan}, Zoom {zoom}")
        self.send_command(pan, zoom, 1000, 1000)

    def return_home(self):
        global current_pan_pos
        self.send_command(-current_pan_pos, 0)


def socket_listener(controller):
    while True:
        try:
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(UNIX_SOCK)
            print("Connected to SmartCam Socket.")

            fileobj = client.makefile('r')
            for line in fileobj:
                data = json.loads(line)

                if data.get("type") == "detection" and data.get("camera") == TARGET_CAM:
                    controller.process_detection(data.get("detections", []))

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
