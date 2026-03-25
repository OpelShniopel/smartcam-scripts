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
SERIAL_PORT_P = "/dev/ttyACM0"      # Pan control serial port
BAUD_RATE = 115200
TARGET_CAM = "CAM2"                 # Camera to use for control

# Frame dimensions (must match your DeepStream output resolution)
FRAME_W = 1280
FRAME_H = 720
CENTER_X = FRAME_W / 2

# Control Tuning
GAIN_X = 0.05       # Pan sensitivity
DEADZONE_PAN = 100  # Pan deadzone

# Control limits
PAN_MAX_STEPS = 180
PAN_MIN_STEPS = -70

current_pan_pos = 0


class PanController:
    def __init__(self):
        try:
            self.ser_p = serial.Serial(SERIAL_PORT_P, BAUD_RATE, timeout=0.1)
            print(f"SUCCESS: Connected to Pan Motor on {SERIAL_PORT_P}")
            pan_homing.auto_home_precision(self.ser_p)
            self.ser_p.write(b"G90\n")  # Ensure absolute mode after homing
        except Exception as e:
            print(f"WARNING: Pan Motor Serial port not found. ({e})")
            self.ser_p = None

        if self.ser_p and not lens_helpers.verify_command(self.ser_p, "G90"):
            print("CRITICAL: Pan motor failed response check.")

    def send_command(self, pan_steps, pan_speed=500):
        global current_pan_pos
        if not self.ser_p:
            return
        new_pan_pos = current_pan_pos + pan_steps
        if PAN_MIN_STEPS <= new_pan_pos <= PAN_MAX_STEPS:
            current_pan_pos = new_pan_pos
            cmd = f"G1 X{int(new_pan_pos)} F{int(pan_speed)}\n"
            self.ser_p.write(cmd.encode())

    def process_detection(self, detections):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        if ball:
            error_x = ball['center_x'] - CENTER_X
            if abs(error_x) > DEADZONE_PAN:
                self.send_command(error_x * GAIN_X)

    def process_manual_pan(self, msg):
        pan = msg.get("pan", 0)
        print(f"Manual Override: Pan {pan}")
        self.send_command(pan, pan_speed=1000)

    def return_home(self):
        global current_pan_pos
        if self.ser_p:
            self.ser_p.write(b"G90 G0 X0\n")
            current_pan_pos = 0


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
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    msg = json.loads(line)
                    if msg.get("camera") == TARGET_CAM:
                        controller.process_detection(msg.get("detections", []))

        except (ConnectionRefusedError, FileNotFoundError):
            print("Waiting for socket...")
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
