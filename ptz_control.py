import json
import socket
import time
import sys
import atexit
import signal
from pan_control_esp_stationary import PanController
from zoom_control_stationary import ZoomController

# --- CONFIGURATION ---
UNIX_SOCK  = "/tmp/ptz_control.sock"
TARGET_CAM = "fixed"

DEBUG = False   # print every raw line received from the socket

# --- ENABLE / DISABLE CONTROLLERS ---
ENABLE_PAN  = True
ENABLE_ZOOM = True


class PTZController:
    def __init__(self):
        self.pan  = PanController()  if ENABLE_PAN  else None
        self.zoom = ZoomController() if ENABLE_ZOOM else None

        if not self.pan and not self.zoom:
            print("WARNING: Both pan and zoom are disabled.")

    def process_detection(self, detections):
        speed_scale = self.zoom.get_pan_speed_factor() if (self.zoom and self.zoom.ser_z) else 1.0
        if self.pan:
            self.pan.process_detection(detections, speed_scale=speed_scale)
        if self.zoom:
            pan_error_x = self.pan.last_error_x if self.pan else 0.0
            self.zoom.process_detection(detections, pan_error_x=pan_error_x)

    def process_manual_ptz(self, msg):
        pan  = msg.get("pan", 0)
        zoom = msg.get("zoom", 0)
        print(f"Manual Override: Pan {pan}, Zoom {zoom}")
        if pan and self.pan:
            self.pan.send_command(pan)
        if zoom and self.zoom:
            self.zoom.send_zoom(zoom)

    def return_home(self):
        if self.pan:
            self.pan.return_home()


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
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    if DEBUG:
                        print(f"[SOCKET] {line}")
                    try:
                        latest_msg = json.loads(line)
                    except json.JSONDecodeError:
                        print(f"[SOCKET] invalid JSON: {line}")
                        continue

                if latest_msg:
                    if latest_msg.get("camera") == TARGET_CAM:
                        controller.process_detection(latest_msg.get("detections", []))
                    elif DEBUG:
                        print(f"[SOCKET] ignored camera={latest_msg.get('camera')}")

        except (ConnectionRefusedError, FileNotFoundError):
            print("Waiting for socket...")
            time.sleep(2)
        except Exception as e:
            print(f"Socket Error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    motor_ctrl = PTZController()

    atexit.register(motor_ctrl.return_home)

    def signal_handler(sig, frame):
        print("\nInterrupt detected! Homing before exit...")
        motor_ctrl.return_home()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    socket_listener(motor_ctrl)
