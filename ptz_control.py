import json
import socket
import time
import sys
import atexit
import signal
from pan_control_esp_stationary import PanController
from zoom_control_stationary import ZoomController
import threading

_cleanup_done = False
_cleanup_lock = threading.Lock()
UNIX_SOCK  = "/tmp/ptz_control.sock"
TARGET_CAM = "fixed"
DEBUG = False 
ENABLE_PAN  = True
ENABLE_ZOOM = True

class PTZController:
    def __init__(self):
        self.pan  = PanController()  if ENABLE_PAN  else None
        self.zoom = ZoomController() if ENABLE_ZOOM else None

        if not self.pan and not self.zoom:
            print("WARNING: Both pan and zoom are disabled.")

    def process_detection(self, detections):
        speed_scale = self.zoom.get_pan_speed_factor() if (self.zoom and hasattr(self.zoom, 'ser_z') and self.zoom.ser_z) else 1.0
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

    def cleanup(self):
        print("Running cleanup...")
        global _cleanup_done
        with _cleanup_lock:
            if _cleanup_done:
                return
            _cleanup_done = True

        if self.zoom:
            try:
                self.zoom.return_home()
            except Exception as e:
                print(f"Zoom home failed: {e}")

        if self.pan:
            try:
                print("Returning home...")
                self.pan.return_home()
                time.sleep(3)
                print("Done. Exiting...")
            except Exception as e:
                print(f"Pan home failed: {e}")

        for name, obj, attr in [("Pan", self.pan, ['ser_p']), ("Zoom", self.zoom, ['ser_z'])]:
            if obj:
                for a in attr:
                    port = getattr(obj, a, None)
                    if port and port.is_open:
                        try:
                            port.reset_input_buffer()
                            port.reset_output_buffer()
                            port.close()
                        except:
                            pass
        time.sleep(0.5)

def socket_listener(controller):
    while True:
        try:
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(UNIX_SOCK)
            client.setblocking(False) # Non-blocking for "drain" logic
            print("Connected to socket.")

            buf = b"" # Use bytes, not strings, for raw socket data
            while True:
                try:
                    # 1. Read everything currently available in the OS buffer
                    while True:
                        try:
                            chunk = client.recv(8192)
                            if not chunk:
                                raise ConnectionResetError
                            buf += chunk
                        except BlockingIOError:
                            break # No more data to read right now

                    if not buf:
                        time.sleep(0.001) # Nano-sleep to prevent 100% CPU
                        continue

                    # 2. Extract all complete lines
                    lines = buf.split(b"\n")
                    
                    # 3. The last element is either empty or a partial line
                    buf = lines.pop() 

                    if not lines:
                        continue

                    # 4. CRITICAL: Only process the LATEST message for the TARGET_CAM
                    # This prevents the "lagging behind" effect
                    latest_valid_msg = None
                    for line in reversed(lines):
                        try:
                            msg = json.loads(line)
                            if msg.get("camera") == TARGET_CAM:
                                latest_valid_msg = msg
                                break # Found the newest one, ignore the rest
                        except json.JSONDecodeError:
                            continue

                    if latest_valid_msg:
                        controller.process_detection(latest_valid_msg.get("detections", []))

                except (socket.error, ConnectionResetError):
                    print("Socket connection lost.")
                    break
                
                # Small sleep to yield to other threads (Pan/Zoom)
                time.sleep(0.005)

        except (ConnectionRefusedError, FileNotFoundError):
            time.sleep(1)
        except Exception as e:
            print(f"Socket Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    motor_ctrl = PTZController()

    def signal_handler(sig, frame):
        motor_ctrl.cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    atexit.register(motor_ctrl.cleanup)

    try:
        socket_listener(motor_ctrl)
    except KeyboardInterrupt:
        motor_ctrl.cleanup()
        sys.exit(0)