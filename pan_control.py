import json
import socket
import serial
import time
import sys
import atexit
import signal
import threading
import lens_helpers
import pan_homing

DEBUG = False

# --- CONFIGURATION ---
UNIX_SOCK = "/tmp/pycam.sock"
SERIAL_PORT_P = "/dev/ttyACM0"
BAUD_RATE = 115200
TARGET_CAM = "CAM2"

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
COMMAND_DT    = 0.04                            # seconds per jog segment: s = (speed/60) * dt
                                                # at MAX_PAN_SPEED: 3.3 units (1.65°) per step

# Control limits (1 unit = 0.5°)
PAN_MAX_STEPS =  180    # +90°  right
PAN_MIN_STEPS =  -70    # -35°  left

class PanController:
    def __init__(self):
        self.current_pan_pos = 0.0
        self.jogging = False
        self.error_x = 0.0
        self.last_error_x = 0.0
        self._serial_lock = threading.Lock()
        self._stop_event = threading.Event()

        try:
            self.ser_p = serial.Serial(SERIAL_PORT_P, BAUD_RATE, timeout=0.1)
            print(f"SUCCESS: Connected to Pan Motor on {SERIAL_PORT_P}")
            pan_homing.auto_home_precision(self.ser_p)
        except Exception as e:
            print(f"WARNING: Pan Motor Serial port not found. ({e})")
            self.ser_p = None

        self._jog_thread = threading.Thread(target=self._jog_loop, daemon=True)
        self._jog_thread.start()

    def _stop_jog(self):
        """Cancel pending jog. Must hold _serial_lock when calling."""
        if not self.jogging:
            return
        self.ser_p.write(b"\x85")
        time.sleep(0.05)
        self.ser_p.reset_input_buffer()
        self.jogging = False
        DEBUG and print(f"[PAN] Stopped at X={self.current_pan_pos:.1f}")

    def _jog_loop(self):
        """Dedicated thread: continuously feeds GRBL planner buffer independently
        of the socket/detection rate. Socket thread only updates self.error_x."""
        while True:
            if not self.ser_p:
                time.sleep(0.1)
                continue

            # Direction reversal requested by detection thread
            if self._stop_event.is_set():
                with self._serial_lock:
                    self._stop_jog()
                self._stop_event.clear()
                continue

            error_x = self.error_x

            if abs(error_x) <= DEADZONE_PAN:
                with self._serial_lock:
                    self._stop_jog()
                time.sleep(0.01)
                continue

            speed = max(MIN_PAN_SPEED, min(MAX_PAN_SPEED, abs(error_x) * SPEED_GAIN))
            step = (speed / 60.0) * COMMAND_DT * (1 if error_x > 0 else -1)
            target = max(PAN_MIN_STEPS, min(PAN_MAX_STEPS, self.current_pan_pos + step))
            actual_step = target - self.current_pan_pos

            if abs(actual_step) < 0.1:
                DEBUG and print(f"[PAN] Limit at X={self.current_pan_pos:.1f}")
                with self._serial_lock:
                    self._stop_jog()
                time.sleep(0.01)
                continue

            with self._serial_lock:
                self.ser_p.write(f"$J=G91 X{actual_step:.3f} F{int(speed)}\n".encode())
                resp = self.ser_p.readline().decode().strip()

            if 'ok' in resp:
                self.current_pan_pos = target
                self.jogging = True
                DEBUG and print(f"[PAN] Jog step={actual_step:+.2f}  pos={target:.1f}  speed={int(speed)}")
            else:
                DEBUG and print(f"[PAN] Response: {resp}")

    def process_detection(self, detections):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        if not ball:
            self.error_x = 0.0
            self.last_error_x = 0.0
            return

        error_x = ball['center_x'] - CENTER_X
        DEBUG and print(f"[DETECT] center_x={ball['center_x']:.0f}  error_x={error_x:+.0f}")

        if abs(error_x) <= DEADZONE_PAN:
            DEBUG and print(f"[DETECT] In deadzone")
            self.error_x = 0.0
        else:
            if self.last_error_x != 0.0 and (error_x > 0) != (self.last_error_x > 0):
                DEBUG and print(f"[DETECT] Direction reversal")
                self._stop_event.set()
            self.error_x = error_x

        self.last_error_x = error_x

    def return_home(self):
        if self.ser_p:
            self.error_x = 0.0
            with self._serial_lock:
                self.ser_p.write(b"\x85")
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