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

# --- CONTROL TUNING ---
GAIN_P = 0.05       # Proportional: How fast it moves toward the target
DEADZONE_PAN = 50
PAN_SPEED = 4000

# Control limits
PAN_MAX_STEPS = 180
PAN_MIN_STEPS = -70

class PanController:
    def __init__(self):
        self.current_pan_pos = 0.0
        self.last_command_time = 0.0
        self.command_interval = 0.05    # 20Hz — motor gets 50ms to move before next update

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
            print(f"[PAN] Position query failed: {e}")
        return self.current_pan_pos  # fallback if query fails

    def send_command(self, pan_delta, pan_speed=PAN_SPEED):
        if not self.ser_p:
            return
        if abs(pan_delta) < 1.0:
            return
        if time.time() - self.last_command_time < self.command_interval:
            return

        # Cancel pending jog first, then read where the motor actually stopped
        self.ser_p.write(b"\x85")
        self.current_pan_pos = self._get_position()

        # Hard limit check against real position — no drift possible
        target = self.current_pan_pos + pan_delta
        target = max(PAN_MIN_STEPS, min(PAN_MAX_STEPS, target))
        if abs(target - self.current_pan_pos) < 1.0:
            print(f"[PAN] Limit at X={self.current_pan_pos:.1f}")
            return

        cmd = f"$J=G90 X{target:.2f} F{int(pan_speed)}\n"
        self.ser_p.write(cmd.encode())

        self.current_pan_pos = target
        self.last_command_time = time.time()
        print(f"[PAN] Jog -> target={target:.1f}  actual_start={self.current_pan_pos:.1f}")

    def process_detection(self, detections):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        if ball:
            error_x = ball['center_x'] - CENTER_X
            print(f"[DETECT] center_x={ball['center_x']:.0f}  error_x={error_x:+.0f}")
            if abs(error_x) > DEADZONE_PAN:
                self.send_command(error_x * GAIN_P)
            else:
                print(f"[DETECT] In deadzone")

    def return_home(self):
        if self.ser_p:
            self.ser_p.write(b"\x85")           # Cancel any pending jog
            self.ser_p.write(b"G90 G0 X0\n")    # Return to home in absolute mode
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