import json
import socket
import serial
import time
import sys
import atexit
import signal
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
        self.last_error_x = 0.0
        self.pending_oks = 0

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
            DEBUG and print(f"[PAN] Position query failed: {e}")
        return self.current_pan_pos  # fallback if query fails

    def _stop_jog(self):
        """Cancel any pending jog and sync position from the board."""
        if not self.jogging:
            return
        self.ser_p.write(b"\x85")
        time.sleep(0.05)                            # wait for GRBL to flush and return to Idle
        self.ser_p.reset_input_buffer()             # discard any leftover 'ok' responses
        self.current_pan_pos = self._get_position()
        self.jogging = False
        DEBUG and print(f"[PAN] Stopped at X={self.current_pan_pos:.1f}")

    def send_command(self, error_x):
        if not self.ser_p:
            return

        # 1. Clear the 'ok' responses to stay in sync
        while self.ser_p.in_waiting > 0:
            resp = self.ser_p.readline().decode().strip()
            if 'ok' in resp:
                self.pending_oks = max(0, self.pending_oks - 1)

        # 2. Buffer Management: Allow 3 commands to stay queued
        # This creates a "wedge" of data so the motor never runs out of instructions
        if self.pending_oks >= 3:
            return

        # 3. Aggressive Curve
        max_possible_error = FRAME_W / 2
        normalized_error = min(1.0, abs(error_x) / max_possible_error)
        curved_factor = pow(normalized_error, 2.2) # Slightly more aggressive
        speed = MIN_PAN_SPEED + (MAX_PAN_SPEED - MIN_PAN_SPEED) * curved_factor

        # 4. INCREASED LOOK-AHEAD (3.0x instead of 1.5x)
        # This is the secret to removing the "twitch"
        step_duration = COMMAND_DT * 3.0 
        step = (speed / 60.0) * step_duration * (1 if error_x > 0 else -1)

        target = max(PAN_MIN_STEPS, min(PAN_MAX_STEPS, self.current_pan_pos + step))
        actual_step = target - self.current_pan_pos
        
        # 5. Ignore "Micro-twitches" that the motor can't physically do smoothly
        if abs(actual_step) < 0.4: 
            return

        cmd = f"$J=G91 X{actual_step:.3f} F{int(speed)}\n"
        self.ser_p.write(cmd.encode())
        
        # Important: Don't update current_pan_pos by the FULL 'step' 
        # only update it by what we expect to cover in ONE COMMAND_DT
        # this keeps the 'target' from drifting too far ahead of reality
        self.current_pan_pos += (speed / 60.0) * COMMAND_DT * (1 if error_x > 0 else -1)
        
        self.jogging = True
        self.pending_oks += 1

    def process_detection(self, detections):
        ball = next((d for d in detections if d['class'] == 'BALL'), None)
        if not ball:
            self._stop_jog()
            return

        error_x = ball['center_x'] - CENTER_X
        DEBUG and print(f"[DETECT] center_x={ball['center_x']:.0f}  error_x={error_x:+.0f}")

        if abs(error_x) <= DEADZONE_PAN:
            DEBUG and print(f"[DETECT] In deadzone")
            self._stop_jog()
        else:
            reversed = self.last_error_x != 0.0 and (error_x > 0) != (self.last_error_x > 0)
            if reversed:
                DEBUG and print(f"[DETECT] Direction reversal, cancelling jog")
                self._stop_jog()
            self.send_command(error_x)

        self.last_error_x = error_x

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