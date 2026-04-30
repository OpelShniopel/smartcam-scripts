import json
import os
import socket
import time
import sys
import atexit
import signal
from pan_control_esp_fixed import PanController
from zoom_control_fixed import ZoomController
import threading

_cleanup_done = False
_cleanup_lock = threading.Lock()
UNIX_SOCK   = "/tmp/ptz_control.sock"
MANUAL_SOCK = "/tmp/ptz_manual.sock"
TARGET_CAM  = "fixed"
DEBUG       = False
ENABLE_PAN  = True
ENABLE_ZOOM = True

STEP_SIZE_STEPS   = 62    # motor steps per Go "step" (0.5° at 125 steps/°)
JOG_STEPS_PER_SPS = 62    # motor steps/sec per Go stepsPerSecond unit
STEP_TIMEOUT_S    = 5.0   # max seconds to wait for G command OK response

ZOOM_STEP_SIZE    = 100   # zoom_pos units per Go "step"
ZOOM_JOG_INTERVAL = 0.1   # seconds between jog ticks
ZOOM_JOG_PER_SPS  = 20    # zoom_pos units added per tick per stepsPerSecond unit


class PTZController:
    def __init__(self):
        self.pan  = PanController()  if ENABLE_PAN  else None
        self.zoom = ZoomController() if ENABLE_ZOOM else None

        if not self.pan and not self.zoom:
            print("WARNING: Both pan and zoom are disabled.")

        self._manual_mode = False
        self._manual_lock = threading.Lock()
        self._zoom_jog_active = False

    def process_detection(self, detections):
        if self._manual_mode:
            return
        speed_scale = self.zoom.get_pan_speed_factor() if (self.zoom and hasattr(self.zoom, 'ser_z') and self.zoom.ser_z) else 1.0
        if self.pan:
            self.pan.process_detection(detections, speed_scale=speed_scale)
        if self.zoom:
            pan_error_x = self.pan.last_error_x if self.pan else 0.0
            self.zoom.process_detection(detections, pan_error_x=pan_error_x)

    def _send_stop(self):
        if self.pan and self.pan.ser_p:
            try:
                self.pan.ser_p.write(b"L\n")
            except OSError:
                pass

    def manual_pan_step(self, direction, steps=1):
        if not self._manual_mode:
            print("[ptz] pan_step ignored: not in manual mode")
            return
        with self._manual_lock:
            if not self.pan or not self.pan.ser_p:
                return
            ser = self.pan.ser_p
            ser.write(b"S\n")
            time.sleep(0.05)
            ser.reset_input_buffer()
            ser.write(b"?\n")
            raw = ser.readline().decode("utf-8", errors="ignore").strip()
            try:
                current = int(raw[1:]) if raw.startswith("P") else 0
            except ValueError:
                current = 0
            sign   = 1 if direction == "right" else -1
            target = current + sign * steps * STEP_SIZE_STEPS
            ser.write(f"G{target}\n".encode())
            deadline = time.time() + STEP_TIMEOUT_S
            while time.time() < deadline:
                resp = ser.readline().decode("utf-8", errors="ignore").strip()
                if resp == "OK":
                    break

    def manual_move_start(self, direction, steps_per_second=10):
        if not self._manual_mode:
            print("[ptz] move_start ignored: not in manual mode")
            return
        with self._manual_lock:
            if not self.pan or not self.pan.ser_p:
                return
            sign = 1 if direction == "right" else -1
            vel  = steps_per_second * JOG_STEPS_PER_SPS
            self.pan.ser_p.write(f"V{sign * vel}\n".encode())

    def manual_move_stop(self):
        if not self._manual_mode:
            return
        with self._manual_lock:
            self._send_stop()
            # mode stays manual — only set_mode changes the mode

    def manual_zoom_step(self, direction, steps=1):
        if not self._manual_mode:
            print("[ptz] zoom_step ignored: not in manual mode")
            return
        with self._manual_lock:
            if not self.zoom or not self.zoom.ser_z:
                return
            sign = -1 if direction == "in" else 1
            self.zoom.send_zoom(sign * steps * ZOOM_STEP_SIZE)

    def manual_zoom_start(self, direction, steps_per_second=10):
        if not self._manual_mode:
            print("[ptz] zoom_start ignored: not in manual mode")
            return
        self._zoom_jog_active = False  # stop any existing jog
        with self._manual_lock:
            if not self.zoom or not self.zoom.ser_z:
                return
            sign = -1 if direction == "in" else 1
            delta = sign * steps_per_second * ZOOM_JOG_PER_SPS
            self._zoom_jog_active = True

            def _jog():
                while self._zoom_jog_active and self._manual_mode:
                    self.zoom.send_zoom(delta)
                    time.sleep(ZOOM_JOG_INTERVAL)

            threading.Thread(target=_jog, daemon=True, name="ptz-zoom-jog").start()

    def manual_zoom_stop(self):
        if not self._manual_mode:
            return
        self._zoom_jog_active = False

    def set_mode(self, mode):
        if mode == "manual":
            self._manual_mode = True   # set FIRST — detection thread sees this immediately
            with self._manual_lock:
                if self.pan and self.pan.ser_p:
                    try:
                        self.pan.ser_p.write(b"S\n")  # hard stop
                    except OSError:
                        pass
            print("[ptz] mode -> manual")
        else:
            self._zoom_jog_active = False
            with self._manual_lock:
                self._send_stop()
            self._manual_mode = False  # clear AFTER — detection resumes on next frame
            print("[ptz] mode -> automatic")

    def process_manual_command(self, msg):
        cmd = msg.get("type")
        if cmd == "pan_step":
            direction = msg.get("direction", "right")
            steps = max(1, int(msg.get("steps", 1)))
            self.manual_pan_step(direction, steps)
        elif cmd == "move_start":
            direction = msg.get("direction", "right")
            sps = max(1, int(msg.get("steps_per_second", 10)))
            self.manual_move_start(direction, sps)
        elif cmd == "move_stop":
            self.manual_move_stop()
        elif cmd == "set_mode":
            self.set_mode(msg.get("mode", "automatic"))
        elif cmd == "zoom_step":
            direction = msg.get("direction", "out")
            steps = max(1, int(msg.get("steps", 1)))
            self.manual_zoom_step(direction, steps)
        elif cmd == "zoom_start":
            direction = msg.get("direction", "out")
            sps = max(1, int(msg.get("steps_per_second", 10)))
            self.manual_zoom_start(direction, sps)
        elif cmd == "zoom_stop":
            self.manual_zoom_stop()

    def return_home(self):
        self._send_stop()
        self._manual_mode = False
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
        try:
            os.unlink(MANUAL_SOCK)
        except FileNotFoundError:
            pass


def manual_socket_server(controller):
    try:
        os.unlink(MANUAL_SOCK)
    except FileNotFoundError:
        pass
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(MANUAL_SOCK)
    os.chmod(MANUAL_SOCK, 0o660)
    srv.listen(2)
    print(f"PTZ manual socket -> {MANUAL_SOCK}")

    def _handle_conn(conn):
        buf = b""
        try:
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(msg, dict):
                        controller.process_manual_command(msg)
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _accept_loop():
        while True:
            try:
                conn, _ = srv.accept()
                threading.Thread(target=_handle_conn, args=(conn,), daemon=True,
                                 name="ptz-manual-conn").start()
            except OSError:
                break

    threading.Thread(target=_accept_loop, daemon=True, name="ptz-manual-accept").start()


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

    manual_socket_server(motor_ctrl)

    try:
        socket_listener(motor_ctrl)
    except KeyboardInterrupt:
        motor_ctrl.cleanup()
        sys.exit(0)
