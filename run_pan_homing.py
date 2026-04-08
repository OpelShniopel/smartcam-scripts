import serial
import time
import argparse
import pan_homing

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--port', default="COM7", help='Serial port')
args = parser.parse_args()

ser = serial.Serial(args.port, 115200, timeout=0.1)
time.sleep(2)
ser.flushInput()

pan_homing.auto_home_precision(ser)

ser.close()
print("Done.")
