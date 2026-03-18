import time
import serial
import argparse
import sys
from pynput.keyboard import Key, Listener

current_pos = 0
MAX_LIMIT = 90
MIN_LIMIT = -90
STEP_SIZE = 10  # Degrees per key press

parser = argparse.ArgumentParser()
parser.add_argument('-p','--port', help='COM port', required=True)
parser.add_argument('-f','--file', help='File name', required=True)
args = parser.parse_args()

ser = serial.Serial(args.port, 115200, timeout=1)
time.sleep(2) # Give the board time to initialize after opening serial
ser.flushInput()

def is_idle(ser):
    ser.write(b'?\n')
    while True:
        line = ser.readline().decode('utf8').strip()
        if line.startswith('<'):
            # \r moves cursor to start of line, end='' prevents newline
            print(f"\rStatus: {line.ljust(50)}", end='') 
            sys.stdout.flush() # Force update the console
            return "Idle" in line
        
def commands_from_file():
    with open(args.file, 'r') as fp:
        for line in fp:
            line = line.strip()
            if not line: continue
            
            ser.write((line + '\n').encode('utf8'))
            print(f"\nSent: {line} | Response: {ser.readline().decode('utf8').strip()}")

            while not is_idle(ser):
                time.sleep(0.05)
            
            time.sleep(0.1) # Small pause to prevent buffer overflow

        print("\nAll commands sent. Exiting...")

def move_motor(amount):
    global current_pos
    new_pos = current_pos + amount
    
    if MIN_LIMIT <= new_pos <= MAX_LIMIT:
        current_pos = new_pos
        ser.write(f"G91\nG1 X{amount} F1000\n".encode('utf8')) # Example G-code
        while not is_idle(ser):
            time.sleep(0.05)
        print(f"Moving {amount}° | Current Position: {current_pos}°")
    else:
        print(f"Limit reached! Cannot move to {new_pos}°")

def commands_from_keyboard(key):

    if key == Key.left:
        move_motor(-STEP_SIZE)
        
    if key == Key.right:
        move_motor(STEP_SIZE)

    if hasattr(key, 'char') and key.char == 'q':
        if current_pos != 0:
            return_move = -current_pos
            print(f"\nReturning to center... Moving {return_move}°")
            ser.write(f"G91\nG1 X{return_move} F1000\n".encode('utf8'))
        print("Safe exit. Position centered.")
        return False
             
try:
    #commands_from_file()

    with Listener(on_press = commands_from_keyboard) as listener:
        listener.join()

finally:
    # This block executes regardless of success or errors
    if ser.is_open:
        ser.flush()      # Ensure all outgoing data is sent
        ser.close()      # Safely close the port
        print("\nSerial port closed.")