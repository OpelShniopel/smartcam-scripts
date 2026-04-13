# scf4_tools.py

import serial
import time

def send_command(ser, cmd, echo=False):
    ser.write(bytes(cmd+"\n", 'utf8'))
    data_in = ser.readline().decode('utf-8').strip()
    if echo:
        print("> "+cmd)
        print("< "+data_in)
        print("")
    return data_in

# Status returns 9 arguments. Internal position counter, PI status and movement status
def parse_status(status_string):
    temp = status_string.split(",")
    ret = []
    for t in temp:
        ret.append(int(t.strip()))
    return ret

def wait_homing(ser, initial_status, axis, timeout_sec=10.0):
    start_time = time.time()
    
    while True:
        # Check if the timeout has been reached
        if time.time() - start_time > timeout_sec:
            print(f"TIMEOUT WARNING: Axis status (index {axis}) did not change within {timeout_sec} seconds. Exiting wait.")
            # Optional: If you know the command to halt the motors immediately, 
            # you can uncomment the next line and add the command (e.g., '!')
            # send_command(ser, "!") 
            break
            
        status_str = send_command(ser, "!1")
        status = parse_status(status_str)
        
        # Break the loop if the status has successfully changed
        if initial_status != status[axis]:
            break
            
        time.sleep(0.01)
        
    time.sleep(0.1)