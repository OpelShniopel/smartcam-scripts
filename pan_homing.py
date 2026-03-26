import time

def auto_home_precision(ser):
    # 1. Clear Alarms
    ser.write(b"$X\n")
    time.sleep(0.5)

    # ==========================================
    # PASS 1: THE FAST SEEK (Overshoot Expected)
    # ==========================================
    print("Fast Search for magnet...")
    ser.write(b"G91 G1 X250 F2500\n") 

    while True:
        ser.reset_input_buffer() # CRITICAL: Destroys old delayed messages
        ser.write(b"?\n")
        line = ser.readline().decode('utf-8')
        
        if "X" in line: 
            print("Rough target hit! Stopping...")
            ser.write(b"\x18") # Stop immediately
            time.sleep(1)
            ser.write(b"$X\n") 
            break
        time.sleep(0.01) # 100Hz polling is plenty fast
            
    # ==========================================
    # PASS 2: THE SLOW TAP (High Precision)
    # ==========================================
    print("Pulling off sensor...")
    ser.write(b"G91 G1 X-10 F1000\n")
    time.sleep(1.5) # Wait for pull-off to finish

    print("Slow precision tap...")
    ser.write(b"G91 G1 X15 F500\n") # Moving very slow

    while True:
        ser.reset_input_buffer() # Keep buffer completely clean
        ser.write(b"?\n")
        line = ser.readline().decode('utf-8')
        
        if "X" in line: 
            print("Exact Magnet Center Detected!")
            ser.write(b"\x18") 
            time.sleep(1)
            ser.write(b"$X\n") 
            break
        time.sleep(0.01)

    # ==========================================
    # FINAL CALIBRATION
    # ==========================================
    
    ser.write(b"G92 X180\n") # Lock in the exact 180 coordinate
    time.sleep(0.2)
    
    print("Moving to 0")
    ser.write(b"G90 G0 X0\n")

