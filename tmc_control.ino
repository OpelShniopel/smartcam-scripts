// tmc_control.ino
// ESP32 + TMC2209 Pan Camera Controller (STEP/DIR mode, no UART)
#include <Arduino.h>
//
// Protocol (250000 baud, USB Serial):
//   V<±sps>\n  — set velocity in steps/sec (positive=right, negative=left)
//   ?\n        — query position → "P<steps>\n"
//   H\n        — run homing → "OK\n" or "ERR:<reason>\n"
//   S\n        — immediate stop (no ramp)
//   G<steps>\n — go to absolute step position (blocking, for return_home)
//   Z\n        — zero position counter at current location

// ============================================================
//  PIN ASSIGNMENTS
// ============================================================
#define STEP_PIN    12
#define DIR_PIN     14
#define EN_PIN      13   // TMC2209 active LOW: LOW=enabled, HIGH=disabled
#define LIMIT_PIN   27   // Limit switch – Active LOW with internal pull-up
                         // (not wired yet — homing disabled by default)

// ============================================================
//  MOTOR CONSTANTS
//  5000 steps = 40°  →  125 steps/degree
// ============================================================
#define STEPS_PER_DEG    125

#define PAN_MAX_DEG       90    // +90° right  (toward limit switch)
#define PAN_MIN_DEG      -35    // -35° left

#define PAN_MAX_STEPS    ( PAN_MAX_DEG * STEPS_PER_DEG)   //  11250
#define PAN_MIN_STEPS    ( PAN_MIN_DEG * STEPS_PER_DEG)   //  -4375

// Velocity
#define MAX_VEL_SPS     5000   // steps/sec (~40°/sec)
#define MIN_VEL_SPS      200   // floor — below this treated as zero

// Acceleration: steps/sec added per 1ms tick
// 80 → 0 to 5000 in ~62ms — snappy but not jarring
#define ACCEL_PER_MS      80

// Serial
#define CMD_BAUD        921600

// ============================================================
//  GLOBALS
// ============================================================
hw_timer_t* stepTimer = NULL;

volatile int32_t stepPos    = 0;   // absolute step counter
volatile int32_t runningVel = 0;   // velocity currently being output (steps/sec, signed)
volatile bool    stepPhase  = false;

volatile int32_t targetVel  = 0;   // velocity commanded by Python

uint32_t lastRampMs = 0;

// ============================================================
//  STEP ISR — fires at 2× step frequency, toggles STEP pin
// ============================================================
void IRAM_ATTR onStep() {
    if (runningVel == 0) {
        gpio_set_level((gpio_num_t)STEP_PIN, 0);
        stepPhase = false;
        return;
    }

    stepPhase = !stepPhase;
    gpio_set_level((gpio_num_t)STEP_PIN, stepPhase ? 1 : 0);

    if (stepPhase) {   // count on rising edge
        if (runningVel > 0) stepPos++;
        else                stepPos--;
    }
}

// ============================================================
//  SET STEP FREQUENCY
// ============================================================
void setStepFreq(int32_t velSPS) {
    if (velSPS == 0) {
        timerStop(stepTimer);
        gpio_set_level((gpio_num_t)STEP_PIN, 0);
        stepPhase = false;
    } else {
        // Half-period in timer ticks (1 MHz base) = 500 000 / |vel|
        uint32_t halfUs = 500000UL / (uint32_t)abs(velSPS);
        if (halfUs < 20) halfUs = 20;   // cap at 25 kHz edge = 12.5 kHz step rate
        timerAlarm(stepTimer, halfUs, true, 0);
        timerStart(stepTimer);
    }
}

// ============================================================
//  HARD STOP
// ============================================================
void hardStop() {
    targetVel  = 0;
    runningVel = 0;
    setStepFreq(0);
}

// ============================================================
//  VELOCITY RAMP — call every loop iteration, acts every 1ms
// ============================================================
void updateVelocity() {
    uint32_t now = millis();
    if (now - lastRampMs < 1) return;
    lastRampMs = now;

    int32_t tv = targetVel;
    int32_t cv = runningVel;
    if (cv == tv) return;

    int32_t newVel;
    if (cv < tv) newVel = min(cv + (int32_t)ACCEL_PER_MS, tv);
    else          newVel = max(cv - (int32_t)ACCEL_PER_MS, tv);

    // Direction change must pass through zero — no reversals without stopping
    if ((cv > 0 && newVel < 0) || (cv < 0 && newVel > 0)) newVel = 0;

    // Enforce hard positional limits
    if (newVel > 0 && stepPos >= PAN_MAX_STEPS) newVel = 0;
    if (newVel < 0 && stepPos <= PAN_MIN_STEPS) newVel = 0;

    if (newVel == cv) return;

    // Set direction pin before changing step frequency
    if      (newVel > 0) gpio_set_level((gpio_num_t)DIR_PIN, HIGH);
    else if (newVel < 0) gpio_set_level((gpio_num_t)DIR_PIN, LOW);

    runningVel = newVel;
    setStepFreq(newVel);
}

// ============================================================
//  HOMING  (requires limit switch on LIMIT_PIN)
//  Layout: limit switch at the +90° right end.
//  After homing, center = stepPos 0.
// ============================================================
void doHoming() {
    Serial.println("HOMING");
    gpio_set_level((gpio_num_t)EN_PIN, LOW);
    delay(50);

    // --- Fast seek rightward until limit triggers ---
    Serial.println("HOMING:SEEK");
    gpio_set_level((gpio_num_t)DIR_PIN, HIGH);
    runningVel = MAX_VEL_SPS;
    setStepFreq(MAX_VEL_SPS);

    uint32_t timeout = millis() + 15000;
    while (digitalRead(LIMIT_PIN) == HIGH) {
        if (millis() > timeout) { hardStop(); Serial.println("ERR:SEEK_TIMEOUT"); return; }
        delay(1);
    }
    hardStop();
    delay(300);

    // --- Back off 5° ---
    Serial.println("HOMING:BACKOFF");
    int32_t backoffSteps = 5 * STEPS_PER_DEG;
    int32_t startPos = stepPos;
    gpio_set_level((gpio_num_t)DIR_PIN, LOW);
    runningVel = -(MAX_VEL_SPS / 4);
    setStepFreq(MAX_VEL_SPS / 4);
    while (abs(stepPos - startPos) < backoffSteps) delay(1);
    hardStop();
    delay(300);

    // --- Slow precision tap back to switch ---
    Serial.println("HOMING:TAP");
    int32_t tapVel = MAX_VEL_SPS / 10;
    gpio_set_level((gpio_num_t)DIR_PIN, HIGH);
    runningVel = tapVel;
    setStepFreq(tapVel);

    timeout = millis() + 8000;
    while (digitalRead(LIMIT_PIN) == HIGH) {
        if (millis() > timeout) { hardStop(); Serial.println("ERR:TAP_TIMEOUT"); return; }
        delay(1);
    }
    hardStop();
    delay(150);

    // At limit = PAN_MAX_STEPS from center; drive left to home (stepPos = 0)
    stepPos = PAN_MAX_STEPS;
    Serial.println("HOMING:CENTER");
    gpio_set_level((gpio_num_t)DIR_PIN, LOW);
    runningVel = -(MAX_VEL_SPS / 2);
    setStepFreq(MAX_VEL_SPS / 2);
    while (stepPos > 0) delay(1);
    hardStop();

    stepPos = 0;
    Serial.println("OK");
}

// ============================================================
//  GO TO ABSOLUTE POSITION  (blocking)
// ============================================================
void gotoPos(int32_t target) {
    target = constrain(target, PAN_MIN_STEPS, PAN_MAX_STEPS);
    int32_t moveVel = (target > stepPos) ? MAX_VEL_SPS / 2 : -(MAX_VEL_SPS / 2);
    int32_t slowZone = STEPS_PER_DEG * 5;   // slow within 5°

    if (moveVel > 0) gpio_set_level((gpio_num_t)DIR_PIN, HIGH);
    else             gpio_set_level((gpio_num_t)DIR_PIN, LOW);
    runningVel = moveVel;
    setStepFreq(moveVel);

    uint32_t timeout = millis() + 20000;
    while (millis() < timeout) {
        int32_t remaining = abs(stepPos - target);
        if (remaining <= 3) break;
        if (remaining < slowZone) {
            int32_t sv = max((int32_t)(MIN_VEL_SPS * 2),
                             (int32_t)(abs(moveVel) * remaining / slowZone));
            int32_t sv_signed = (moveVel > 0) ? sv : -sv;
            if (runningVel != sv_signed) {
                runningVel = sv_signed;
                setStepFreq(sv_signed);
            }
        }
        delay(1);
    }
    hardStop();
    delay(50);
    Serial.println("OK");
}

// ============================================================
//  SERIAL COMMAND PARSER
// ============================================================
String cmdBuf = "";

void parseCommand(const String& cmd) {
    if (cmd.length() == 0) return;
    char c = cmd.charAt(0);

    if (c == 'V') {
        int32_t vel = (int32_t)cmd.substring(1).toInt();
        vel = constrain(vel, -MAX_VEL_SPS, MAX_VEL_SPS);
        if (vel > 0 && stepPos >= PAN_MAX_STEPS) vel = 0;
        if (vel < 0 && stepPos <= PAN_MIN_STEPS) vel = 0;
        targetVel = vel;

    } else if (c == '?') {
        Serial.print('P');
        Serial.println(stepPos);

    } else if (c == 'H') {
        doHoming();

    } else if (c == 'S') {
        hardStop();

    } else if (c == 'G') {
        gotoPos((int32_t)cmd.substring(1).toInt());

    } else if (c == 'Z') {
        stepPos = 0;

    } else if (c == 'E') {
        gpio_set_level((gpio_num_t)EN_PIN, LOW);

    } else if (c == 'D') {
        hardStop();
        delay(50);
        gpio_set_level((gpio_num_t)EN_PIN, HIGH);
    }
}

// ============================================================
//  SETUP
// ============================================================
void setup() {
    Serial.begin(CMD_BAUD);
    delay(100);

    pinMode(STEP_PIN,  OUTPUT);
    pinMode(DIR_PIN,   OUTPUT);
    pinMode(EN_PIN,    OUTPUT);
    pinMode(LIMIT_PIN, INPUT_PULLUP);

    gpio_set_level((gpio_num_t)EN_PIN,   LOW);   // enable driver
    gpio_set_level((gpio_num_t)STEP_PIN, LOW);
    gpio_set_level((gpio_num_t)DIR_PIN,  HIGH);

    // Hardware timer at 1 MHz (core v3.x API)
    stepTimer = timerBegin(1000000);
    timerAttachInterrupt(stepTimer, &onStep);
    timerStop(stepTimer);   // stays stopped until first V command

    lastRampMs = millis();
    Serial.println("READY");
}

// ============================================================
//  MAIN LOOP
// ============================================================
void loop() {
    updateVelocity();

    // Continuously enforce limits
    if (stepPos >= PAN_MAX_STEPS && targetVel > 0) targetVel = 0;
    if (stepPos <= PAN_MIN_STEPS && targetVel < 0) targetVel = 0;

    while (Serial.available()) {
        char ch = (char)Serial.read();
        if (ch == '\n' || ch == '\r') {
            cmdBuf.trim();
            if (cmdBuf.length() > 0) {
                parseCommand(cmdBuf);
                cmdBuf = "";
            }
        } else {
            if (cmdBuf.length() < 32) cmdBuf += ch;
        }
    }
}
