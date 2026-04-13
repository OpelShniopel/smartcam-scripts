// tmc_control_v2.ino
// ESP32 + TMC2209 Pan Camera Controller — onboard P control
// Python sends pixel error; ESP32 computes velocity and decelerates naturally.
//
// Protocol (921600 baud):
//   X<error_px>[,<scale_pct>]\n  — ball offset from center in pixels (signed).
//                                   Optional scale 1-100 (default 100).
//                                   ESP32 computes velocity; motor slows as error→0.
//   L\n          — ball lost; decelerate to stop
//   ?\n          — query position → "P<steps>\n"
//   H\n          — homing → "OK\n" or "ERR:<reason>\n"
//   S\n          — immediate hard stop
//   G<steps>\n   — go to absolute position (blocking)
//   Z\n          — zero position counter
//   V<±sps>\n    — manual velocity override (bypass P control)
//   E\n / D\n    — enable / disable driver

#include <Arduino.h>

// ============================================================
//  PIN ASSIGNMENTS
// ============================================================
#define STEP_PIN    12
#define DIR_PIN     14
#define EN_PIN      13
#define LIMIT_PIN   27

// ============================================================
//  MOTOR CONSTANTS
// ============================================================
#define STEPS_PER_DEG    125

#define PAN_MAX_DEG       90
#define PAN_MIN_DEG      -35
#define PAN_MAX_STEPS    (PAN_MAX_DEG * STEPS_PER_DEG)    //  11250
#define PAN_MIN_STEPS    (PAN_MIN_DEG * STEPS_PER_DEG)    //  -4375

// Direction — flip between 0 and 1 to reverse motor direction
#define INVERT_DIR  1
#define DIR_FWD     (INVERT_DIR ? LOW  : HIGH)
#define DIR_REV     (INVERT_DIR ? HIGH : LOW)

// ============================================================
//  P CONTROLLER CONSTANTS  (tune these)
// ============================================================
#define FRAME_W          1280     // must match Python / inference resolution
#define DEADZONE_PX        50     // pixels — no movement inside this window

#define SPEED_FACTOR      3.5f    // exponent of speed curve (higher = more exponential)
#define MIN_VEL_SPS        100    // steps/sec at minimum error outside deadzone
#define MAX_VEL_SPS       7000    // steps/sec at full-frame error
#define HOMING_VEL_SPS    2000

#define BALL_BOOST_THR      20    // px/frame — above this, apply velocity boost
#define BALL_BOOST_GAIN    1.7f   // boost multiplier coefficient
#define BALL_TIMEOUT_MS    300    // ms without X update before auto-stop

// ============================================================
//  MOTION CONSTANTS
// ============================================================
#define ACCEL_PER_MS      120     // steps/sec per ms ramp rate

// ============================================================
//  SERIAL
// ============================================================
#define CMD_BAUD        921600

// ============================================================
//  GLOBALS
// ============================================================
hw_timer_t* stepTimer = NULL;

volatile int32_t stepPos    = 0;
volatile int32_t runningVel = 0;
volatile bool    stepPhase  = false;
volatile int32_t targetVel  = 0;

uint32_t lastRampMs  = 0;
uint32_t lastXMs     = 0;      // timestamp of last X command
int32_t  lastErrorPx = 0;      // previous frame error for velocity boost
bool     ballActive  = false;  // true while receiving X updates

// ============================================================
//  STEP ISR
// ============================================================
void IRAM_ATTR onStep() {
    if (runningVel == 0) {
        gpio_set_level((gpio_num_t)STEP_PIN, 0);
        stepPhase = false;
        return;
    }
    stepPhase = !stepPhase;
    gpio_set_level((gpio_num_t)STEP_PIN, stepPhase ? 1 : 0);
    if (stepPhase) {
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
//  VELOCITY RAMP  — 1ms tick
// ============================================================
void updateVelocity() {
    uint32_t now = millis();
    if (now - lastRampMs < 1) return;
    lastRampMs = now;

    // Auto-stop if ball updates stopped arriving
    if (ballActive && (now - lastXMs > BALL_TIMEOUT_MS)) {
        ballActive = false;
        targetVel  = 0;
    }

    int32_t tv = targetVel;
    int32_t cv = runningVel;
    if (cv == tv) return;

    int32_t newVel;
    if (cv < tv) newVel = min(cv + (int32_t)ACCEL_PER_MS, tv);
    else          newVel = max(cv - (int32_t)ACCEL_PER_MS, tv);

    if ((cv > 0 && newVel < 0) || (cv < 0 && newVel > 0)) newVel = 0;

    if (newVel > 0 && stepPos >= PAN_MAX_STEPS) newVel = 0;
    if (newVel < 0 && stepPos <= PAN_MIN_STEPS) newVel = 0;

    if (newVel == cv) return;

    if      (newVel > 0) gpio_set_level((gpio_num_t)DIR_PIN, DIR_FWD);
    else if (newVel < 0) gpio_set_level((gpio_num_t)DIR_PIN, DIR_REV);

    runningVel = newVel;
    setStepFreq(newVel);
}

// ============================================================
//  P CONTROLLER  — called on every X command
//  Returns target velocity in steps/sec (signed).
// ============================================================
int32_t computeVelocity(int32_t error_px, float scale) {
    if (abs(error_px) <= DEADZONE_PX) return 0;

    float normalized   = min(1.0f, (float)abs(error_px) / (FRAME_W / 2.0f));
    float base_factor  = powf(normalized, SPEED_FACTOR);

    // Velocity boost when ball is moving fast across frame
    int32_t ball_vel   = abs(error_px - lastErrorPx);
    float multiplier   = 1.0f;
    if (ball_vel > BALL_BOOST_THR)
        multiplier += (ball_vel / 100.0f) * BALL_BOOST_GAIN;

    float speed = MIN_VEL_SPS + (MAX_VEL_SPS - MIN_VEL_SPS) * base_factor;
    speed = min((float)MAX_VEL_SPS, speed * multiplier * scale);

    return (error_px > 0) ? (int32_t)speed : -(int32_t)speed;
}

// ============================================================
//  HOMING
// ============================================================
void doHoming() {
    Serial.println("HOMING");
    gpio_set_level((gpio_num_t)EN_PIN, LOW);
    delay(50);

    Serial.println("HOMING:SEEK");
    gpio_set_level((gpio_num_t)DIR_PIN, DIR_FWD);
    runningVel = HOMING_VEL_SPS;
    setStepFreq(HOMING_VEL_SPS);

    uint32_t timeout = millis() + 15000;
    while (digitalRead(LIMIT_PIN) == HIGH) {
        if (millis() > timeout) { hardStop(); Serial.println("ERR:SEEK_TIMEOUT"); return; }
        delay(1);
    }
    hardStop();
    delay(300);

    Serial.println("HOMING:BACKOFF");
    int32_t backoffSteps = 5 * STEPS_PER_DEG;
    int32_t startPos = stepPos;
    gpio_set_level((gpio_num_t)DIR_PIN, DIR_REV);
    runningVel = -(MAX_VEL_SPS / 4);
    setStepFreq(MAX_VEL_SPS / 4);
    while (abs(stepPos - startPos) < backoffSteps) delay(1);
    hardStop();
    delay(300);

    Serial.println("HOMING:TAP");
    int32_t tapVel = MAX_VEL_SPS / 10;
    gpio_set_level((gpio_num_t)DIR_PIN, DIR_FWD);
    runningVel = tapVel;
    setStepFreq(tapVel);

    timeout = millis() + 8000;
    while (digitalRead(LIMIT_PIN) == HIGH) {
        if (millis() > timeout) { hardStop(); Serial.println("ERR:TAP_TIMEOUT"); return; }
        delay(1);
    }
    hardStop();
    delay(150);

    stepPos = PAN_MAX_STEPS;
    Serial.println("HOMING:CENTER");
    gpio_set_level((gpio_num_t)DIR_PIN, DIR_REV);
    runningVel = -(MAX_VEL_SPS / 2);
    setStepFreq(MAX_VEL_SPS / 2);
    while (stepPos > 0) delay(1);
    hardStop();

    stepPos = 0;
    Serial.println("OK");
}

// ============================================================
//  GO TO ABSOLUTE POSITION
// ============================================================
void gotoPos(int32_t target) {
    target = constrain(target, PAN_MIN_STEPS, PAN_MAX_STEPS);
    int32_t moveVel  = (target > stepPos) ? MAX_VEL_SPS / 2 : -(MAX_VEL_SPS / 2);
    int32_t slowZone = STEPS_PER_DEG * 5;

    if (moveVel > 0) gpio_set_level((gpio_num_t)DIR_PIN, DIR_FWD);
    else             gpio_set_level((gpio_num_t)DIR_PIN, DIR_REV);
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
            if (runningVel != sv_signed) { runningVel = sv_signed; setStepFreq(sv_signed); }
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

    if (c == 'X') {
        // X<error_px>  or  X<error_px>,<scale_pct>
        int32_t error_px = 0;
        float   scale    = 1.0f;

        int comma = cmd.indexOf(',');
        if (comma > 0) {
            error_px = (int32_t)cmd.substring(1, comma).toInt();
            int32_t pct = cmd.substring(comma + 1).toInt();
            scale = constrain(pct, 1, 100) / 100.0f;
        } else {
            error_px = (int32_t)cmd.substring(1).toInt();
        }

        lastXMs    = millis();
        ballActive = true;
        targetVel  = computeVelocity(error_px, scale);
        lastErrorPx = error_px;

    } else if (c == 'L') {
        // Ball lost — decelerate gracefully via ramp
        ballActive = false;
        targetVel  = 0;

    } else if (c == 'V') {
        // Manual velocity override
        ballActive = false;
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
        ballActive = false;
        hardStop();

    } else if (c == 'G') {
        ballActive = false;
        gotoPos((int32_t)cmd.substring(1).toInt());

    } else if (c == 'Z') {
        stepPos = 0;

    } else if (c == 'E') {
        gpio_set_level((gpio_num_t)EN_PIN, LOW);

    } else if (c == 'D') {
        hardStop();
        ballActive = false;
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

    gpio_set_level((gpio_num_t)EN_PIN,   LOW);
    gpio_set_level((gpio_num_t)STEP_PIN, LOW);
    gpio_set_level((gpio_num_t)DIR_PIN,  DIR_FWD);

    stepTimer = timerBegin(1000000);
    timerAttachInterrupt(stepTimer, &onStep);
    timerStop(stepTimer);

    lastRampMs = millis();
    Serial.println("READY");
}

// ============================================================
//  MAIN LOOP
// ============================================================
void loop() {
    updateVelocity();

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
