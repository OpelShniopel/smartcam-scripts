// tmc_control.ino — ESP32 + TMC2209 Pan Camera Controller
//
// Supports two control modes, switchable at runtime via C command:
//   C0  — PTZ mode      : velocity P-control  (pixel error → motor velocity)
//   C1  — STATIONARY    : position P-control  (pixel error → step target, 1ms loop)
//
// Protocol (460800 baud):
//   X<error_px>[,<scale_pct>]\n  — ball offset from frame centre (signed px)
//   C<0|1>\n                     — set control mode; replies "OK\n"
//   L\n    — ball lost; decelerate to stop
//   ?\n    — query position → "P<steps>\n"
//   H\n    — homing → "OK\n" or "ERR:<reason>\n"
//   S\n    — immediate hard stop
//   G<steps>\n — go to absolute step position (blocking)
//   Z\n    — zero position counter
//   V<±sps>\n  — manual velocity override (bypasses P control)
//   E\n / D\n  — enable / disable driver

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

#define PAN_MAX_DEG       95
#define PAN_MIN_DEG      -35
#define PAN_MAX_STEPS    (PAN_MAX_DEG * STEPS_PER_DEG)
#define PAN_MIN_STEPS    (PAN_MIN_DEG * STEPS_PER_DEG)

#define INVERT_DIR  1
#define DIR_FWD     (INVERT_DIR ? LOW  : HIGH)
#define DIR_REV     (INVERT_DIR ? HIGH : LOW)

// ============================================================
//  PTZ MODE TUNING  (C0 — velocity P-control, pixel space)
// ============================================================
#define PTZ_FRAME_HALF        640
#define PTZ_MIN_DEADZONE_PX   10
#define PTZ_MAX_DEADZONE_PX   60
#define PTZ_SPEED_FACTOR      2.0f
#define PTZ_MIN_VEL_SPS       100
#define PTZ_MAX_VEL_SPS       8000
#define PTZ_ACCEL_PER_MS      250
#define PTZ_BALL_TIMEOUT_MS   300
#define PTZ_BOOST_THR_PX      20
#define PTZ_BOOST_GAIN        4.0f
#define PTZ_HOMING_VEL_SPS    3000

// ============================================================
//  STATIONARY CAM MODE TUNING  (C1 — position P-control, step space)
// ============================================================
#define STAT_FRAME_HALF            640.0f
#define STAT_STEPS_PER_PX          17.6f   // tune: STEPS_PER_DEG * half_fov_deg / FRAME_HALF
#define STAT_MIN_DEADZONE_STEPS    100
#define STAT_MAX_DEADZONE_STEPS    200
#define STAT_MIN_VEL_SPS           400
#define STAT_MAX_VEL_SPS           12000
#define STAT_ACCEL_PER_MS          400
#define STAT_BALL_TIMEOUT_MS       1000
#define STAT_BOOST_THR_STEPS       100
#define STAT_BOOST_GAIN            5.0f
#define STAT_HOMING_VEL_SPS        3000

// ============================================================
//  CONTROL MODES
// ============================================================
#define MODE_PTZ         0
#define MODE_STATIONARY  1

// ============================================================
//  GLOBALS
// ============================================================
hw_timer_t* stepTimer = NULL;

volatile int32_t stepPos    = 0;
volatile int32_t runningVel = 0;
volatile bool    stepPhase  = false;
volatile int32_t targetVel  = 0;

uint8_t  ctrlMode  = MODE_PTZ;

// Position P-control state (MODE_STATIONARY)
int32_t  targetSteps     = 0;
int32_t  lastTargetSteps = 0;

// Velocity P-control state (MODE_PTZ)
int32_t  lastErrorPx = 0;

// Shared tracking state
bool     ballActive = false;
uint32_t lastXMs    = 0;
uint32_t lastRampMs = 0;

// Runtime vars — set on mode switch so hot paths need no branches
int32_t activeAccelPerMs    = PTZ_ACCEL_PER_MS;
int32_t activeBallTimeoutMs = PTZ_BALL_TIMEOUT_MS;

char    cmdBuf[32];
uint8_t bufIdx = 0;

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
        uint32_t halfUs = 500000UL / (uint32_t)abs(velSPS);
        if (halfUs < 20) halfUs = 20;
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
//  P CONTROLLER — MODE_PTZ
//  pixel error → velocity (set once per X command)
// ============================================================
int32_t computeVelocityPTZ(int32_t error_px, float scale) {
    float dynamic_dz = PTZ_MIN_DEADZONE_PX + (PTZ_MAX_DEADZONE_PX - PTZ_MIN_DEADZONE_PX) * (1.0f - scale);

    if (abs(error_px) <= (int32_t)dynamic_dz) return 0;

    float normalized  = min(1.0f, (float)abs(error_px) / PTZ_FRAME_HALF);
    float base_factor = powf(normalized, PTZ_SPEED_FACTOR);

    int32_t ball_vel  = abs(error_px - lastErrorPx);
    float multiplier  = 1.0f;
    if (ball_vel > PTZ_BOOST_THR_PX)
        multiplier += (ball_vel / 100.0f) * PTZ_BOOST_GAIN;

    float speed = PTZ_MIN_VEL_SPS + (PTZ_MAX_VEL_SPS - PTZ_MIN_VEL_SPS) * base_factor;
    speed = min((float)PTZ_MAX_VEL_SPS, speed * multiplier * scale);

    return (error_px > 0) ? (int32_t)speed : -(int32_t)speed;
}

// ============================================================
//  P CONTROLLER — MODE_STATIONARY
//  step error → velocity (re-run every 1ms from updateVelocity)
// ============================================================
int32_t computeVelocityStationary(int32_t target, float scale) {
    int32_t step_error = target - stepPos;

    float dynamic_dz = STAT_MIN_DEADZONE_STEPS
                     + (STAT_MAX_DEADZONE_STEPS - STAT_MIN_DEADZONE_STEPS) * (1.0f - scale);

    if (abs(step_error) <= (int32_t)dynamic_dz) return 0;

    float full_range  = STAT_STEPS_PER_PX * STAT_FRAME_HALF;
    float normalized  = min(1.0f, (float)abs(step_error) / full_range);

    int32_t target_delta = abs(target - lastTargetSteps);
    float multiplier = 1.0f;
    if (target_delta > STAT_BOOST_THR_STEPS)
        multiplier += (target_delta / full_range) * STAT_BOOST_GAIN;

    float speed = STAT_MIN_VEL_SPS + (STAT_MAX_VEL_SPS - STAT_MIN_VEL_SPS) * normalized;
    speed = min((float)STAT_MAX_VEL_SPS, speed * multiplier * scale);

    return (step_error > 0) ? (int32_t)speed : -(int32_t)speed;
}

// ============================================================
//  VELOCITY RAMP — 1ms tick
// ============================================================
void updateVelocity() {
    uint32_t now = millis();
    if (now - lastRampMs < 1) return;
    lastRampMs = now;

    if (ballActive && (now - lastXMs > (uint32_t)activeBallTimeoutMs)) {
        ballActive = false;
        if (ctrlMode == MODE_STATIONARY) targetSteps = stepPos;
        targetVel = 0;
    }

    // Position P loop runs continuously in STATIONARY mode
    if (ballActive && ctrlMode == MODE_STATIONARY) {
        targetVel = computeVelocityStationary(targetSteps, 1.0f);
    }

    if (runningVel > 0 && stepPos >= PAN_MAX_STEPS) { hardStop(); targetVel = 0; return; }
    if (runningVel < 0 && stepPos <= PAN_MIN_STEPS) { hardStop(); targetVel = 0; return; }

    int32_t tv = targetVel;
    int32_t cv = runningVel;
    if (cv == tv) return;

    int32_t newVel;
    if (cv < tv) newVel = min(cv + activeAccelPerMs, tv);
    else          newVel = max(cv - activeAccelPerMs, tv);

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
//  HOMING
// ============================================================
void doHoming() {
    int32_t homingVel = (ctrlMode == MODE_STATIONARY) ? STAT_HOMING_VEL_SPS : PTZ_HOMING_VEL_SPS;

    Serial.println("HOMING");
    gpio_set_level((gpio_num_t)EN_PIN, LOW);
    delay(50);

    Serial.println("HOMING:SEEK");
    gpio_set_level((gpio_num_t)DIR_PIN, DIR_FWD);
    runningVel = homingVel;
    setStepFreq(homingVel);

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
    int32_t backVel = (ctrlMode == MODE_STATIONARY) ? STAT_MAX_VEL_SPS : PTZ_MAX_VEL_SPS;
    runningVel = -(backVel / 4);
    setStepFreq(backVel / 4);
    while (abs(stepPos - startPos) < backoffSteps) delay(1);
    hardStop();
    delay(300);

    Serial.println("HOMING:TAP");
    int32_t tapVel = (ctrlMode == MODE_STATIONARY) ? STAT_MAX_VEL_SPS : PTZ_MAX_VEL_SPS;
    tapVel /= 10;
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

    int32_t maxVel = (ctrlMode == MODE_STATIONARY) ? STAT_MAX_VEL_SPS : PTZ_MAX_VEL_SPS;
    stepPos     = PAN_MAX_STEPS;
    targetSteps = PAN_MAX_STEPS;
    Serial.println("HOMING:CENTER");
    gpio_set_level((gpio_num_t)DIR_PIN, DIR_REV);
    runningVel = -(maxVel / 2);
    setStepFreq(maxVel / 2);
    while (stepPos > 0) delay(1);
    hardStop();

    stepPos     = 0;
    targetSteps = 0;
    Serial.println("OK");
}

// ============================================================
//  GO TO ABSOLUTE POSITION
// ============================================================
void gotoPos(int32_t target) {
    target = constrain(target, PAN_MIN_STEPS, PAN_MAX_STEPS);
    targetSteps = target;

    int32_t maxVel   = (ctrlMode == MODE_STATIONARY) ? STAT_MAX_VEL_SPS : PTZ_MAX_VEL_SPS;
    int32_t minVel   = (ctrlMode == MODE_STATIONARY) ? STAT_MIN_VEL_SPS : PTZ_MIN_VEL_SPS;
    int32_t moveVel  = (target > stepPos) ? maxVel / 2 : -(maxVel / 2);
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
            int32_t sv = max(minVel * 2,
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
//  COMMAND HANDLER
// ============================================================
void handleCommand(char* cmd) {
    if (strlen(cmd) == 0) return;
    char c = cmd[0];

    switch (c) {
        case 'X': {
            char* commaPos = strchr(cmd, ',');
            int32_t error_px;
            float scale = 1.0f;

            if (commaPos) {
                *commaPos = '\0';
                error_px = atoi(cmd + 1);
                scale = constrain(atoi(commaPos + 1), 1, 100) / 100.0f;
            } else {
                error_px = atoi(cmd + 1);
            }

            lastXMs    = millis();
            ballActive = true;

            if (ctrlMode == MODE_STATIONARY) {
                lastTargetSteps = targetSteps;
                targetSteps = constrain(
                    (int32_t)(error_px * STAT_STEPS_PER_PX),
                    PAN_MIN_STEPS, PAN_MAX_STEPS);
                targetVel = computeVelocityStationary(targetSteps, scale);
            } else {
                targetVel   = computeVelocityPTZ(error_px, scale);
                lastErrorPx = error_px;
            }
            break;
        }

        case 'C': {
            uint8_t newMode = (uint8_t)atoi(cmd + 1);
            if (newMode > 1) { Serial.println("ERR:INVALID_MODE"); break; }
            ballActive = false;
            hardStop();
            ctrlMode = newMode;
            if (ctrlMode == MODE_STATIONARY) {
                targetSteps         = stepPos;
                activeAccelPerMs    = STAT_ACCEL_PER_MS;
                activeBallTimeoutMs = STAT_BALL_TIMEOUT_MS;
            } else {
                activeAccelPerMs    = PTZ_ACCEL_PER_MS;
                activeBallTimeoutMs = PTZ_BALL_TIMEOUT_MS;
            }
            Serial.println("OK");
            break;
        }

        case 'L':
            ballActive = false;
            if (ctrlMode == MODE_STATIONARY) targetSteps = stepPos;
            targetVel = 0;
            break;

        case 'V':
            ballActive = false;
            targetVel = constrain(atoi(cmd + 1),
                -(ctrlMode == MODE_STATIONARY ? STAT_MAX_VEL_SPS : PTZ_MAX_VEL_SPS),
                 (ctrlMode == MODE_STATIONARY ? STAT_MAX_VEL_SPS : PTZ_MAX_VEL_SPS));
            break;

        case '?':
            Serial.print('P');
            Serial.println(stepPos);
            break;

        case 'H':
            doHoming();
            break;

        case 'S':
            ballActive = false;
            if (ctrlMode == MODE_STATIONARY) targetSteps = stepPos;
            hardStop();
            break;

        case 'G':
            ballActive = false;
            gotoPos(atoi(cmd + 1));
            break;

        case 'Z':
            stepPos     = 0;
            targetSteps = 0;
            break;

        case 'E':
            gpio_set_level((gpio_num_t)EN_PIN, LOW);
            break;

        case 'D':
            hardStop();
            ballActive = false;
            delay(50);
            gpio_set_level((gpio_num_t)EN_PIN, HIGH);
            break;
    }
}

// ============================================================
//  SETUP
// ============================================================
void setup() {
    Serial.begin(460800);
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

    while (Serial.available()) {
        char ch = (char)Serial.read();
        if (ch == '\n' || ch == '\r') {
            if (bufIdx > 0) {
                cmdBuf[bufIdx] = '\0';
                handleCommand(cmdBuf);
                bufIdx = 0;
            }
        } else if (bufIdx < 31) {
            cmdBuf[bufIdx++] = ch;
        }
    }
}
