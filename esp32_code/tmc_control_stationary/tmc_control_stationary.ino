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
#define PAN_MAX_STEPS    (PAN_MAX_DEG * STEPS_PER_DEG)    //  11375
#define PAN_MIN_STEPS    (PAN_MIN_DEG * STEPS_PER_DEG)    //  -4375

// Direction — flip between 0 and 1 to reverse motor direction
#define INVERT_DIR  1
#define DIR_FWD     (INVERT_DIR ? LOW  : HIGH)
#define DIR_REV     (INVERT_DIR ? HIGH : LOW)

// ============================================================
//  STATIONARY-CAM MAPPING
// ============================================================
#define STATIONARY_FRAME_HALF     640.0f   // half-width of stationary cam (px)
#define STEPS_PER_STATIONARY_PX   10.0f  // Mapping pixels to steps

// ============================================================
//  P CONTROLLER CONSTANTS
// ============================================================
#define MIN_DEADZONE_STEPS   100      // steps — no movement inside this window (~0.5°)
#define MAX_DEADZONE_STEPS  200      // steps — used at minimum speed_scale

#define SPEED_FACTOR        1.2f    // lower = more linear approach, less crawl near target
#define MIN_VEL_SPS         400     // steps/sec — floor keeps the final approach decisive
#define MAX_VEL_SPS         12000    // steps/sec at full-range error
#define HOMING_VEL_SPS      3000

#define BALL_BOOST_THR_STEPS  100   // step/frame delta — above this, apply velocity boost
#define BALL_BOOST_GAIN        6.0f
#define BALL_TIMEOUT_MS        1000  // ms without X update before auto-stop

// ============================================================
//  MOTION CONSTANTS
// ============================================================
#define ACCEL_PER_MS         700     // steps/sec per ms ramp rate

// ============================================================
//  SERIAL
// ============================================================
#define CMD_BAUD            460800

// ============================================================
//  GLOBALS
// ============================================================
hw_timer_t* stepTimer = NULL;

volatile int32_t stepPos    = 0;
volatile int32_t runningVel = 0;
volatile bool    stepPhase  = false;
volatile int32_t targetVel  = 0;

int32_t  targetSteps  = 0;    // desired absolute position (set by X command)
int32_t  lastTargetSteps = 0; // previous target for boost detection
uint32_t lastRampMs   = 0;
uint32_t lastXMs      = 0;
bool     ballActive   = false;

char cmdBuf[32];
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
//  POSITION P CONTROLLER
//  Computes velocity from (targetSteps - stepPos).
// ============================================================
int32_t computeVelocityFromSteps(int32_t target, float scale) {
    int32_t step_error = target - stepPos;

    float dynamic_deadzone = MIN_DEADZONE_STEPS + (MAX_DEADZONE_STEPS - MIN_DEADZONE_STEPS) * (1.0f - scale);

    if (abs(step_error) <= (int32_t)dynamic_deadzone) return 0;

    float full_range  = STEPS_PER_STATIONARY_PX * STATIONARY_FRAME_HALF;
    float normalized  = min(1.0f, (float)abs(step_error) / full_range);
    float base_factor = normalized;//powf(normalized, SPEED_FACTOR);

    // Velocity boost when target jumps quickly
    int32_t target_delta = abs(target - lastTargetSteps);
    float multiplier = 1.0f;
    if (target_delta > BALL_BOOST_THR_STEPS)
        multiplier += (target_delta / full_range) * BALL_BOOST_GAIN;

    float speed = MIN_VEL_SPS + (MAX_VEL_SPS - MIN_VEL_SPS) * base_factor;
    speed = min((float)MAX_VEL_SPS, speed * multiplier * scale);

    return (step_error > 0) ? (int32_t)speed : -(int32_t)speed;
}

// ============================================================
//  VELOCITY RAMP  — 1ms tick
// ============================================================
void updateVelocity() {
    uint32_t now = millis();
    if (now - lastRampMs < 1) return;
    lastRampMs = now;

    if (ballActive && (now - lastXMs > BALL_TIMEOUT_MS)) {
        ballActive  = false;
        targetSteps = stepPos;   // hold current position
        targetVel   = 0;
    }

    if (ballActive) {
        targetVel = computeVelocityFromSteps(targetSteps, 1.0f);
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

    stepPos     = PAN_MAX_STEPS;
    targetSteps = PAN_MAX_STEPS;
    Serial.println("HOMING:CENTER");
    gpio_set_level((gpio_num_t)DIR_PIN, DIR_REV);
    runningVel = -(MAX_VEL_SPS / 2);
    setStepFreq(MAX_VEL_SPS / 2);
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
    int32_t moveVel  = (target > stepPos) ? MAX_VEL_SPS / 2 : -(MAX_VEL_SPS / 2);
    
    if (moveVel > 0) gpio_set_level((gpio_num_t)DIR_PIN, DIR_FWD);
    else             gpio_set_level((gpio_num_t)DIR_PIN, DIR_REV);
    runningVel = moveVel;
    setStepFreq(moveVel);

    uint32_t timeout = millis() + 20000;
    while (millis() < timeout) {
        int32_t remaining = abs(stepPos - target);
        if (remaining <= 3) break;
        delay(1);
    }
    hardStop();
    delay(50);
    Serial.println("OK");
}

// ============================================================
//  COMMAND HANDLER (Switch-based for speed)
// ============================================================
void handleCommand(char* cmd) {
    if (strlen(cmd) == 0) return;
    char c = cmd[0];

    switch (c) {
        case 'X': {
            // X<error_px>[,<scale_pct>]
            char* commaPos = strchr(cmd, ',');
            int32_t error_px;
            float scale = 1.0f;

            if (commaPos) {
                *commaPos = '\0'; // Split the string
                error_px = atoi(cmd + 1);
                scale = constrain(atoi(commaPos + 1), 1, 100) / 100.0f;
            } else {
                error_px = atoi(cmd + 1);
            }

            lastTargetSteps = targetSteps;
            targetSteps = constrain((int32_t)(error_px * STEPS_PER_STATIONARY_PX), PAN_MIN_STEPS, PAN_MAX_STEPS);
            lastXMs = millis();
            ballActive = true;
            targetVel = computeVelocityFromSteps(targetSteps, scale);
            break;
        }

        case 'L': // Ball Lost
            ballActive = false;
            targetSteps = stepPos;
            targetVel = 0;
            break;

        case 'V': // Manual Velocity Override
            ballActive = false;
            targetVel = constrain(atoi(cmd + 1), -MAX_VEL_SPS, MAX_VEL_SPS);
            break;

        case '?': // Query Position
            Serial.print('P');
            Serial.println(stepPos);
            break;

        case 'H': // Homing
            doHoming();
            break;

        case 'S': // Hard Stop
            ballActive = false;
            targetSteps = stepPos;
            hardStop();
            break;

        case 'G': // Goto Position
            ballActive = false;
            gotoPos(atoi(cmd + 1));
            break;

        case 'Z': // Zero
            stepPos = 0;
            targetSteps = 0;
            break;

        case 'E': // Enable
            gpio_set_level((gpio_num_t)EN_PIN, LOW);
            break;

        case 'D': // Disable
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

    // Fast serial character ingestion
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