"""Camera names and device paths shared by the pipeline and RTMP worker."""

import os

# Current working defaults. Change these to /dev/fixed_camera and /dev/ptz_camera
# when those stable device links are fixed on the Jetson.
FIXED_CAMERA_DEVICE = (
    os.environ.get("SMARTCAM_FIXED_CAMERA_DEVICE")
    or os.environ.get("SMARTCAM_CAM0_DEVICE")
    or "/dev/video0"
)
PTZ_CAMERA_DEVICE = (
    os.environ.get("SMARTCAM_PTZ_CAMERA_DEVICE")
    or os.environ.get("SMARTCAM_CAM2_DEVICE")
    or "/dev/video2"
)

# Backward-compatible names for older imports.
CAM0_DEVICE = FIXED_CAMERA_DEVICE
CAM2_DEVICE = PTZ_CAMERA_DEVICE

FIXED_CAMERA = "fixed"
PTZ_CAMERA = "ptz"

CAMERA_DEVICE_BY_STREAM_CAMERA = {
    FIXED_CAMERA: FIXED_CAMERA_DEVICE,
    PTZ_CAMERA: PTZ_CAMERA_DEVICE,
}

CAMERA_DEVICE_ALIASES = {
    "0": FIXED_CAMERA,
    "cam0": FIXED_CAMERA,
    "camera0": FIXED_CAMERA,
    "fixed": FIXED_CAMERA,
    "fixed_camera": FIXED_CAMERA,
    "/dev/video0": FIXED_CAMERA,
    FIXED_CAMERA_DEVICE.lower(): FIXED_CAMERA,
    "2": PTZ_CAMERA,
    "cam2": PTZ_CAMERA,
    "camera2": PTZ_CAMERA,
    "ptz": PTZ_CAMERA,
    "ptz_camera": PTZ_CAMERA,
    "/dev/video2": PTZ_CAMERA,
    PTZ_CAMERA_DEVICE.lower(): PTZ_CAMERA,
}

__all__ = [
    "CAM0_DEVICE",
    "CAM2_DEVICE",
    "CAMERA_DEVICE_ALIASES",
    "CAMERA_DEVICE_BY_STREAM_CAMERA",
    "FIXED_CAMERA",
    "FIXED_CAMERA_DEVICE",
    "PTZ_CAMERA",
    "PTZ_CAMERA_DEVICE",
]
