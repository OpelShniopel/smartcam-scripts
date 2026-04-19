"""Camera device paths shared by the main pipeline and RTMP worker."""

import os

# Current working defaults. Change these to /dev/fixed_camera and /dev/ptz_camera
# when those stable device links are fixed on the Jetson.
CAM0_DEVICE = os.environ.get("SMARTCAM_CAM0_DEVICE") or "/dev/video0"
CAM2_DEVICE = os.environ.get("SMARTCAM_CAM2_DEVICE") or "/dev/video2"

CAMERA_DEVICE_BY_STREAM_CAMERA = {
    "cam0": CAM0_DEVICE,
    "cam2": CAM2_DEVICE,
}

CAMERA_DEVICE_ALIASES = {
    "0": "cam0",
    "cam0": "cam0",
    "camera0": "cam0",
    CAM0_DEVICE.lower(): "cam0",
    "2": "cam2",
    "cam2": "cam2",
    "camera2": "cam2",
    CAM2_DEVICE.lower(): "cam2",
}

__all__ = [
    "CAM0_DEVICE",
    "CAM2_DEVICE",
    "CAMERA_DEVICE_ALIASES",
    "CAMERA_DEVICE_BY_STREAM_CAMERA",
]
