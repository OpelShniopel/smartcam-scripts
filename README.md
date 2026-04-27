# Smartcam DeepStream scripts

Python wrappers and GStreamer/DeepStream pipelines for the smartcam basketball
camera setup.

## Runtime directory

Run this from `/home/smartcam/DeepStream-Yolo` only:

```bash
cd /home/smartcam/DeepStream-Yolo
python3 run_pipeline.py
```

The pipeline uses relative DeepStream config paths such as
`config_infer_primary_yoloV8_cam0.txt` and
`config_infer_primary_yoloV8_cam2.txt`, so starting it from another directory
will make `nvinfer` look for model config files in the wrong place.

Use `Ctrl+C` to stop it. Prefer `run_pipeline.py` over calling `pipeline.py`
directly because the wrapper restarts the pipeline after crashes or expected
restart exit codes.

## Runtime requirements

- Jetson with DeepStream/GStreamer/Python bindings installed.
- Camera devices configured in `camera_config.py`. The current defaults are
  `/dev/video0` for the fixed camera and `/dev/video2` for the PTZ camera;
  switch them to `/dev/fixed_camera` and `/dev/ptz_camera` there once those
  stable device links are working. For temporary tests, override them with
  `SMARTCAM_FIXED_CAMERA_DEVICE` and `SMARTCAM_PTZ_CAMERA_DEVICE`. Legacy
  `SMARTCAM_CAM0_DEVICE` and `SMARTCAM_CAM2_DEVICE` overrides are still accepted.
- MediaMTX accepting local RTSP publishing on port `8554`; WebRTC URLs are
  reported on port `8889`.
- DeepStream-Yolo model/config files in `/home/smartcam/DeepStream-Yolo`.
- `scoreboard.png` in the same directory as these scripts if RTMP streaming
  with the scoreboard overlay is used.

## Outputs and control endpoints

The main process publishes:

- Switched program clean feed:
    - `http://<jetson-ip>:8889/program_clean`
    - `rtsp://<jetson-ip>:8554/program_clean`
- AI/debug streams:
    - Fixed camera: `http://<jetson-ip>:8889/camera0_ai`
    - PTZ camera: `http://<jetson-ip>:8889/camera2_ai` when
      `ENABLE_PTZ_CAMERA_AI` is enabled.
    - Fixed camera: `rtsp://<jetson-ip>:8554/camera0_ai`
    - PTZ camera: `rtsp://<jetson-ip>:8554/camera2_ai` when
      `ENABLE_PTZ_CAMERA_AI` is enabled.
- Local HTTP debug API:
    - `GET http://127.0.0.1:9101/status`
    - `POST http://127.0.0.1:9101/score`
- Unix socket for the Go bridge:
    - `/tmp/smartcam.sock`
- Unix socket for PTZ control detections:
    - `/tmp/ptz-control.sock`

Example score update:

```bash
curl -X POST http://127.0.0.1:9101/score \
  -H 'Content-Type: application/json' \
  -d '{"visible":true,"home_name":"HOME","away_name":"AWAY","home_points":12,"away_points":8,"quarter":1,"clock":"07:42"}'
```

## RTMP streaming

The main pipeline creates a switched `program_clean` feed that stays on one
stable RTSP/WebRTC path while the selected camera changes upstream. RTMP
forwarding is handled by a separate worker so RTMP failures do not take down
the camera/AI pipeline. The active camera is controlled through
`stream_worker_config.json` or the Go bridge `switch_cam` command.

Normal flow:

1. Start the main process with `python3 run_pipeline.py`.
2. From devtablet, send the stream command with the YouTube RTMP URL:
   `start_stream rtmp://a.rtmp.youtube.com/live2/stream-key`
3. `pipeline.py` writes `stream.conf` and starts `run_stream_worker.py`.
4. `run_stream_worker.py` restarts `stream_worker.py` if the RTMP worker fails.
5. `stream_worker.py` reads `program_clean`, applies the scoreboard overlay, and
   publishes to RTMP.

The scoreboard OSD is hidden unless OSD is enabled from devtablet. To see the
OSD graphics in the RTMP stream, send:

```text
set_osd true
```

Manual RTMP test flow:

```bash
cd /home/smartcam/DeepStream-Yolo
printf '%s\n' 'rtmp://example/live/key' > stream.conf
python3 run_pipeline.py
```

To stop RTMP streaming from devtablet, send:

```text
stop_stream
```

To switch the RTMP source, send one of:

```text
switch_cam uuid1 - fixed
switch_cam uuid2 - ptz
```

For manual debugging only, replacing `stream.conf` with `# disabled` prevents
RTMP streaming on the next worker/pipeline start.

## For OSD/scoreboard graphics work

- `scoreboard.png`: the background image placed behind all text overlays.
  Replace this file to change the scoreboard design.
- `rtmp_elements.py`: text positions, fonts, colors, and scoreboard PNG
  coordinates. All static overlay configuration is in
  `configure_scoreboard_texts()` and `configure_scoreboard_background()`.
  Also contains `update_score_clock_overlays()`, `update_quarter_overlay()`,
  and `update_milestone_overlays()` which control what text is shown.
- `stream_worker.py`: `_update_overlay()` builds the fouls/timeouts text string
  and controls per-element visibility. Edit here to change how those stats are
  formatted or displayed.
- `pipeline.py`: `_update_osd_texts()` mirrors the fouls/timeouts formatting
  from `stream_worker.py` — keep both in sync when changing display format.
- `score_utils.py`: `truncate_team_name()` extracts the first word of the team
  name before display. Edit here to change how team names are shortened.

For terminal FPS logs:

- `ENABLE_TERMINAL_FPS_METRICS` in `pipeline.py`: enables or disables main
  pipeline FPS logs.
- `ENABLE_AI_FPS_METRICS` in `pipeline.py`: prints FPS only for enabled AI
  branches that are receiving frames.
- `ENABLE_TERMINAL_FPS_METRICS` in `stream_worker.py`: enables or disables RTMP
  worker FPS logs.
- `ENABLE_RTMP_FPS_METRICS` in `stream_worker.py`: prints outbound RTMP FPS for
  the active stream camera.

## Runtime-generated files

These files are created or updated while the system runs:

- `stream.conf`: RTMP URL, or `# disabled`.
- `score_state.json`: persisted score and overlay visibility.
- `stream_worker_config.json`: RTMP worker settings such as `bitrateKbps` and
  `activeCamera` (`fixed` or `ptz`; legacy `cam0` and `cam2` aliases are still
  accepted).
- `stream_worker_status.json`: worker health and RTMP status.
- `stream_worker.pid`: RTMP worker wrapper PID metadata.

Do not treat those files as source-of-truth code changes unless the specific
runtime state is intentionally being captured.
