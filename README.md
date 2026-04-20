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
  `/dev/video0` for CAM0 and `/dev/video2` for CAM2; switch them to
  `/dev/fixed_camera` and `/dev/ptz_camera` there once those stable device links
  are working. For temporary tests, override them with `SMARTCAM_CAM0_DEVICE`
  and `SMARTCAM_CAM2_DEVICE`.
- MediaMTX accepting local RTSP publishing on port `8554`; WebRTC URLs are
  reported on port `8889`.
- DeepStream-Yolo model/config files in `/home/smartcam/DeepStream-Yolo`.
- `scoreboard.png` in the same directory as these scripts if RTMP streaming
  with the scoreboard overlay is used.

## Outputs and control endpoints

The main process publishes:

- Clean streams:
    - `http://<jetson-ip>:8889/camera0_clean`
    - `http://<jetson-ip>:8889/camera2_clean`
    - `rtsp://<jetson-ip>:8554/camera0_clean`
    - `rtsp://<jetson-ip>:8554/camera2_clean`
- AI/debug streams:
    - `http://<jetson-ip>:8889/camera0_ai`
    - `http://<jetson-ip>:8889/camera2_ai` when `ENABLE_CAM2_AI` is enabled.
    - `rtsp://<jetson-ip>:8554/camera0_ai`
    - `rtsp://<jetson-ip>:8554/camera2_ai` when `ENABLE_CAM2_AI` is enabled.
- Internal RTSP camera streams for the RTMP worker:
    - `rtsp://<jetson-ip>:8554/camera0_stream`
    - `rtsp://<jetson-ip>:8554/camera2_stream`
- Local HTTP debug API:
    - `GET http://127.0.0.1:9101/status`
    - `POST http://127.0.0.1:9101/score`
- Unix socket for the Go bridge:
    - `/tmp/smartcam.sock`
- Unix socket for camera control detections:
    - `/tmp/pycam.sock`

Example score update:

```bash
curl -X POST http://127.0.0.1:9101/score \
  -H 'Content-Type: application/json' \
  -d '{"visible":true,"home_name":"HOME","away_name":"AWAY","home_points":12,"away_points":8,"quarter":1,"clock":"07:42"}'
```

## RTMP streaming

The main pipeline creates internal `camera0_stream` and `camera2_stream` feeds.
RTMP forwarding is handled by a separate worker so RTMP failures do not take down
the camera/AI pipeline. The worker defaults to CAM2 and can switch between
available camera feeds through `stream_worker_config.json` or the Go bridge
`switch_cam` command.

Normal flow:

1. Start the main process with `python3 run_pipeline.py`.
2. From devtablet, send the stream command with the YouTube RTMP URL:
   `start_stream rtmp://a.rtmp.youtube.com/live2/stream-key`
3. `pipeline.py` writes `stream.conf` and starts `run_stream_worker.py`.
4. `run_stream_worker.py` restarts `stream_worker.py` if the RTMP worker fails.
5. `stream_worker.py` reads the active internal RTSP feed, applies the scoreboard
   overlay, and publishes to RTMP.

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
switch_cam cam0
switch_cam cam2
```

For manual debugging only, replacing `stream.conf` with `# disabled` prevents
RTMP streaming on the next worker/pipeline start.

## Files to edit

- `run_pipeline.py`: crash-proof wrapper for the main DeepStream process.
- `pipeline.py`: main cameras, inference, RTSP outputs, sockets, HTTP API, score
  state, and worker startup logic.
- `run_stream_worker.py`: crash-proof wrapper for the RTMP worker.
- `stream_worker.py`: active RTMP pipeline and scoreboard overlay rendering.

For OSD/scoreboard graphics work, start with `stream_worker.py`:

- `SCOREBOARD_PNG`: expected background asset path, currently `scoreboard.png`
  next to these scripts.
- `SCOREBOARD_W`, `SCOREBOARD_H`: rendered scoreboard background size.
- `SCOREBOARD_OFFSET_X`, `SCOREBOARD_OFFSET_Y`: background placement in the
  1080p stream.
- `configure_scoreboard_texts(...)` in `rtmp_elements.py`: text positions,
  fonts, and colors for quarter, team names, score, clock, fouls/timeouts, and
  milestone banners.
- `_update_overlay(...)`: maps `score_state.json` fields to overlay text and
  visibility.

## Runtime-generated files

These files are created or updated while the system runs:

- `stream.conf`: RTMP URL, or `# disabled`.
- `score_state.json`: persisted score and overlay visibility.
- `stream_worker_config.json`: RTMP worker settings such as `bitrateKbps` and
  `activeCamera`.
- `stream_worker_status.json`: worker health and RTMP status.
- `stream_worker.pid`: RTMP worker wrapper PID metadata.

Do not treat those files as source-of-truth code changes unless the specific
runtime state is intentionally being captured.
