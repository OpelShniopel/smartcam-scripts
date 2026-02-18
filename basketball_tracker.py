#!/usr/bin/env python3

import sys
import gi

gi.require_version('Gst', '1.0')
from gi.repository import GLib, Gst
import pyds

MUXER_BATCH_TIMEOUT_USEC = 33000
PGIE_CLASS_ID_BALL = 1
PGIE_CLASS_ID_RIM = 2


def osd_sink_pad_buffer_probe(pad, info, u_data):
    gst_buffer = info.get_buffer()
    if not gst_buffer:
        return Gst.PadProbeReturn.OK

    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    l_frame = batch_meta.frame_meta_list

    while l_frame is not None:
        try:
            frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
        except StopIteration:
            break

        frame_number = frame_meta.frame_num
        ball_found = False

        l_obj = frame_meta.obj_meta_list
        while l_obj is not None:
            try:
                obj_meta = pyds.NvDsObjectMeta.cast(l_obj.data)
            except StopIteration:
                break

            if obj_meta.class_id == PGIE_CLASS_ID_BALL:
                left = obj_meta.rect_params.left
                top = obj_meta.rect_params.top
                width = obj_meta.rect_params.width
                height = obj_meta.rect_params.height
                confidence = obj_meta.confidence
                center_x = left + width / 2
                center_y = top + height / 2
                ball_found = True
                print(f"Frame {frame_number} | BALL center=({center_x:.0f}, {center_y:.0f}) | conf={confidence:.2f}")

            elif obj_meta.class_id == PGIE_CLASS_ID_RIM:
                print(f"Frame {frame_number} | RIM detected")

            try:
                l_obj = l_obj.next
            except StopIteration:
                break

        if not ball_found:
            print(f"Frame {frame_number} | No ball detected")

        try:
            l_frame = l_frame.next
        except StopIteration:
            break

    return Gst.PadProbeReturn.OK


def main():
    Gst.init(None)

    pipeline = Gst.Pipeline()

    # Source
    source = Gst.ElementFactory.make("v4l2src", "usb-cam-source")
    source.set_property("device", "/dev/video0")
    source.set_property("io-mode", 2)

    caps_src = Gst.ElementFactory.make("capsfilter", "caps-src")
    caps_src.set_property("caps", Gst.Caps.from_string(
        "image/jpeg, width=1920, height=1080, framerate=30/1"
    ))

    jpegparse = Gst.ElementFactory.make("jpegparse", "jpeg-parser")

    decoder = Gst.ElementFactory.make("nvv4l2decoder", "decoder")
    decoder.set_property("mjpeg", 1)

    nvvidconv1 = Gst.ElementFactory.make("nvvideoconvert", "convertor1")

    caps_nvmm = Gst.ElementFactory.make("capsfilter", "caps-nvmm")
    caps_nvmm.set_property("caps", Gst.Caps.from_string(
        "video/x-raw(memory:NVMM), format=NV12"
    ))

    # Streammux
    streammux = Gst.ElementFactory.make("nvstreammux", "stream-muxer")
    streammux.set_property("width", 1920)
    streammux.set_property("height", 1080)
    streammux.set_property("batch-size", 1)
    streammux.set_property("batched-push-timeout", MUXER_BATCH_TIMEOUT_USEC)

    # Inference
    pgie = Gst.ElementFactory.make("nvinfer", "primary-inference")
    pgie.set_property("config-file-path", "deepstream_rfdetr_bbox_config.txt")

    # OSD
    nvvidconv2 = Gst.ElementFactory.make("nvvideoconvert", "convertor2")
    nvosd = Gst.ElementFactory.make("nvdsosd", "onscreendisplay")

    # Display sink
    nvvidconv3 = Gst.ElementFactory.make("nvvideoconvert", "convertor3")
    caps_bgrx = Gst.ElementFactory.make("capsfilter", "caps-bgrx")
    caps_bgrx.set_property("caps", Gst.Caps.from_string("video/x-raw, format=BGRx"))
    sink = Gst.ElementFactory.make("nv3dsink", "display-sink")
    sink.set_property("sync", False)

    # Check all elements created ok
    for name, el in [("source", source), ("caps_src", caps_src),
                     ("jpegparse", jpegparse), ("decoder", decoder),
                     ("nvvidconv1", nvvidconv1), ("caps_nvmm", caps_nvmm),
                     ("streammux", streammux), ("pgie", pgie),
                     ("nvvidconv2", nvvidconv2), ("nvosd", nvosd),
                     ("nvvidconv3", nvvidconv3), ("caps_bgrx", caps_bgrx),
                     ("sink", sink)]:
        if not el:
            sys.stderr.write(f"Failed to create element: {name}\n")
            sys.exit(1)

    # Add all elements
    for el in [source, caps_src, jpegparse, decoder, nvvidconv1, caps_nvmm,
               streammux, pgie, nvvidconv2, nvosd, nvvidconv3, caps_bgrx, sink]:
        pipeline.add(el)

    # Link source chain up to streammux
    source.link(caps_src)
    caps_src.link(jpegparse)
    jpegparse.link(decoder)
    decoder.link(nvvidconv1)
    nvvidconv1.link(caps_nvmm)

    sinkpad = streammux.request_pad_simple("sink_0")
    srcpad = caps_nvmm.get_static_pad("src")
    srcpad.link(sinkpad)

    # Link inference + display chain
    streammux.link(pgie)
    pgie.link(nvvidconv2)
    nvvidconv2.link(nvosd)
    nvosd.link(nvvidconv3)
    nvvidconv3.link(caps_bgrx)
    caps_bgrx.link(sink)

    # Attach probe
    osdsinkpad = nvosd.get_static_pad("sink")
    osdsinkpad.add_probe(Gst.PadProbeType.BUFFER, osd_sink_pad_buffer_probe, 0)

    # Bus error handling
    def on_message(bus, msg):
        if msg.type == Gst.MessageType.ERROR:
            err, debug = msg.parse_error()
            print(f"ERROR: {err} — {debug}")
            loop.quit()
        elif msg.type == Gst.MessageType.EOS:
            print("End of stream")
            loop.quit()

    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", on_message)

    print("Starting pipeline... press Ctrl+C to stop")
    pipeline.set_state(Gst.State.PLAYING)
    try:
        loop.run()
    except KeyboardInterrupt:
        pass

    pipeline.set_state(Gst.State.NULL)
    print("Pipeline stopped.")


if __name__ == '__main__':
    sys.exit(main())
