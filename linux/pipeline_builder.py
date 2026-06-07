"""
pipeline_builder.py — Shared GStreamer pipeline construction.

Encoder priority (Legion Go / AMD APU):
  1. vah264lpenc / vah264enc / vaapih264enc  (VAAPI via Mesa/AMD — HW, preferred)
  2. x264enc                                 (SW fallback — warning logged)
"""

import logging
import os
import shutil
import subprocess
import time

log = logging.getLogger("pipeline")

# System GStreamer plugin directory — ensures libgstva.so (vah264enc) is found
# even when running from the project venv whose subprocess PATH differs.
_GST_SYSTEM_PLUGIN_DIR = "/usr/lib64/gstreamer-1.0"


def _gst_env() -> dict:
    """
    Return an env dict that guarantees gst-launch-1.0 / gst-inspect-1.0 find
    all system GStreamer plugins including libgstva.so (vah264enc).
    """
    env = os.environ.copy()
    existing = env.get("GST_PLUGIN_SYSTEM_PATH", "")
    if _GST_SYSTEM_PLUGIN_DIR not in existing:
        env["GST_PLUGIN_SYSTEM_PATH"] = (
            f"{_GST_SYSTEM_PLUGIN_DIR}:{existing}" if existing
            else _GST_SYSTEM_PLUGIN_DIR
        )
    log.debug("[PIPELINE] GST_PLUGIN_SYSTEM_PATH=%s", env["GST_PLUGIN_SYSTEM_PATH"])
    return env


# ── Encoder detection ─────────────────────────────────────────────────────────

def detect_igpu_encoder() -> str | None:
    """
    Detect an iGPU VA-API H.264 encoder.
    Skips NVIDIA dGPU entries.  Returns element name or None (→ CPU fallback).

    AMD APU (Legion Go) priority: vah264lpenc → vah264enc → vaapih264enc
    """
    _vainfo()
    env = _gst_env()

    for enc in ("vah264lpenc", "vah264enc", "vaapih264enc"):
        try:
            r = subprocess.run(
                ["gst-inspect-1.0", enc],
                capture_output=True, text=True, timeout=5,
                env=env,
            )
            if r.returncode == 0 and "nvidia" not in r.stdout.lower():
                log.info("[ENCODER] Selected HW encoder: %s", enc)
                return enc
        except Exception:
            continue

    log.warning(
        "[ENCODER] ⚠  No VAAPI HW encoder found — falling back to x264enc (CPU). "
        "Expect high CPU usage and possible thermal throttling on Legion Go."
    )
    return None


def _vainfo():
    """Log vainfo output to help diagnose HW encoder availability."""
    vainfo_bin = shutil.which("vainfo") or "/usr/bin/vainfo"
    try:
        r = subprocess.run([vainfo_bin], capture_output=True, text=True, timeout=5)
        preview = r.stdout[:400] or r.stderr[:200]
        log.info("[VAAPI] vainfo (%s):\n%s", vainfo_bin, preview.strip())
    except FileNotFoundError:
        log.warning("[VAAPI] vainfo not found at %s", vainfo_bin)
    except Exception as exc:
        log.warning("[VAAPI] vainfo failed: %s", exc)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _align16(n: int) -> int:
    """Round n UP to the nearest multiple of 16 (H.264 macroblock grid)."""
    return (n + 15) & ~15


def _hw_encoder_params(enc_name: str, bitrate: int, key_int: int) -> str:
    if enc_name == "vaapih264enc":
        return (
            f"{enc_name} rate-control=cbr bitrate={bitrate} "
            f"keyframe-period={key_int} max-bframes=0 quality-level=7"
        )
    # vah264enc / vah264lpenc
    return (
        f"{enc_name} rate-control=cbr bitrate={bitrate} "
        f"key-int-max={key_int} ref-frames=1 b-frames=0 target-usage=7"
    )


def _cpu_encoder_params(bitrate: int, key_int: int) -> str:
    # vbv-bufsize must be >= bitrate to avoid throttling; 2× gives CBR headroom.
    # Cap at 20000 kbps — x264 cannot keep up at higher rates on the Legion Go APU.
    safe_bitrate = min(bitrate, 20_000)
    if bitrate > safe_bitrate:
        log.warning(
            "[ENCODER] Capping CPU x264enc bitrate %d → %d kbps "
            "(higher values cause lag without HW encoder)",
            bitrate, safe_bitrate,
        )
    vbv = safe_bitrate * 2
    return (
        f"x264enc tune=zerolatency speed-preset=ultrafast bitrate={safe_bitrate} "
        f"key-int-max={key_int} byte-stream=true "
        f"option-string=\"bframes=0:ref=1:sliced-threads=0:"
        f"rc-lookahead=0:sync-lookahead=0:threads=4:"
        f"vbv-bufsize={vbv}:vbv-maxrate={safe_bitrate}\""
    )


# ── Pipeline builder ──────────────────────────────────────────────────────────

def build_pipeline(*, pw_fd, node_id, width, height, fps, bitrate, port,
                   hw_encoder=None) -> str:
    """
    Build a full gst-launch-1.0 pipeline string.

    Pipeline order
    --------------
    pipewiresrc  →  videorate(drop-only)  →  queue(pre)
                 →  videoscale  →  videoconvert(NV12)
                 →  encoder  →  h264parse  →  queue(post)  →  tcpclientsink

    Key latency decisions
    ---------------------
    • drop-only=true on videorate: passes frames straight through, drops
      excess — no hold-until-next-frame latency.
    • pre-encode queue: 3 bufs / leaky=downstream — absorbs burst without
      blocking the PipeWire source; drops oldest, not newest.
    • post-encode queue: 4 bufs / leaky=upstream — drops newly-encoded
      P-frames when full, preserving keyframes already queued. The previous
      leaky=downstream would drop the oldest (a keyframe), stalling Android
      until the next IDR (up to 0.5 s at 60 fps).
    • tcpclientsink sync=false: no clock-driven pacing; send as fast as the
      socket allows.
    • scale → convert order: scale in native pixel format first, one
      videoconvert to NV12 at the end — avoids double format conversion.
    """
    enc_w = _align16(width)
    enc_h = _align16(height)
    if enc_w != width or enc_h != height:
        log.info(
            "[PIPELINE] Aligning %dx%d → %dx%d (H.264 macroblock, black borders)",
            width, height, enc_w, enc_h,
        )

    encoder_label = hw_encoder or "x264enc (CPU fallback)"
    log.info(
        "[CONFIG] Resolution=%dx%d  FPS=%d  Bitrate=%d kbps  Encoder=%s  Port=%d",
        width, height, fps, bitrate, encoder_label, port,
    )

    if pw_fd is not None:
        src = (f"pipewiresrc fd={pw_fd} path={node_id} "
               f"do-timestamp=true always-copy=true keepalive-time=2000")
    else:
        src = (f"pipewiresrc path={node_id} "
               f"do-timestamp=true always-copy=true keepalive-time=2000")

    # drop-only=true: pass frames through, drop excess — no 1-frame hold latency
    framerate = (
        f"videorate drop-only=true skip-to-first=true ! "
        f"video/x-raw,framerate={fps}/1"
    )

    queue_pre  = "queue max-size-buffers=3 max-size-time=0 leaky=downstream"
    # Post-encode queue: leaky=upstream drops newly-encoded frames when full,
    # preserving keyframes already queued. leaky=downstream (previous) dropped
    # the oldest buffer — which could be a keyframe — stalling the Android
    # decoder until the next IDR (up to key_int frames = 0.5 s at 60 fps).
    queue_post = "queue max-size-buffers=4 max-size-time=0 leaky=upstream"

    key_int = max(fps // 2, 15)   # keyframe every ~0.5 s

    # Scale first (in native format), then single convert to NV12
    scale_caps = (
        f"videoscale add-borders=true ! "
        f"video/x-raw,width={enc_w},height={enc_h},pixel-aspect-ratio=1/1 ! "
        f"videoconvert n-threads=4 ! "
        f"video/x-raw,format=NV12"
    )

    encoder = (
        _hw_encoder_params(hw_encoder, bitrate, key_int)
        if hw_encoder else
        _cpu_encoder_params(bitrate, key_int)
    )

    parse    = "h264parse config-interval=-1"
    caps_out = "video/x-h264,stream-format=byte-stream,alignment=au"
    sink     = f"tcpclientsink host=127.0.0.1 port={port} sync=false"

    pipeline = (
        f"gst-launch-1.0 -e "
        f"{src} ! {framerate} ! {queue_pre} ! {scale_caps} ! "
        f"{encoder} ! {parse} ! {caps_out} ! {queue_post} ! {sink}"
    )
    return pipeline


# ── Launcher with HW→CPU fallback ────────────────────────────────────────────

def launch_with_fallback(*, pw_fd, node_id, width, height, fps, bitrate, port,
                         hw_encoder=None, pass_fds=None):
    """
    Launch the streaming pipeline and return the Popen object immediately.

    If a HW encoder is requested, we wait up to 4 s to detect a fast exit
    (negotiation failure) and fall back to CPU.  Once the HW encoder passes
    the 4 s probe we return the Popen — we do NOT call proc.wait() here
    because the pipeline runs indefinitely; blocking here would freeze the
    calling thread (and stop all frames from being processed).
    """
    pipeline = build_pipeline(
        pw_fd=pw_fd, node_id=node_id,
        width=width, height=height, fps=fps, bitrate=bitrate, port=port,
        hw_encoder=hw_encoder,
    )
    log.info("[PIPELINE] %s", pipeline)

    # Kill any stale gst-launch-1.0 processes from previous sessions that are
    # still holding the TCP port open. Multiple senders on the same port causes
    # the Android decoder to receive garbled interleaved data and freeze.
    try:
        killed = subprocess.run(
            ["pkill", "-f", f"gst-launch.*{port}"],
            capture_output=True,
        )
        if killed.returncode == 0:
            log.info("[PIPELINE] Killed stale gst-launch-1.0 on port %d", port)
            time.sleep(0.3)   # let the OS release the port
    except Exception:
        pass

    env = _gst_env()
    kwargs: dict = {"shell": True, "env": env}
    if pass_fds:
        kwargs["pass_fds"] = pass_fds

    t0 = time.monotonic()
    proc = subprocess.Popen(pipeline, **kwargs)
    log.info("[SEND] GStreamer pipeline started (pid=%d)", proc.pid)

    if hw_encoder:
        try:
            proc.wait(timeout=4)
        except subprocess.TimeoutExpired:
            # Still running after 4 s — HW encoder negotiated successfully.
            # Return immediately; the calling thread must NOT block here.
            log.info("[PIPELINE] HW encoder (%s) running OK (%.1f s) — streaming",
                     hw_encoder, time.monotonic() - t0)
            return proc

        # Exited within 4 s — negotiation failure, fall back to CPU
        rc = proc.returncode
        if rc != 0:
            log.warning(
                "[PIPELINE] HW encoder %s failed (exit %d) after %.2f s — "
                "retrying with x264enc (CPU)",
                hw_encoder, rc, time.monotonic() - t0,
            )
            pipeline = build_pipeline(
                pw_fd=pw_fd, node_id=node_id,
                width=width, height=height, fps=fps, bitrate=bitrate, port=port,
                hw_encoder=None,
            )
            log.info("[PIPELINE] CPU fallback: %s", pipeline)
            proc = subprocess.Popen(pipeline, **kwargs)
            log.info("[SEND] CPU fallback pipeline started (pid=%d)", proc.pid)
        return proc

    return proc
