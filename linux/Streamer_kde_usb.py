#!/usr/bin/env python3
"""
Streamer_kde_usb.py — KDE Plasma / Wayland streamer (USB mode).

Modes (--mode flag, also passed by GUI):
  virtual  (default) — stream the dedicated virtual monitor (TabletDisplay);
                        the Legion Go's physical screen is unaffected.
  desktop             — stream the selected physical display.

Usage (from GUI):
  python3 Streamer_kde_usb.py <width> <height> <fps> <bitrate> [--mode virtual|desktop]
"""
import os, sys, signal, subprocess, threading, logging, time
import dbus
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GLib

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("streamer-kde")

sys.path.insert(0, os.path.dirname(__file__))
from pipeline_builder import detect_igpu_encoder, launch_with_fallback  # noqa: E402

# ── CLI args ──────────────────────────────────────────────────────────────────
WIDTH   = int(sys.argv[1]) if len(sys.argv) > 1 else 2560
HEIGHT  = int(sys.argv[2]) if len(sys.argv) > 2 else 1600
FPS     = int(sys.argv[3]) if len(sys.argv) > 3 else 60
BITRATE = int(sys.argv[4]) if len(sys.argv) > 4 else 8000
MODE    = "virtual"
for _i, _arg in enumerate(sys.argv):
    if _arg == "--mode" and _i + 1 < len(sys.argv):
        MODE = sys.argv[_i + 1].lower()
    elif _arg.startswith("--mode="):
        MODE = _arg.split("=", 1)[1].lower()
if MODE not in ("virtual", "desktop"):
    log.error("--mode must be 'virtual' or 'desktop', got %r — using 'virtual'", MODE)
    MODE = "virtual"

PORT = 7110

log.info(
    "[CONFIG] Mode=%s  Resolution=%dx%d  FPS=%d  Bitrate=%d kbps  Port=%d",
    MODE, WIDTH, HEIGHT, FPS, BITRATE, PORT,
)

# ── Encoder ───────────────────────────────────────────────────────────────────
HW_ENCODER = detect_igpu_encoder()

# ── D-Bus / GLib ──────────────────────────────────────────────────────────────
DBusGMainLoop(set_as_default=True)
loop     = GLib.MainLoop()
bus      = dbus.SessionBus()
desktop  = bus.get_object("org.freedesktop.portal.Desktop",
                          "/org/freedesktop/portal/desktop")
sc       = dbus.Interface(desktop, "org.freedesktop.portal.ScreenCast")
state    = {"step": "create_session", "session": None}
gst_proc = None


def cleanup(sig=None, frame=None):
    log.info("[CLEANUP] Shutting down (sig=%s)…", sig)
    if gst_proc and gst_proc.poll() is None:
        gst_proc.terminate()
        try:
            gst_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            gst_proc.kill()
    if loop.is_running():
        loop.quit()
    sys.exit(0)


signal.signal(signal.SIGINT,  cleanup)
signal.signal(signal.SIGTERM, cleanup)


def launch_streaming(fd, node_id):
    global gst_proc
    log.info("[CAPTURE] PipeWire node=%d  fd=%d — launching GStreamer", node_id, fd)
    t0 = time.monotonic()
    gst_proc = launch_with_fallback(
        pw_fd=fd, node_id=node_id,
        width=WIDTH, height=HEIGHT, fps=FPS, bitrate=BITRATE, port=PORT,
        hw_encoder=HW_ENCODER, pass_fds=(fd,),
    )
    # launch_with_fallback returns the Popen immediately after confirming the
    # encoder is alive.  We must block HERE (in this daemon thread) waiting
    # for gst-launch-1.0 to exit — that is what keeps this thread alive for
    # the entire streaming session and keeps the Portal session open.
    if gst_proc is not None:
        log.info("[SEND] GStreamer running (pid=%d) — waiting for stream end…", gst_proc.pid)
        gst_proc.wait()
    log.info("[SEND] Pipeline exited after %.1f s", time.monotonic() - t0)


def on_response(response, results, **kw):
    if response != 0:
        log.error("[PORTAL] Denied (code %d)", response)
        loop.quit()
        return

    step = state["step"]

    if step == "create_session":
        state["session"] = str(results["session_handle"])
        state["step"]    = "select_sources"
        log.info("[PORTAL] Session: %s", state["session"])
        sc.SelectSources(state["session"], {
            "types":        dbus.UInt32(1),
            "multiple":     dbus.Boolean(False),
            "cursor_mode":  dbus.UInt32(2),
            "handle_token": dbus.String("tok2"),
        })

    elif step == "select_sources":
        state["step"] = "start"
        log.info("[PORTAL] Sources selected — starting…")
        sc.Start(state["session"], "", {"handle_token": dbus.String("tok3")})

    elif step == "start":
        streams = results.get("streams", [])
        if not streams:
            log.error("[PORTAL] No streams returned")
            loop.quit()
            return
        node_id = int(streams[0][0])
        fd_obj  = sc.OpenPipeWireRemote(state["session"], {})
        fd      = fd_obj.take()
        log.info("[PORTAL] PipeWire node=%d  fd=%d", node_id, fd)
        t = threading.Thread(target=launch_streaming, args=(fd, node_id), daemon=True)
        t.start()
        # Keep loop alive — portal session must stay open while streaming


bus.add_signal_receiver(
    on_response,
    signal_name="Response",
    dbus_interface="org.freedesktop.portal.Request",
)

_hint = "TabletDisplay" if MODE == "virtual" else "your physical display"
log.info("[PORTAL] Creating session (%s mode) — pick '%s' in the KDE picker.", MODE, _hint)

sc.CreateSession({
    "handle_token":         dbus.String("tok1"),
    "session_handle_token": dbus.String("ses1"),
})

loop.run()
