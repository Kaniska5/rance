"""
RANCE — Flask API Server + WebSocket

Endpoints:
  POST /compress        — compress submitted data, return real stats
  GET  /stats           — live engine stats
  GET  /recent          — last N compression events
  GET  /network         — current network snapshot
  WS   /ws              — push live events to dashboard

Run:
  python server.py
  python server.py --port 5050
"""
import json
import time
import base64
import threading
import argparse
import logging
from collections import deque

from flask import Flask, request, jsonify, send_from_directory
from flask_sock import Sock

from engine import RANCECompressor, RANCEDecompressor
from engine.probe import RealNetworkProbe
from engine.adaptive import classify_condition, profile_data

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rance.server")

app   = Flask(__name__, static_folder=".")
sock  = Sock(app)

# ── Single shared engine instance ──────────────────────────────────────────
_probe      = RealNetworkProbe()
_compressor = RANCECompressor(probe=_probe)
_decomp     = RANCEDecompressor()
_ws_clients: list = []
_ws_lock    = threading.Lock()
_event_bus: deque[dict] = deque(maxlen=500)


def _broadcast(event: dict):
    """Push a JSON event to all connected WebSocket clients."""
    _event_bus.append(event)
    dead = []
    with _ws_lock:
        clients = list(_ws_clients)
    for ws in clients:
        try:
            ws.send(json.dumps(event))
        except Exception:
            dead.append(ws)
    if dead:
        with _ws_lock:
            for d in dead:
                if d in _ws_clients:
                    _ws_clients.remove(d)


# ── Routes ──────────────────────────────────────────────────────────────────

@app.after_request
def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"]  = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return resp


@app.route("/compress", methods=["POST", "OPTIONS"])
def compress():
    """
    Compress submitted data using the real RANCE engine.

    Body (JSON):
      { "data": "<string or base64>", "encoding": "text"|"base64" }

    Returns real compression stats, algorithm chosen, network snapshot.
    """
    if request.method == "OPTIONS":
        return "", 204

    body = request.get_json(force=True, silent=True) or {}
    encoding = body.get("encoding", "text")
    raw      = body.get("data", "")

    if encoding == "base64":
        try:
            data = base64.b64decode(raw)
        except Exception:
            return jsonify({"error": "Invalid base64"}), 400
    else:
        data = raw.encode("utf-8", errors="replace")

    if not data:
        return jsonify({"error": "Empty data"}), 400

    # ── Run real compression ─────────────────────────────────────────────
    t0       = time.perf_counter_ns()
    framed   = _compressor.compress(data)
    elapsed  = (time.perf_counter_ns() - t0) / 1e6

    # Verify decompression round-trip
    recovered = _decomp.decompress(framed)
    roundtrip_ok = (recovered == data)

    recent = _compressor.recent(1)
    ev     = recent[-1] if recent else {}

    result = {
        "ok":             True,
        "original_size":  len(data),
        "compressed_size": len(framed),
        "ratio":          round(len(data) / max(len(framed), 1), 4),
        "savings_pct":    round((1 - len(framed) / max(len(data), 1)) * 100, 2),
        "latency_ms":     round(elapsed, 3),
        "algo":           ev.get("algo", "unknown"),
        "algo_id":        ev.get("algo_id", -1),
        "condition":      ev.get("condition", "unknown"),
        "data_profile":   ev.get("data_profile", "unknown"),
        "reason":         ev.get("reason", ""),
        "scores":         ev.get("scores", {}),
        "network":        ev.get("network", {}),
        "roundtrip_ok":   roundtrip_ok,
        "compressed_b64": base64.b64encode(framed).decode(),
        "timestamp":      time.time(),
    }

    _broadcast({"type": "compression", "data": result})
    return jsonify(result)


@app.route("/decompress", methods=["POST", "OPTIONS"])
def decompress():
    if request.method == "OPTIONS":
        return "", 204

    body   = request.get_json(force=True, silent=True) or {}
    b64    = body.get("compressed_b64", "")
    try:
        framed   = base64.b64decode(b64)
        original = _decomp.decompress(framed)
        return jsonify({
            "ok":            True,
            "original_size": len(original),
            "data":          original.decode("utf-8", errors="replace"),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/stats")
def stats():
    return jsonify(_compressor.stats())


@app.route("/recent")
def recent():
    n = int(request.args.get("n", 50))
    return jsonify(_compressor.recent(n))


@app.route("/network")
def network():
    snap = _probe.snapshot()
    cond = classify_condition(snap)
    return jsonify({
        **snap.to_dict(),
        "condition": cond.value,
    })


@app.route("/probe/analyze", methods=["POST"])
def probe_analyze():
    """Analyze data without compressing — return what the engine would decide."""
    body = request.get_json(force=True, silent=True) or {}
    raw  = body.get("data", "").encode("utf-8", errors="replace")
    if not raw:
        return jsonify({"error": "Empty"}), 400

    snap    = _probe.snapshot()
    cond    = classify_condition(snap)
    profile = profile_data(raw)

    return jsonify({
        "data_profile": profile.value,
        "condition":    cond.value,
        "network":      snap.to_dict(),
        "data_size":    len(raw),
    })


@app.route("/")
def index():
    return send_from_directory(".", "dashboard.html")


@sock.route("/ws")
def websocket(ws):
    """WebSocket endpoint — push live compression events."""
    with _ws_lock:
        _ws_clients.append(ws)
    log.info(f"WS client connected ({len(_ws_clients)} total)")

    # Send last 20 events on connect so dashboard has history
    for ev in list(_event_bus)[-20:]:
        try:
            ws.send(json.dumps(ev))
        except Exception:
            break

    # Send network snapshots periodically
    def _net_push():
        while ws in _ws_clients:
            try:
                snap = _probe.snapshot()
                cond = classify_condition(snap)
                ws.send(json.dumps({
                    "type":    "network",
                    "data":    {**snap.to_dict(), "condition": cond.value}
                }))
            except Exception:
                break
            time.sleep(2.0)

    t = threading.Thread(target=_net_push, daemon=True)
    t.start()

    try:
        while True:
            msg = ws.receive(timeout=30)
            if msg is None:
                break
    except Exception:
        pass
    finally:
        with _ws_lock:
            if ws in _ws_clients:
                _ws_clients.remove(ws)
        log.info(f"WS client disconnected ({len(_ws_clients)} remaining)")


def _network_broadcast_loop():
    """Background thread: broadcast network stats every 3 seconds."""
    while True:
        try:
            snap = _probe.snapshot()
            cond = classify_condition(snap)
            _broadcast({"type": "network", "data": {**snap.to_dict(), "condition": cond.value}})
        except Exception:
            pass
        time.sleep(3.0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RANCE Server")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    args = parser.parse_args()

    log.info("Starting RANCE engine...")
    _compressor.start()
    log.info("Real network probe started — measuring actual RTT...")

    threading.Thread(target=_network_broadcast_loop, daemon=True).start()

    log.info(f"RANCE server → http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=False, threaded=True)
