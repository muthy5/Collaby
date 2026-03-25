"""
bridge_client.py — Python client for the brain.js ↔ scrape.py TCP bridge.

Usage in scrape.py:
    from bridge_client import ask_brain, notify_brain, is_bridge_connected

    # Blocking request/response
    response = ask_brain("captcha", {"source": "Zumper", "url": "https://..."})
    # response = {"action": "heal", "instructions": "..."} or None if bridge down

    # Fire-and-forget status update
    notify_brain("status", {"message": "Starting Craigslist scraper..."})

Falls back gracefully if bridge is not running — all calls return None.
"""
import socket
import json
import threading
import time

BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = 9400
CONNECT_TIMEOUT = 3
READ_TIMEOUT = 120  # Wait up to 2 min for brain to respond

_sock = None
_lock = threading.Lock()
_connected = False


def _connect():
    """Try to connect to the bridge server. Returns True if connected."""
    global _sock, _connected
    if _connected and _sock:
        return True
    try:
        _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _sock.settimeout(CONNECT_TIMEOUT)
        _sock.connect((BRIDGE_HOST, BRIDGE_PORT))
        _sock.settimeout(READ_TIMEOUT)
        _connected = True
        print("🔌 Bridge: connected to brain.js")
        return True
    except (ConnectionRefusedError, OSError, socket.timeout):
        _sock = None
        _connected = False
        return False


def _send(msg_dict):
    """Send a JSON message over the socket."""
    global _sock, _connected
    if not _sock:
        return False
    try:
        data = json.dumps(msg_dict) + "\n"
        _sock.sendall(data.encode("utf-8"))
        return True
    except (BrokenPipeError, OSError):
        _connected = False
        _sock = None
        return False


def _recv():
    """Read one newline-delimited JSON response from the socket."""
    global _sock, _connected
    if not _sock:
        return None
    buf = ""
    try:
        while True:
            chunk = _sock.recv(4096)
            if not chunk:
                _connected = False
                _sock = None
                return None
            buf += chunk.decode("utf-8", errors="ignore")
            if "\n" in buf:
                line = buf.split("\n")[0].strip()
                if line:
                    return json.loads(line)
                return None
    except (socket.timeout, OSError, json.JSONDecodeError):
        return None


def is_bridge_connected():
    """Check if the bridge is connected."""
    return _connected


def ask_brain(msg_type, payload=None):
    """
    Send a message to brain.js and wait for a response.
    Returns the response dict, or None if bridge is not running.
    Thread-safe.
    """
    with _lock:
        if not _connect():
            return None
        msg = {"type": msg_type}
        if payload:
            msg.update(payload)
        if not _send(msg):
            return None
        return _recv()


def notify_brain(msg_type, payload=None):
    """
    Send a fire-and-forget message to brain.js (no response expected).
    Returns True if sent, False if bridge is not running.
    """
    with _lock:
        if not _connect():
            return False
        msg = {"type": msg_type}
        if payload:
            msg.update(payload)
        return _send(msg)


def close_bridge():
    """Close the bridge connection."""
    global _sock, _connected
    with _lock:
        if _sock:
            try:
                _sock.close()
            except Exception:
                pass
        _sock = None
        _connected = False


# Try to connect on import (non-blocking, silent failure)
try:
    _connect()
except Exception:
    pass
