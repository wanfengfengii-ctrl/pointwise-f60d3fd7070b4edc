"""Real-socket smoke: app.py CLI over portal.py, including resume and version switch.

Run from the repository root: python3 -B smoke.py
"""
import http.client
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
from http.server import ThreadingHTTPServer
from urllib.parse import urlsplit

import portal
import pipeline
import hashlib

PY = [sys.executable, "-B"]


def cli(*args):
    out = subprocess.run(PY + ["app.py", *args], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def control(url, payload):
    parts = urlsplit(url)
    body = json.dumps(payload).encode()
    connection = http.client.HTTPConnection(parts.hostname, parts.port, timeout=5)
    connection.request("POST", "/control", body=body,
                       headers={"Content-Type": "application/json",
                                "Content-Length": str(len(body))})
    response = connection.getresponse()
    response.read()
    connection.close()
    assert response.status == 200


def main():
    fixture = portal.Portal()
    server = ThreadingHTTPServer(("127.0.0.1", 0), portal.Handler)
    server.daemon_threads = True
    server.portal = fixture
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = "http://127.0.0.1:%d/export" % server.server_port
    temporary = Path(tempfile.mkdtemp(prefix="smoke-"))
    try:
        root, ctl = temporary / "data", temporary / "control"
        cli("init", str(root), str(ctl))
        cli("enqueue", str(root), "export", url)

        control(url, {"cut_after": 262144 - 100000})
        cli("run", str(root))
        state = cli("downloads", str(root))[0]
        print("after_cut", state["state"], state["received"], state["error"])
        # Truncated at 162144; the trailing partial block was never committed.
        assert state["state"] == "queued" and 0 < state["received"] <= 162144
        assert state["received"] % 32768 == 0
        confirmed = state["received"]

        cli("run", str(root))
        state = cli("downloads", str(root))[0]
        print("after_resume", state["state"], state["received"], state["etag"])
        assert state["state"] == "completed" and state["length"] == 262144
        first = (root / "downloads" / "export.raw").read_bytes()
        assert first == fixture.versions[1]

        # Version switch on a separate repository: truncated v1 prefix, then v2.
        root2, ctl2 = temporary / "data2", temporary / "control2"
        cli("init", str(root2), str(ctl2))
        cli("enqueue", str(root2), "export", url)
        control(url, {"version": 1, "cut_after": 100000})
        cli("run", str(root2))
        pre = cli("downloads", str(root2))[0]
        print("v1_prefix", pre["state"], pre["received"], pre["etag"])
        assert pre["state"] == "queued" and 0 < pre["received"] <= 100000
        control(url, {"version": 2})
        cli("run", str(root2))
        state = cli("downloads", str(root2))[0]
        print("after_v2", state["state"], state["received"], state["etag"])
        second = (root2 / "downloads" / "export.raw").read_bytes()
        assert second == fixture.versions[2]
        assert state["state"] == "completed" and state["etag"] == '"portal-2"'
        print("version2_raw_sha256", hashlib.sha256(second).hexdigest())
        read = cli("read", str(root2), "sample")
        print("read", read["available"], read["result"])
        assert read["result"][1]["sum"] == 110
        states = cli("downloads", str(root))
        assert len(states) == 1 and states[0]["state"] == "completed"
        print("SMOKE_OK")
    finally:
        server.shutdown()
        server.server_close()
        shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
