"""Recorded HTTP wire benchmark; subprocesses use real files and SQLite."""
import hashlib
import http.client
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import checks
import collector
import pipeline
import portal
import runtime

MIB = 1024 * 1024
LIMIT = 8 * MIB + 48 * 1024


def raw_version(version):
    batch = checks.small()
    if version == 2:
        batch["rows"][2]["value"] = 11
    body = pipeline.encode(batch)
    return body + b" " * (8 * MIB - len(body))


def recording(version, cut, fault, emit):
    fixture = portal.Portal()
    fixture.versions = {v: raw_version(v) for v in (1, 2)}
    fixture.version = version
    fixture.cut_after = cut
    fixture.bad_digest = fault == "digest"

    class Wire(io.BytesIO):
        def __init__(self, data, boundary):
            super().__init__(data)
            self.boundary = boundary

        def counted(self, before):
            count = max(0, self.tell() - self.boundary) - max(0, before - self.boundary)
            if count:
                emit({"body_bytes": count})

        def read(self, size=-1):
            before = self.tell()
            value = super().read(size)
            self.counted(before)
            return value

        def readline(self, size=-1):
            before = self.tell()
            value = super().readline(size)
            self.counted(before)
            return value

        def readinto(self, buffer):
            before = self.tell()
            value = super().readinto(buffer)
            self.counted(before)
            return value

        def read1(self, size=-1):
            return self.read(size)

    class Socket:
        def __init__(self, wire):
            self.wire = wire

        def makefile(self, *args):
            return self.wire

    class Connection:
        def __init__(self, *args, **kwargs):
            pass

        def request(self, method, path, body=None, headers=None, **kwargs):
            self.method, self.path = method, path
            self.headers = {k.lower(): v for k, v in (headers or {}).items()}

        def getresponse(self):
            status, headers, body = fixture.reply(self.method, self.path, self.headers)
            if fault == "range" and self.method == "GET":
                status = 206
                headers["Content-Range"] = "bytes 7-8388614/8388608"
            emit({"request": {"method": self.method, "status": status,
                              "range": self.headers.get("range"),
                              "if_range": self.headers.get("if-range")}})
            head = ("HTTP/1.1 %d Recorded\r\n" % status).encode()
            head += b"".join(("%s: %s\r\n" % pair).encode() for pair in headers.items())
            head += b"\r\n"
            response = http.client.HTTPResponse(Socket(Wire(head + body, len(head))),
                                                method=self.method)
            response.begin()
            return response

        def close(self):
            pass

    return Connection


def probe():
    events = []
    total = 0
    for cut in (MIB, MIB, MIB, None):
        output = io.BytesIO()
        connection = recording(1, cut, None, events.append)
        try:
            collector.download("http://recording/export", output,
                               connection_factory=connection)
        except (OSError, http.client.HTTPException):
            assert cut is not None
        else:
            assert cut is None and output.getvalue() == raw_version(1)
        total += len(output.getvalue())
    wire = sum(item.get("body_bytes", 0) for item in events)
    assert wire == total == 11 * MIB
    return {"probe": "WHOLE_RESPONSE_BASELINE_OK", "response_body_bytes": wire,
            "final_bytes": 8 * MIB, "target_limit": LIMIT}


def child(root, version, cut, fault):
    emit = lambda value: print(json.dumps(value), flush=True)
    http.client.HTTPConnection = recording(version, cut, fault, emit)
    collector.run_one(root)
    emit({"download": collector.status(root)[0]})


def invoke(root, version=1, cut=None, fault=None, kill_phase=None):
    argv = [sys.executable, "-B", str(Path(__file__).resolve()), "_child",
            str(root), str(version), str(cut) if cut is not None else "-", fault or "-"]
    control = root.parent / "control"
    if kill_phase:
        (control / "hook.json").write_text(json.dumps({"phase": kill_phase}))
    process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    # Continuously drain output; otherwise a child can block on the metrics pipe.
    import threading
    lines = []
    drain = threading.Thread(target=lambda: lines.extend(process.stdout), daemon=True)
    drain.start()
    try:
        if kill_phase:
            ready = control / (kill_phase + ".ready")
            deadline = time.monotonic() + 20
            while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.01)
            assert ready.exists(), "HOOK_NOT_REACHED"
            assert int(ready.read_text()) == process.pid
            process.kill()
        process.wait(timeout=30)
        drain.join(timeout=5)
        assert not drain.is_alive(), "OUTPUT_NOT_DRAINED"
        stderr = process.stderr.read()
        assert process.returncode == (-9 if kill_phase else 0), stderr
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        drain.join(timeout=5)
        process.stdout.close()
        process.stderr.close()
        if kill_phase:
            (control / "hook.json").unlink(missing_ok=True)
    events = [json.loads(line) for line in lines]
    return {"bytes": sum(e.get("body_bytes", 0) for e in events),
            "requests": [e["request"] for e in events if "request" in e],
            "killed": kill_phase}


def create(directory):
    root, control = directory / "data", directory / "control"
    runtime.init(root, control)
    collector.init(root)
    collector.enqueue(root, "export", "http://recording/export")
    return root


def empty_ledger(root):
    with runtime.connect(root) as db:
        assert db.execute("SELECT COUNT(*) FROM ledger").fetchone() == (0,)
        assert db.execute("SELECT COUNT(*) FROM jobs").fetchone() == (0,)


def completed(root, version):
    raw = raw_version(version)
    assert (root / "downloads" / "export.raw").read_bytes() == raw, "RAW_MISMATCH"
    status = collector.status(root)[0]
    assert status["state"] == "completed"
    assert status["etag"] == '"portal-%d"' % version
    assert status["length"] == len(raw)
    assert status["sha256"] == hashlib.sha256(raw).hexdigest()
    report = checks.installed(root, json.loads(raw))
    assert status["receipt"] == runtime.read(root, "sample")["receipt"]
    return report


def suite(mode):
    reports = {}
    with tempfile.TemporaryDirectory(prefix="prefix-bench-") as temporary:
        base = Path(temporary)
        for name in ("normal", "stable", "changed", "written", "checkpoint", "range", "digest"):
            root = create(base / name)
            attempts = []
            if name in ("stable", "changed", "written", "checkpoint", "range"):
                count = 3 if name == "stable" else 1
                for _ in range(count):
                    attempts.append(invoke(root, cut=MIB))
                    assert collector.status(root)[0]["state"] == "queued"
                    empty_ledger(root)
            if name in ("written", "checkpoint"):
                attempts.append(invoke(root, kill_phase=name))
                empty_ledger(root)
            version = 2 if name == "changed" else 1
            fault = name if name in ("range", "digest") else None
            attempts.append(invoke(root, version=version, fault=fault))
            if fault:
                empty_ledger(root)
                assert collector.status(root)[0]["state"] != "completed"
                evidence = {"rejected": True}
            else:
                evidence = completed(root, version)
                # A second invocation must not perform another download/accounting.
                again = invoke(root, version=version)
                assert again["bytes"] == 0
                completed(root, version)
            amount = sum(a["bytes"] for a in attempts)
            if mode == "baseline" and name == "stable":
                assert amount == 11 * MIB
            if mode == "target" and name in ("stable", "written", "checkpoint"):
                assert amount <= LIMIT, ("RETRANSFER_BUDGET", name, amount, LIMIT)
            reports[name] = {"response_body_bytes": amount, "attempts": attempts,
                             "evidence": evidence}
    return {"mode": mode, "limit": LIMIT, "business": checks.normal(),
            "source": checks.source(), "cases": reports}


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "baseline"
    if mode == "_child":
        child(Path(sys.argv[2]), int(sys.argv[3]),
              None if sys.argv[4] == "-" else int(sys.argv[4]),
              None if sys.argv[5] == "-" else sys.argv[5])
    elif mode == "probe":
        print(json.dumps(probe(), sort_keys=True))
    elif mode in ("baseline", "target"):
        print(json.dumps(suite(mode), sort_keys=True))
    else:
        raise SystemExit("usage: bench.py baseline|target|probe")


if __name__ == "__main__":
    main()
