"""Independent boundary checks for resumable collection.

Runs real child processes over the recorded portal for the restart cases and
exercises the resume validators directly for malformed 206 replies.  It does
not change the frozen sources, workloads, meters or checks.py.
"""
import hashlib
import http.client
import io
import json
from pathlib import Path
import tempfile

import bench
import checks
import collector
import portal
import runtime

MIB = 1024 * 1024


def fake(status, headers, body):
    """A connection whose response is exactly the given recorded bytes."""
    class Connection:
        def __init__(self, *args, **kwargs):
            pass

        def request(self, method, path, body=None, headers=None):
            self.method = method
            self.headers = {k.lower(): v for k, v in (headers or {}).items()}

        def getresponse(self):
            head = ("HTTP/1.1 %d X\r\n" % status).encode()
            head += b"".join(("%s: %s\r\n" % pair).encode() for pair in headers.items())
            head += b"\r\n"

            class Socket:
                def makefile(self, *args):
                    return io.BytesIO(head + body)

            response = http.client.HTTPResponse(Socket(), method=self.method)
            response.begin()
            return response

        def close(self):
            pass

    return Connection


def validator_cases():
    raw = bench.raw_version(1)
    digest = hashlib.sha256(raw).hexdigest()
    expect = {"etag": '"portal-1"', "length": len(raw), "sha256": digest}
    suffix = raw[1024:]
    common = {"ETag": '"portal-1"', "X-Content-SHA256": digest,
              "Content-Length": str(len(suffix))}
    results = {}

    def attempt(status, headers, resume, prefix=b""):
        target = io.BytesIO(prefix)
        target.seek(resume)
        try:
            metadata = collector.download("http://recording/export", target,
                                          connection_factory=fake(status, headers, suffix),
                                          resume_from=resume, expect=expect)
        except ValueError as error:
            return {"rejected": str(error)}
        return {"accepted": metadata, "bytes": len(target.getvalue())}

    # A 206 we did not ask for may never be treated as a prefix.
    unrequested = dict(common, **{"Content-Range": "bytes 0-8388607/8388608"})
    results["unrequested_206"] = attempt(206, unrequested, 0)
    assert results["unrequested_206"].get("rejected") == "UNREQUESTED_RANGE"

    # Correct headers and body: the only accepted 206.
    good = dict(common, **{"Content-Range": "bytes 1024-8388607/8388608"})
    results["valid_206"] = attempt(206, good, 1024, prefix=raw[:1024])
    assert results["valid_206"]["bytes"] == len(raw)
    assert results["valid_206"]["accepted"] == expect

    for name, change, expected in (
            ("wrong_end", {"Content-Range": "bytes 1024-8388606/8388608"}, "CONTENT_RANGE"),
            ("wrong_start", {"Content-Range": "bytes 7-8388614/8388608"}, "CONTENT_RANGE"),
            ("wrong_total", {"Content-Range": "bytes 1024-8999999/9000000",
                             "Content-Length": "8998976"}, "LENGTH"),
            ("wrong_length", {"Content-Length": str(len(suffix) - 1)}, "CONTENT_RANGE")):
        headers = dict(good)
        headers.update(change)
        results[name] = attempt(206, headers, 1024, prefix=raw[:1024])
        assert results[name].get("rejected") == expected, (name, results[name])

    bad_digest = dict(good, **{"X-Content-SHA256": "0" * 64})
    results["wrong_digest"] = attempt(206, bad_digest, 1024, prefix=raw[:1024])
    assert results["wrong_digest"].get("rejected") == "DIGEST", results["wrong_digest"]

    bad_etag = dict(good, **{"ETag": '"portal-2"'})
    results["wrong_etag"] = attempt(206, bad_etag, 1024, prefix=raw[:1024])
    assert results["wrong_etag"].get("rejected") == "ETAG", results["wrong_etag"]
    return results


def ledger_empty(root):
    with runtime.connect(root) as db:
        assert db.execute("SELECT COUNT(*) FROM jobs").fetchone() == (0,)
        assert db.execute("SELECT COUNT(*) FROM ledger").fetchone() == (0,)


def row(root):
    return collector.status(root)[0]


def assert_complete(root, version):
    raw = bench.raw_version(version)
    assert (root / "downloads" / "export.raw").read_bytes() == raw
    status = row(root)
    assert status["state"] == "completed"
    assert status["length"] == len(raw)
    assert status["sha256"] == hashlib.sha256(raw).hexdigest()
    report = checks.installed(root, json.loads(raw))
    assert status["receipt"] == runtime.read(root, "sample")["receipt"]
    return report


def scenarios(base):
    out = {}

    # 1. Two truncations then completion; every restart continues the prefix.
    root = bench.create(base / "resume")
    total = 0
    seen = []
    for _ in range(2):
        attempt = bench.invoke(root, cut=MIB)
        total += attempt["bytes"]
        seen.extend(r["range"] for r in attempt["requests"])
        assert row(root)["state"] == "queued"
        assert row(root)["received"] == total
        ledger_empty(root)
    attempt = bench.invoke(root)
    total += attempt["bytes"]
    seen.extend(r["range"] for r in attempt["requests"])
    assert attempt["requests"][0]["range"] == "bytes=%d-" % (2 * MIB)
    assert attempt["requests"][0]["if_range"] == '"portal-1"'
    assert_complete(root, 1)
    assert seen == [None, "bytes=1048576-", "bytes=2097152-"], seen
    out["resume"] = {"response_body_bytes": total}

    # 2. Version switch after a truncated version 1: full 200 drops the prefix.
    root = bench.create(base / "changed")
    first = bench.invoke(root, cut=MIB)
    assert row(root)["state"] == "queued"
    second = bench.invoke(root, version=2)
    assert second["requests"][0]["status"] == 200
    assert second["requests"][0]["range"] == "bytes=%d-" % MIB
    assert_complete(root, 2)
    assert checks.expected(json.loads(bench.raw_version(2))) == [
        {"station": "east", "minute": 0, "count": 2, "sum": 0},
        {"station": "west", "minute": 1, "count": 1, "sum": 110}]
    out["changed"] = {"response_body_bytes": first["bytes"] + second["bytes"]}

    # 3. A complete-looking prefix already at the entity end yields 416.
    root = bench.create(base / "unsatisfied")
    raw = bench.raw_version(1)
    (root / "downloads" / "export.part").write_bytes(raw)
    with runtime.connect(root) as db:
        db.execute("UPDATE downloads SET received=?,etag=?,length=?,sha256=? WHERE id='export'",
                   (len(raw), '"portal-1"', len(raw), hashlib.sha256(raw).hexdigest()))
    missed = bench.invoke(root)
    assert missed["bytes"] == 0
    assert missed["requests"][0]["status"] == 416
    assert row(root)["state"] == "queued" and row(root)["received"] == 0
    assert row(root)["error"] == "HTTP_416"
    ledger_empty(root)
    finished = bench.invoke(root)
    assert finished["requests"][0]["range"] is None
    assert_complete(root, 1)
    out["unsatisfied"] = {"response_body_bytes": missed["bytes"] + finished["bytes"]}

    # 4. An untrusted 206 range is rejected before any byte is accepted.
    root = bench.create(base / "range")
    first = bench.invoke(root, cut=MIB)
    bad = bench.invoke(root, fault="range")
    assert bad["bytes"] == 0
    assert row(root)["state"] == "failed"
    assert row(root)["error"] == "CONTENT_RANGE"
    ledger_empty(root)
    assert not (root / "downloads" / "export.raw").exists()
    out["range"] = {"response_body_bytes": first["bytes"] + bad["bytes"]}

    # 5. A digest that disagrees with the stored summary is never submitted.
    root = bench.create(base / "digest_after")
    first = bench.invoke(root, cut=MIB)
    bad = bench.invoke(root, fault="digest")
    assert bad["bytes"] == 0
    assert row(root)["state"] == "failed"
    ledger_empty(root)
    assert not (root / "downloads" / "export.raw").exists()
    out["digest_after"] = {"response_body_bytes": first["bytes"] + bad["bytes"]}

    # 6. Full 200 with a wrong summary must not be published or accounted.
    root = bench.create(base / "digest_full")
    bad = bench.invoke(root, fault="digest")
    assert bad["bytes"] == len(bench.raw_version(1))
    assert row(root)["state"] == "failed" and row(root)["error"] == "DIGEST"
    ledger_empty(root)
    assert not (root / "downloads" / "export.raw").exists()
    out["digest_full"] = {"response_body_bytes": bad["bytes"]}

    # 7/8. SIGKILL on both sides of the progress commit.
    for phase in ("written", "checkpoint"):
        root = bench.create(base / phase)
        first = bench.invoke(root, cut=MIB)
        killed = bench.invoke(root, kill_phase=phase)
        part = root / "downloads" / "export.part"
        confirmed = row(root)["received"]
        size = part.stat().st_size
        if phase == "written":
            assert confirmed == MIB and size > confirmed
        else:
            assert confirmed > MIB and size == confirmed
        ledger_empty(root)
        rest = bench.invoke(root)
        assert rest["requests"][0]["range"] == "bytes=%d-" % confirmed
        assert_complete(root, 1)
        out[phase] = {"response_body_bytes": first["bytes"] + killed["bytes"] + rest["bytes"],
                      "confirmed_at_kill": confirmed, "file_at_kill": size,
                      "resume_range": rest["requests"][0]["range"]}

    # 9. Completed tasks never transfer again and account once.
    root = bench.create(base / "repeat")
    bench.invoke(root)
    again = bench.invoke(root)
    assert again["bytes"] == 0 and not again["requests"]
    assert_complete(root, 1)
    with runtime.connect(root) as db:
        assert db.execute("SELECT COUNT(*) FROM ledger").fetchone() == (1,)
    out["repeat"] = {"response_body_bytes": again["bytes"]}
    return out


def main():
    validators = validator_cases()
    with tempfile.TemporaryDirectory(prefix="boundary-") as temporary:
        scenarios_ = scenarios(Path(temporary))
    print(json.dumps({"validators": validators, "scenarios": scenarios_,
                      "business": checks.normal()["regression"],
                      "source": checks.source()["regression"]}, sort_keys=True))


if __name__ == "__main__":
    main()
