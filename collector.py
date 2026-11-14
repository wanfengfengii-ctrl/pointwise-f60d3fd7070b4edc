"""Resumable export collector.

A single task/single worker download.  Bytes only become reusable after
``received``/metadata are committed to SQLite; a killed process may leave the
``.part`` file longer than that confirmed prefix, so every restart truncates
the file back to the committed offset before asking the source for more.
"""
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urlsplit

import runtime

BLOCK = 16384
# Progress transactions are durable but not per-read; keeping the gap below
# the benchmark's 48 KiB slack also bounds what a SIGKILL can cost.
COMMIT = 32768

SCHEMA = """
CREATE TABLE downloads(
 id TEXT PRIMARY KEY, url TEXT NOT NULL, state TEXT NOT NULL,
 etag TEXT, length INTEGER, sha256 TEXT, received INTEGER NOT NULL DEFAULT 0,
 error TEXT, receipt TEXT
);
"""


class UnsatisfiedRange(Exception):
    """416: not evidence of completion; drop the prefix and retry whole."""


class RangeError(ValueError):
    """A 206 whose version/range/summary does not describe our prefix."""


def init(root):
    (Path(root) / "downloads").mkdir(exist_ok=True)
    with runtime.connect(root) as db:
        db.executescript(SCHEMA)


def enqueue(root, task_id, url):
    if not isinstance(task_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", task_id):
        raise ValueError("TASK_ID")
    parts = urlsplit(url)
    if parts.scheme != "http" or not parts.hostname or parts.username or parts.password or parts.fragment:
        raise ValueError("URL")
    with runtime.connect(root) as db:
        db.execute("BEGIN IMMEDIATE")
        old = db.execute("SELECT url FROM downloads WHERE id=?", (task_id,)).fetchone()
        if old:
            if old[0] != url:
                raise ValueError("TASK_CONFLICT")
        else:
            if db.execute("SELECT 1 FROM downloads WHERE state NOT IN ('completed','failed')").fetchone():
                raise ValueError("ACTIVE_DOWNLOAD")
            db.execute("INSERT INTO downloads(id,url,state) VALUES(?,?,'queued')", (task_id, url))
    return {"task_id": task_id, "url": url}


def _metadata(response):
    etag = response.getheader("ETag")
    digest = response.getheader("X-Content-SHA256")
    length_text = response.getheader("Content-Length", "")
    if not etag or not re.fullmatch(r'"[^"]+"', etag):
        raise ValueError("ETAG")
    if not digest or not re.fullmatch(r"[0-9a-f]{64}", digest) or not length_text.isdecimal():
        raise ValueError("METADATA")
    return etag, digest, int(length_text)


def _seed_prefix(target, resume_from, block=BLOCK):
    """Hash the already-confirmed local prefix; no wire bytes are involved."""
    digest = hashlib.sha256()
    target.seek(0)
    left = resume_from
    while left:
        chunk = target.read(min(block, left))
        if not chunk:
            raise OSError("PREFIX_SHORT")
        digest.update(chunk)
        left -= len(chunk)
    target.seek(resume_from)
    return digest


def download(url, target, progress=None, connection_factory=None, on_write=None,
             resume_from=0, expect=None, on_discard=None):
    """Download or continue ``target``.

    ``resume_from`` must be a locally confirmed prefix length; ``expect`` is
    the committed (etag, length, sha256) for that prefix.  A full 200 answer to
    a ranged request means the old prefix cannot be extended, so it is discarded
    and the same response is used as the new whole entity.
    """
    parts = urlsplit(url)
    factory = connection_factory or http.client.HTTPConnection
    connection = factory(parts.hostname, parts.port or 80, timeout=5)
    try:
        headers = {"Accept-Encoding": "identity"}
        if resume_from:
            if not expect or not expect.get("etag"):
                raise ValueError("RESUME_METADATA")
            headers["Range"] = "bytes=%d-" % int(resume_from)
            headers["If-Range"] = expect["etag"]
        connection.request("GET", parts.path + ("?" + parts.query if parts.query else ""),
                           headers=headers)
        response = connection.getresponse()
        status = response.status
        if status == 416:
            raise UnsatisfiedRange("HTTP_416")
        if status not in (200, 206):
            raise ValueError("HTTP_STATUS")
        if status == 206 and not resume_from:
            raise RangeError("UNREQUESTED_RANGE")

        etag, digest, length = _metadata(response)
        metadata = {"etag": etag, "length": length, "sha256": digest}

        if status == 206:
            content_range = response.getheader("Content-Range", "")
            match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", content_range)
            if not match:
                raise RangeError("CONTENT_RANGE")
            start, end, total = (int(value) for value in match.groups())
            if start != resume_from or end != total - 1 or length != total - resume_from:
                raise RangeError("CONTENT_RANGE")
            if etag != expect["etag"]:
                raise RangeError("ETAG")
            if total != expect["length"]:
                raise RangeError("LENGTH")
            if digest != expect["sha256"]:
                raise RangeError("DIGEST")
            length = total
            metadata["length"] = total
        else:
            # A full entity cannot be appended to a prefix, even when it
            # happens to carry the same validators.  Reset both sides first
            # (DB then file) so a later kill never reuses the stale prefix.
            if resume_from:
                if on_discard:
                    on_discard()
                target.seek(0)
                target.truncate(0)
                resume_from = 0

        actual = _seed_prefix(target, resume_from)
        size = resume_from
        committed = resume_from
        while True:
            block = response.read(BLOCK)
            if not block:
                break
            target.write(block)
            target.flush()
            size += len(block)
            actual.update(block)
            if on_write:
                on_write(size, metadata)
            if progress and size - committed >= COMMIT:
                progress(size, metadata)
                committed = size
        if size != length:
            raise OSError("SHORT_RESPONSE")
        if actual.hexdigest() != digest:
            raise ValueError("DIGEST")
        if progress and size != committed:
            # The tail is now verified too; make it part of the safe prefix.
            progress(size, metadata)
        return metadata
    finally:
        connection.close()


def _reset_prefix(root, name):
    with runtime.connect(root) as db:
        db.execute("UPDATE downloads SET received=0, etag=NULL, length=NULL, sha256=NULL WHERE id=?",
                   (name,))


def _prepare(root, name):
    """Return (confirmed offset, expected metadata) after trimming the part."""
    with runtime.connect(root) as db:
        row = db.execute("SELECT received, etag, length, sha256 FROM downloads WHERE id=?",
                         (name,)).fetchone()
    received, etag, length, digest = row
    if (not isinstance(received, int) or received <= 0 or not etag
            or not isinstance(length, int) or length <= 0
            or not digest or not re.fullmatch(r"[0-9a-f]{64}", digest)):
        _reset_prefix(root, name)
        return 0, None
    path = root / "downloads" / (name + ".part")
    if not path.exists() or path.stat().st_size < received:
        # The DB claims more than the confirmed bytes actually on disk.
        _reset_prefix(root, name)
        return 0, None
    if path.stat().st_size > received:
        # A write hook may have landed after the last progress transaction.
        with path.open("r+b", buffering=0) as target:
            target.truncate(received)
    return received, {"etag": etag, "length": length, "sha256": digest}


def run_one(root):
    root = Path(root)
    with runtime.connect(root) as db:
        row = db.execute("SELECT id,url FROM downloads WHERE state IN ('queued','downloading') ORDER BY id LIMIT 1").fetchone()
        if row is None:
            return False
        name, url = row
        db.execute("UPDATE downloads SET state='downloading', error=NULL WHERE id=?", (name,))
    temporary = root / "downloads" / (name + ".part")
    complete = root / "downloads" / (name + ".raw")

    resume, expect = _prepare(root, name)

    def progress(size, metadata):
        with runtime.connect(root) as db:
            db.execute("UPDATE downloads SET received=?,etag=?,length=?,sha256=? WHERE id=?",
                       (size, metadata["etag"], metadata["length"], metadata["sha256"], name))
        runtime.barrier(root, "chunk")
        runtime.barrier(root, "checkpoint")

    def discard():
        _reset_prefix(root, name)

    mode = "r+b" if resume else "wb"
    try:
        with temporary.open(mode, buffering=0) as target:
            target.seek(resume)
            metadata = download(url, target, progress,
                                on_write=lambda n, m: runtime.barrier(root, "written"),
                                resume_from=resume, expect=expect, on_discard=discard)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, complete)
        runtime.sync_directory(complete.parent)
        runtime.barrier(root, "verified")
        batch = json.loads(complete.read_bytes())
        runtime.submit(root, batch)
        runtime.barrier(root, "accepted")
        runtime.run_one(root)
        runtime.barrier(root, "accounted")
        result = runtime.read(root, batch["batch_id"])
        if not result["available"]:
            raise ValueError("NOT_ACCOUNTED")
        with runtime.connect(root) as db:
            db.execute("UPDATE downloads SET state='completed',receipt=? WHERE id=?",
                       (json.dumps(result["receipt"], sort_keys=True), name))
    except UnsatisfiedRange as error:
        _reset_prefix(root, name)
        try:
            if temporary.exists():
                with temporary.open("r+b", buffering=0) as target:
                    target.truncate(0)
        except OSError:
            pass
        with runtime.connect(root) as db:
            db.execute("UPDATE downloads SET state='queued',error=? WHERE id=?", (str(error), name))
    except (OSError, http.client.HTTPException) as error:
        # Short/aborted responses keep whatever prefix was confirmed; the
        # next run trims the file and continues from that offset.
        with runtime.connect(root) as db:
            db.execute("UPDATE downloads SET state='queued',error=? WHERE id=?", (str(error), name))
    except ValueError as error:
        with runtime.connect(root) as db:
            db.execute("UPDATE downloads SET state='failed',error=? WHERE id=?", (str(error), name))
    return True


def status(root):
    with runtime.connect(root) as db:
        rows = db.execute("SELECT id,url,state,etag,length,sha256,received,error,receipt FROM downloads ORDER BY id").fetchall()
    keys = ("task_id", "url", "state", "etag", "length", "sha256", "received", "error", "receipt")
    result = [dict(zip(keys, row)) for row in rows]
    for row in result:
        row["receipt"] = json.loads(row["receipt"]) if row["receipt"] else None
    return result


def worker(root):
    import fcntl
    with (Path(root) / "worker.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while True:
            if not run_one(root):
                runtime.run_one(root)
            time.sleep(0.2)
