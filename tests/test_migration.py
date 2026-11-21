"""Migration of a version-1 database to the recovery-enabled schema v2."""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from app.db import SCHEMA_VERSION
from app.main import create_app


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

# The original v1 DDL, kept here verbatim so the test builds a genuine old
# database instead of downgrading a current one.
V1_SCHEMA = """
CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE bases (
    id                     TEXT PRIMARY KEY,
    name                   TEXT NOT NULL UNIQUE,
    block_size             INTEGER NOT NULL CHECK (block_size > 0),
    block_count            INTEGER NOT NULL CHECK (block_count > 0),
    status                 TEXT NOT NULL
                           CHECK (status IN ('active', 'retiring', 'retired')),
    created_at             TEXT NOT NULL,
    retirement_prepared_at TEXT,
    retired_at             TEXT,
    CHECK ((status = 'retired') = (retired_at IS NOT NULL)),
    CHECK (status <> 'active' OR retirement_prepared_at IS NULL)
);
CREATE TABLE payloads (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    block_size   INTEGER NOT NULL CHECK (block_size > 0),
    content_hash TEXT,
    data         BLOB NOT NULL,
    refcount     INTEGER NOT NULL DEFAULT 0 CHECK (refcount >= 0),
    free         INTEGER NOT NULL DEFAULT 0 CHECK (free IN (0, 1)),
    reuse_count  INTEGER NOT NULL DEFAULT 0 CHECK (reuse_count >= 0),
    created_at   TEXT NOT NULL,
    freed_at     TEXT,
    CHECK (length(data) = block_size),
    CHECK ((free = 1) = (content_hash IS NULL))
);
CREATE UNIQUE INDEX idx_payload_content
    ON payloads(block_size, content_hash) WHERE free = 0;
CREATE INDEX idx_payload_free ON payloads(block_size, free);
CREATE TABLE base_blocks (
    base_id     TEXT NOT NULL REFERENCES bases(id) ON DELETE CASCADE,
    block_index INTEGER NOT NULL CHECK (block_index >= 0),
    payload_id  INTEGER NOT NULL REFERENCES payloads(id),
    PRIMARY KEY (base_id, block_index)
);
CREATE TABLE volumes (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    base_id     TEXT REFERENCES bases(id) ON DELETE SET NULL,
    base_name   TEXT NOT NULL,
    block_size  INTEGER NOT NULL CHECK (block_size > 0),
    block_count INTEGER NOT NULL CHECK (block_count > 0),
    created_at  TEXT NOT NULL
);
CREATE TABLE volume_blocks (
    volume_id   TEXT NOT NULL REFERENCES volumes(id) ON DELETE CASCADE,
    block_index INTEGER NOT NULL CHECK (block_index >= 0),
    payload_id  INTEGER NOT NULL REFERENCES payloads(id),
    origin      TEXT NOT NULL CHECK (origin IN ('base', 'written', 'zeroed')),
    PRIMARY KEY (volume_id, block_index)
);
CREATE TABLE reclaim_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    payload_id   INTEGER NOT NULL,
    block_size   INTEGER NOT NULL,
    data_bytes   INTEGER NOT NULL,
    reason       TEXT NOT NULL,
    reclaimed_at TEXT NOT NULL,
    reused_at    TEXT
);
CREATE INDEX idx_volumes_base ON volumes(base_id);
CREATE INDEX idx_base_blocks_payload ON base_blocks(payload_id);
CREATE INDEX idx_volume_blocks_payload ON volume_blocks(payload_id);
CREATE INDEX idx_reclaim_events_time ON reclaim_events(reclaimed_at);
"""


def _build_v1_database(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(V1_SCHEMA)
        conn.execute("INSERT INTO meta(key, value) VALUES('schema_version', '1')")
        conn.execute(
            "INSERT INTO bases(id, name, block_size, block_count, status, created_at) "
            "VALUES('b1', 'old-base', 8, 1, 'active', '2026-01-01T00:00:00+00:00')"
        )
        conn.execute(
            "INSERT INTO payloads(id, block_size, content_hash, data, refcount, free, "
            "created_at) VALUES(1, 8, ?, ?, 2, 0, '2026-01-01T00:00:00+00:00')",
            (hashlib.sha256(b"AAAAAAAA").hexdigest(), b"AAAAAAAA"),
        )
        conn.execute(
            "INSERT INTO base_blocks(base_id, block_index, payload_id) "
            "VALUES('b1', 0, 1)"
        )
        conn.execute(
            "INSERT INTO volumes(id, name, base_id, base_name, block_size, "
            "block_count, created_at) VALUES('v1', 'old-vol', 'b1', 'old-base', "
            "8, 1, '2026-01-01T00:00:00+00:00')"
        )
        conn.execute(
            "INSERT INTO volume_blocks(volume_id, block_index, payload_id, origin) "
            "VALUES('v1', 0, 1, 'base')"
        )
        conn.commit()
    finally:
        conn.close()


def test_v1_database_migrates_and_keeps_data(tmp_path):
    db_path = tmp_path / "v1.db"
    _build_v1_database(db_path)

    with TestClient(create_app(str(db_path), busy_timeout=10)) as client:
        # Schema upgraded.
        from app.db import Database

        database = Database(str(db_path))
        assert database.schema_version() == SCHEMA_VERSION
        database.close_thread_connection()

        # Existing base, volume and payload survived untouched.
        base = client.get("/bases/b1").json()
        assert base["name"] == "old-base"
        assert base["block_count"] == 1
        volume = client.get("/volumes/v1").json()
        assert volume["blocks_by_origin"] == {"base": 1}
        response = client.get("/volumes/v1/blocks/0")
        assert response.json()["data_base64"] == "QUFBQUFBQUE="
        assert client.get("/verify").json()["ok"] is True

        # The new origin value and recovery tables are usable.
        recovery = client.post(
            "/recoveries",
            json={
                "name": "rec-on-migrated-db",
                "volume_name": "new-restored",
                "block_size": 8,
                "manifest": [sha(b"AAAAAAAA")],
            },
        )
        assert recovery.status_code == 201, recovery.text
        # The block already exists in the migrated store -> nothing missing.
        assert recovery.json()["blocks_missing"] == 0
        commit = client.post(f"/recoveries/{recovery.json()['id']}/commit")
        assert commit.status_code == 200
        restored = commit.json()
        assert restored["blocks_by_origin"] == {"restored": 1}
        assert client.get("/verify").json()["ok"] is True


def test_migration_is_idempotent_on_reopen(tmp_path):
    db_path = tmp_path / "v1.db"
    _build_v1_database(db_path)
    for _ in range(2):
        with TestClient(create_app(str(db_path), busy_timeout=10)) as client:
            assert client.get("/healthz").status_code == 200
            assert client.get("/verify").json()["ok"] is True
