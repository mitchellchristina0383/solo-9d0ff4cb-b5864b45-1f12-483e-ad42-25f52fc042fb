"""Recovery across ungraceful restarts, interleaved with reclamation.

These tests start a real uvicorn process, SIGKILL it mid-recovery and restart
on the same SQLite file, exercising WAL durability of the recovery state.
"""

from __future__ import annotations

import base64
import hashlib
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

BLOCK = 8


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Server:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.port = _free_port()
        self.process: subprocess.Popen | None = None
        self.url = f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        env = {
            "PATH": "/usr/bin:/bin",
            "BLOCKSTORE_DB": str(self.db_path),
            "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
        }
        self.process = subprocess.Popen(
            [
                sys.executable, "-m", "uvicorn", "app.main:app",
                "--host", "127.0.0.1", "--port", str(self.port),
                "--log-level", "warning",
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                if httpx.get(f"{self.url}/healthz", timeout=0.5).status_code == 200:
                    return
            except httpx.HTTPError:
                time.sleep(0.1)
        self.kill()
        raise RuntimeError("server did not start")

    def kill(self) -> None:
        if self.process and self.process.poll() is None:
            os.kill(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=10)
        self.process = None

    def client(self) -> httpx.Client:
        return httpx.Client(base_url=self.url, timeout=30)


@pytest.fixture()
def server_factory(db_path: Path):
    servers: list[Server] = []

    def make() -> Server:
        server = Server(db_path)
        server.start()
        servers.append(server)
        return server

    yield make
    for server in servers:
        server.kill()


def test_recovery_resumes_after_sigkill(server_factory):
    blocks = [b"BACKUP1!", b"BACKUP2!", b"BACKUP3!"]
    first = server_factory()
    with first.client() as client:
        recovery = client.post(
            "/recoveries",
            json={
                "name": "dr-1",
                "volume_name": "restored-vol",
                "block_size": BLOCK,
                "manifest": [sha(b) for b in blocks],
            },
        ).json()
        recovery_id = recovery["id"]
        assert recovery["missing_count"] == 3
        # Upload one block, then SIGKILL before the rest.
        ack = client.put(
            f"/recoveries/{recovery_id}/blocks/0",
            json={"sha256": sha(blocks[0]), "data_base64": b64(blocks[0])},
        )
        assert ack.status_code == 200
    first.kill()

    second = server_factory()
    with second.client() as client:
        detail = client.get(f"/recoveries/{recovery_id}").json()
        # The acknowledged upload survived; the other two are still missing.
        assert detail["blocks_uploaded"] == 1
        assert detail["blocks_present"] == 1
        assert set(detail["missing_digests"]) == {sha(blocks[1]), sha(blocks[2])}

        # Commit still fails with the refreshed missing list; data retained.
        incomplete = client.post(f"/recoveries/{recovery_id}/commit")
        assert incomplete.status_code == 409
        assert incomplete.json()["code"] == "recovery_incomplete"

        for index in (1, 2):
            client.put(
                f"/recoveries/{recovery_id}/blocks/{index}",
                json={"sha256": sha(blocks[index]),
                      "data_base64": b64(blocks[index])},
            )
        commit = client.post(f"/recoveries/{recovery_id}/commit")
        assert commit.status_code == 200, commit.text
        volume_id = commit.json()["id"]
    second.kill()

    third = server_factory()
    with third.client() as client:
        # The published volume and its content survive another restart.
        for index, expected in enumerate(blocks):
            response = client.get(f"/volumes/{volume_id}/blocks/{index}")
            assert base64.b64decode(response.json()["data_base64"]) == expected
        assert client.post(f"/recoveries/{recovery_id}/commit").json()[
            "volume_id"
        ] == volume_id
        assert client.get("/verify").json()["ok"] is True


def test_recovery_interleaved_with_reclamation_across_restarts(server_factory):
    blocks = [b"BACKUP1!", b"BACKUP2!"]
    first = server_factory()
    with first.client() as client:
        # The store already holds the backup content via a base + volume, so
        # opening the recovery transfers nothing.
        base = client.post(
            "/bases",
            json={
                "name": "base",
                "block_size": BLOCK,
                "blocks_base64": [b64(b) for b in blocks],
            },
        ).json()
        volume = client.post(
            "/volumes", json={"name": "vol", "base_id": base["id"]}
        ).json()
        recovery = client.post(
            "/recoveries",
            json={
                "name": "dr-2",
                "volume_name": "restored-vol-2",
                "block_size": BLOCK,
                "manifest": [sha(b) for b in blocks],
            },
        ).json()
        assert recovery["blocks_missing"] == 0
        recovery_id = recovery["id"]
    first.kill()  # restart before any upload: store-matched state is derived

    second = server_factory()
    with second.client() as client:
        detail = client.get(f"/recoveries/{recovery_id}").json()
        assert detail["status"] == "recovering"
        assert detail["blocks_missing"] == 0

        # Reclaim everything the recovery was only matching through the store.
        client.post(f"/bases/{base['id']}/retirement/prepare")
        client.post(f"/bases/{base['id']}/retirement/commit")
        client.delete(f"/volumes/{volume['id']}")

        detail = client.get(f"/recoveries/{recovery_id}").json()
        assert detail["blocks_missing"] == 2
        assert set(detail["missing_digests"]) == {sha(blocks[0]), sha(blocks[1])}

        # Re-upload just the first block from the backup, then die again.
        ack = client.put(
            f"/recoveries/{recovery_id}/blocks/0",
            json={"sha256": sha(blocks[0]), "data_base64": b64(blocks[0])},
        )
        assert ack.status_code == 200
        stats = ack.json()
        assert stats["blocks_present"] == 1
    second.kill()

    third = server_factory()
    with third.client() as client:
        detail = client.get(f"/recoveries/{recovery_id}").json()
        # The uploaded block is pinned across the restart; the second is missing.
        assert detail["blocks_uploaded"] == 1
        assert detail["missing_digests"] == [sha(blocks[1])]
        assert client.get("/stats").json()["payloads"]["distinct"] == 1

        client.put(
            f"/recoveries/{recovery_id}/blocks/1",
            json={"sha256": sha(blocks[1]), "data_base64": b64(blocks[1])},
        )
        commit = client.post(f"/recoveries/{recovery_id}/commit")
        assert commit.status_code == 200
        volume_id = commit.json()["id"]
        for index, expected in enumerate(blocks):
            response = client.get(f"/volumes/{volume_id}/blocks/{index}")
            assert base64.b64decode(response.json()["data_base64"]) == expected
        assert client.get("/verify").json()["ok"] is True


def test_abandoned_uploads_are_reclaimed_after_restart(server_factory):
    first = server_factory()
    with first.client() as client:
        recovery = client.post(
            "/recoveries",
            json={
                "name": "dr-3",
                "volume_name": "restored-vol-3",
                "block_size": BLOCK,
                "manifest": [sha(b"BACKUP1!")],
            },
        ).json()
        client.put(
            f"/recoveries/{recovery['id']}/blocks/0",
            json={"sha256": sha(b"BACKUP1!"), "data_base64": b64(b"BACKUP1!")},
        )
        assert client.get("/stats").json()["payloads"]["distinct"] == 1
    first.kill()

    second = server_factory()
    with second.client() as client:
        result = client.delete(f"/recoveries/{recovery['id']}")
        assert result.status_code == 200
        assert result.json()["payloads_reclaimed"] == 1
        assert client.get("/stats").json()["free_slots"]["count"] == 1
        assert client.get("/verify").json()["ok"] is True
