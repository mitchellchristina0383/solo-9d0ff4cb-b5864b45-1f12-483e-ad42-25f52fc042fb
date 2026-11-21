"""Core block-store logic.

The module is intentionally independent of FastAPI: every operation takes and
returns plain dictionaries and raises :class:`ServiceError` for domain errors.
Routes translate those into HTTP responses.

Key invariants
--------------
1. A ``payloads`` row is the single stored copy of a block's bytes.
   ``refcount`` equals the number of ``base_blocks`` rows plus the number of
   ``volume_blocks`` rows plus the number of linked ``recovery_blocks`` rows
   pointing at it.
2. Every volume block always points at a payload.  Volumes therefore never
   need the base to serve a read, which is what makes base removal safe.
3. References are only dropped inside the same transaction that removes the
   referring row, so a crash can never leave refcounts inconsistent.
4. Unreferenced payloads are deleted immediately and a ``reclaim_events`` row
   records the released bytes.  SQLite puts their pages on the freelist, where
   the next writes reuse them.
5. A recovery pins only the blocks the client explicitly uploads.  Blocks
   merely matched against content already in the store hold no reference, so
   ordinary reclamation can remove them; the missing list and the commit are
   recomputed from live payloads, and the volume is published atomically only
   when the whole manifest resolves inside one write transaction.
"""

from __future__ import annotations

import base64
import hashlib
import re
import sqlite3
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Sequence

from .db import Database

MAX_BLOCK_SIZE = 1024 * 1024          # 1 MiB per block
MAX_BLOCK_COUNT = 65536               # 65 536 blocks per base or recovery
MAX_BASE_BYTES = 256 * 1024 * 1024    # 256 MiB per base image
SHA256_HEX_LENGTH = 64                # SHA-256 digest rendered as lowercase hex
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
# Restored volumes have no base; the provenance column is kept NOT NULL with
# this sentinel rather than relaxing the schema.
RESTORED_BASE_NAME = ""
RESTORED_ORIGIN = "restored"


class ServiceError(Exception):
    """Domain error carrying the HTTP status the API should return."""

    def __init__(
        self,
        status_code: int,
        detail: str,
        code: str = "error",
        extra: dict | None = None,
    ) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.code = code
        # Optional extra keys merged into the HTTP error body (e.g. the latest
        # missing-block list returned by an unsuccessful recovery commit).
        self.extra = extra or {}


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def validate_name(name: str, what: str = "name") -> str:
    if not _NAME_RE.match(name or ""):
        raise ServiceError(
            422,
            f"{what} must match {_NAME_RE.pattern} (1-64 chars, letters, digits, '.', '_', '-')",
            "invalid_name",
        )
    return name


def validate_geometry(block_size: int, block_count: int) -> None:
    if not (1 <= block_size <= MAX_BLOCK_SIZE):
        raise ServiceError(
            422, f"block_size must be between 1 and {MAX_BLOCK_SIZE}", "invalid_block_size"
        )
    if not (1 <= block_count <= MAX_BLOCK_COUNT):
        raise ServiceError(
            422, f"block_count must be between 1 and {MAX_BLOCK_COUNT}", "invalid_block_count"
        )
    if block_size * block_count > MAX_BASE_BYTES:
        raise ServiceError(
            422, f"base image exceeds {MAX_BASE_BYTES} bytes", "base_too_large"
        )


def validate_manifest(block_size: int, block_count: int, manifest: Sequence[str]) -> None:
    """Validate a recovery request's geometry and ordered SHA-256 manifest."""
    validate_geometry(block_size, block_count)
    if not isinstance(manifest, Sequence) or len(manifest) != block_count:
        raise ServiceError(
            422,
            f"manifest must list exactly {block_count} SHA-256 digests, got "
            f"{len(manifest) if isinstance(manifest, Sequence) else 0}",
            "manifest_length_mismatch",
        )
    for index, digest in enumerate(manifest):
        if not isinstance(digest, str) or not _SHA256_RE.match(digest):
            raise ServiceError(
                422,
                f"manifest[{index}] is not a 64-character lowercase hex SHA-256 digest",
                "invalid_manifest_digest",
            )


# ---------------------------------------------------------------------------
# payload / refcount helpers (must be called inside a write transaction)
# ---------------------------------------------------------------------------
def _acquire_payload(
    conn: sqlite3.Connection, block_size: int, data: bytes, now: str
) -> int:
    """Return the payload id for ``data``, inserting or recycling a slot.

    Content addressing (``block_size`` + SHA-256) is what makes two volumes
    that inherited the same base block share a single stored copy.  When no
    live row holds the bytes, a free slot for this block size is recycled if
    one exists -- that is how reclaimed storage is handed back to later writes.
    """
    digest = hashlib.sha256(data).hexdigest()
    row = conn.execute(
        "SELECT id FROM payloads "
        "WHERE block_size = ? AND content_hash = ? AND free = 0",
        (block_size, digest),
    ).fetchone()
    if row is not None:
        return int(row["id"])

    free_slot = conn.execute(
        "SELECT id FROM payloads WHERE block_size = ? AND free = 1 "
        "ORDER BY id LIMIT 1",
        (block_size,),
    ).fetchone()
    if free_slot is not None:
        payload_id = int(free_slot["id"])
        conn.execute(
            "UPDATE payloads SET content_hash = ?, data = ?, free = 0, "
            "freed_at = NULL, reuse_count = reuse_count + 1 WHERE id = ?",
            (digest, data, payload_id),
        )
        conn.execute(
            "UPDATE reclaim_events SET reused_at = ? "
            "WHERE id = (SELECT MAX(id) FROM reclaim_events "
            "            WHERE payload_id = ? AND reused_at IS NULL)",
            (now, payload_id),
        )
        return payload_id

    cursor = conn.execute(
        "INSERT INTO payloads(block_size, content_hash, data, refcount, free, "
        "created_at) VALUES (?, ?, ?, 0, 0, ?)",
        (block_size, digest, data, now),
    )
    return int(cursor.lastrowid)


def _add_ref(
    conn: sqlite3.Connection,
    payload_id: int,
    delta: int,
    now: str,
    reason: str,
) -> bool:
    """Adjust a payload's refcount; free the slot when it hits zero.

    Freeing overwrites the bytes with zeros, clears the content address and
    puts the slot on the free list, so its pages are reusable for later writes
    and the retired content is no longer stored.  Returns ``True`` when the
    payload was released.
    """
    conn.execute(
        "UPDATE payloads SET refcount = refcount + ? WHERE id = ? AND free = 0",
        (delta, payload_id),
    )
    row = conn.execute(
        "SELECT id, block_size, refcount, free, length(data) AS data_bytes "
        "FROM payloads WHERE id = ?",
        (payload_id,),
    ).fetchone()
    if row is None:
        return False
    if int(row["free"]) == 1:
        # Releasing a reference to an already-freed payload is a bug.
        raise ServiceError(500, "reference to a freed payload", "internal_invariant")
    if int(row["refcount"]) < 0:
        # Never expected; roll back rather than silently corrupt bookkeeping.
        raise ServiceError(500, "refcount underflow detected", "internal_invariant")
    if int(row["refcount"]) == 0:
        conn.execute(
            "UPDATE payloads SET content_hash = NULL, data = ?, free = 1, "
            "freed_at = ? WHERE id = ?",
            (bytes(int(row["block_size"])), now, payload_id),
        )
        conn.execute(
            "INSERT INTO reclaim_events(payload_id, block_size, data_bytes, reason, "
            "reclaimed_at) VALUES (?, ?, ?, ?, ?)",
            (
                payload_id,
                int(row["block_size"]),
                int(row["data_bytes"]),
                reason,
                now,
            ),
        )
        return True
    return False


def _release_payloads(
    conn: sqlite3.Connection,
    counts: Counter[int],
    now: str,
    reason: str,
) -> tuple[int, int]:
    """Release ``counts`` references per payload.  Returns (rows, bytes) freed."""
    freed_rows = 0
    freed_bytes = 0
    for payload_id, count in counts.items():
        if count <= 0:
            continue
        row = conn.execute(
            "SELECT block_size, length(data) AS data_bytes FROM payloads WHERE id = ?",
            (payload_id,),
        ).fetchone()
        if row is None:
            raise ServiceError(500, "dangling payload reference", "internal_invariant")
        if _add_ref(conn, payload_id, -count, now, reason):
            freed_rows += 1
            freed_bytes += int(row["data_bytes"])
    return freed_rows, freed_bytes


class BlockStoreService:
    def __init__(self, db: Database) -> None:
        self.db = db

    # ------------------------------------------------------------------ bases
    def import_base(
        self, name: str, block_size: int, blocks: Sequence[bytes]
    ) -> dict:
        validate_name(name, "base name")
        validate_geometry(block_size, len(blocks))
        for index, block in enumerate(blocks):
            if len(block) != block_size:
                raise ServiceError(
                    422,
                    f"block {index} is {len(block)} bytes, expected {block_size}",
                    "invalid_block_length",
                )

        now = utcnow()
        base_id = str(uuid.uuid4())
        try:
            with self.db.write() as conn:
                conn.execute(
                    "INSERT INTO bases(id, name, block_size, block_count, status, created_at) "
                    "VALUES (?, ?, ?, ?, 'active', ?)",
                    (base_id, name, block_size, len(blocks), now),
                )
                for index, block in enumerate(blocks):
                    payload_id = _acquire_payload(conn, block_size, block, now)
                    conn.execute(
                        "INSERT INTO base_blocks(base_id, block_index, payload_id) "
                        "VALUES (?, ?, ?)",
                        (base_id, index, payload_id),
                    )
                    _add_ref(conn, payload_id, 1, now, "base_write")
        except sqlite3.IntegrityError as exc:
            if "bases.name" in str(exc):
                raise ServiceError(409, f"base name '{name}' already exists", "name_conflict")
            raise
        return self.get_base(base_id)

    def list_bases(self, status: str | None = None) -> list[dict]:
        with self.db.read() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM bases WHERE status = ? ORDER BY created_at",
                    (status,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM bases ORDER BY created_at"
                ).fetchall()
            return [self._base_summary(conn, row) for row in rows]

    def get_base(self, base_id: str) -> dict:
        with self.db.read() as conn:
            row = conn.execute("SELECT * FROM bases WHERE id = ?", (base_id,)).fetchone()
            if row is None:
                raise ServiceError(404, f"base '{base_id}' not found", "base_not_found")
            detail = self._base_summary(conn, row)
            detail["distinct_blocks_with_shared_payload"] = int(
                conn.execute(
                    "SELECT COUNT(DISTINCT payload_id) FROM base_blocks WHERE base_id = ?",
                    (base_id,),
                ).fetchone()[0]
            )
            return detail

    def read_base_block(self, base_id: str, block_index: int) -> bytes:
        with self.db.read() as conn:
            base = conn.execute(
                "SELECT block_size, block_count, status FROM bases WHERE id = ?",
                (base_id,),
            ).fetchone()
            if base is None:
                raise ServiceError(404, f"base '{base_id}' not found", "base_not_found")
            self._check_index(block_index, int(base["block_count"]))
            row = conn.execute(
                "SELECT p.data FROM base_blocks b JOIN payloads p ON p.id = b.payload_id "
                "WHERE b.base_id = ? AND b.block_index = ?",
                (base_id, block_index),
            ).fetchone()
            if row is None:
                raise ServiceError(
                    410,
                    "base content has been retired and is no longer readable",
                    "base_retired",
                )
            return bytes(row["data"])

    def prepare_retirement(self, base_id: str) -> dict:
        """Enter the writable retirement phase (``active -> retiring``)."""
        now = utcnow()
        with self.db.write() as conn:
            base = conn.execute(
                "SELECT * FROM bases WHERE id = ?", (base_id,)
            ).fetchone()
            if base is None:
                raise ServiceError(404, f"base '{base_id}' not found", "base_not_found")
            if base["status"] == "retired":
                raise ServiceError(409, "base is already retired", "base_retired")
            if base["status"] == "active":
                conn.execute(
                    "UPDATE bases SET status = 'retiring', retirement_prepared_at = ? "
                    "WHERE id = ?",
                    (now, base_id),
                )
            return self._retirement_status(conn, base_id)

    def cancel_retirement(self, base_id: str) -> dict:
        with self.db.write() as conn:
            base = conn.execute(
                "SELECT * FROM bases WHERE id = ?", (base_id,)
            ).fetchone()
            if base is None:
                raise ServiceError(404, f"base '{base_id}' not found", "base_not_found")
            if base["status"] == "retired":
                raise ServiceError(
                    409, "base is already retired; cancellation is impossible", "base_retired"
                )
            conn.execute(
                "UPDATE bases SET status = 'active', retirement_prepared_at = NULL "
                "WHERE id = ?",
                (base_id,),
            )
            return self._retirement_status(conn, base_id)

    def commit_retirement(self, base_id: str) -> dict:
        """Remove all base content and mark the base retired, atomically.

        Every volume already owns independent payload references, so writes
        acknowledged before or during the retirement phase are untouched.
        The operation validates that every inheriting volume is complete before
        dropping anything, and the whole switch is one transaction.
        """
        now = utcnow()
        with self.db.write() as conn:
            base = conn.execute(
                "SELECT * FROM bases WHERE id = ?", (base_id,)
            ).fetchone()
            if base is None:
                raise ServiceError(404, f"base '{base_id}' not found", "base_not_found")
            if base["status"] == "retired":
                raise ServiceError(409, "base is already retired", "base_retired")
            if base["status"] != "retiring":
                raise ServiceError(
                    409,
                    "base must be prepared for retirement before it can be committed",
                    "retirement_not_prepared",
                )

            # Safety: never drop the base if a volume is missing blocks.
            volumes = conn.execute(
                "SELECT id, name, block_count FROM volumes WHERE base_id = ?",
                (base_id,),
            ).fetchall()
            for volume in volumes:
                actual = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM volume_blocks WHERE volume_id = ?",
                        (volume["id"],),
                    ).fetchone()[0]
                )
                if actual != int(volume["block_count"]):
                    raise ServiceError(
                        500,
                        f"volume '{volume['name']}' has {actual} of "
                        f"{volume['block_count']} blocks; refusing to retire base",
                        "internal_invariant",
                    )

            blocks = conn.execute(
                "SELECT payload_id FROM base_blocks WHERE base_id = ?", (base_id,)
            ).fetchall()
            counts = Counter(int(row["payload_id"]) for row in blocks)
            conn.execute("DELETE FROM base_blocks WHERE base_id = ?", (base_id,))
            freed_rows, freed_bytes = _release_payloads(
                conn, counts, now, "base_retirement"
            )
            conn.execute(
                "UPDATE bases SET status = 'retired', retired_at = ? WHERE id = ?",
                (now, base_id),
            )
            return {
                "base_id": base_id,
                "status": "retired",
                "retired_at": now,
                "volumes_preserved": [v["name"] for v in volumes],
                "volume_blocks_preserved": sum(int(v["block_count"]) for v in volumes),
                "base_blocks_removed": len(blocks),
                "payloads_reclaimed": freed_rows,
                "bytes_reclaimed": freed_bytes,
            }

    def delete_base(self, base_id: str) -> dict:
        """Delete a retired base's catalogue row.

        Volumes keep ``base_name`` for provenance; their ``base_id`` becomes
        NULL via ``ON DELETE SET NULL``.
        """
        with self.db.write() as conn:
            base = conn.execute(
                "SELECT status, name FROM bases WHERE id = ?", (base_id,)
            ).fetchone()
            if base is None:
                raise ServiceError(404, f"base '{base_id}' not found", "base_not_found")
            if base["status"] != "retired":
                raise ServiceError(
                    409, "only a retired base can be deleted", "base_not_retired"
                )
            # Defensive: a retired base must have no content left, otherwise the
            # ON DELETE CASCADE would drop references without adjusting counts.
            remaining = int(
                conn.execute(
                    "SELECT COUNT(*) FROM base_blocks WHERE base_id = ?", (base_id,)
                ).fetchone()[0]
            )
            if remaining:
                raise ServiceError(
                    500,
                    f"retired base still stores {remaining} blocks; refusing to delete",
                    "internal_invariant",
                )
            conn.execute("DELETE FROM bases WHERE id = ?", (base_id,))
            return {"base_id": base_id, "name": base["name"], "deleted": True}

    def retirement_status(self, base_id: str) -> dict:
        with self.db.read() as conn:
            base = conn.execute("SELECT * FROM bases WHERE id = ?", (base_id,)).fetchone()
            if base is None:
                raise ServiceError(404, f"base '{base_id}' not found", "base_not_found")
            return self._retirement_status(conn, base_id)

    def _retirement_status(self, conn: sqlite3.Connection, base_id: str) -> dict:
        base = conn.execute("SELECT * FROM bases WHERE id = ?", (base_id,)).fetchone()
        volumes = conn.execute(
            "SELECT id, name, block_count, created_at FROM volumes WHERE base_id = ? "
            "ORDER BY created_at",
            (base_id,),
        ).fetchall()
        written = int(
            conn.execute(
                "SELECT COUNT(*) FROM volume_blocks vb "
                "JOIN volumes v ON v.id = vb.volume_id "
                "WHERE v.base_id = ? AND vb.origin = 'written'",
                (base_id,),
            ).fetchone()[0]
        )
        return {
            "base_id": base_id,
            "name": base["name"],
            "status": base["status"],
            "retirement_prepared_at": base["retirement_prepared_at"],
            "retired_at": base["retired_at"],
            "writable_volumes": [v["name"] for v in volumes],
            "volume_blocks": sum(int(v["block_count"]) for v in volumes),
            "volume_blocks_written": written,
            "note": (
                "volumes remain readable and writable during retirement; "
                "their blocks no longer depend on the base"
            ),
        }

    def _base_summary(self, conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
        base_id = row["id"]
        inheriting = int(
            conn.execute(
                "SELECT COUNT(*) FROM volumes WHERE base_id = ?", (base_id,)
            ).fetchone()[0]
        )
        stored = int(
            conn.execute(
                "SELECT COUNT(*) FROM base_blocks WHERE base_id = ?", (base_id,)
            ).fetchone()[0]
        )
        distinct_payloads = int(
            conn.execute(
                "SELECT COUNT(DISTINCT payload_id) FROM base_blocks WHERE base_id = ?",
                (base_id,),
            ).fetchone()[0]
        )
        return {
            "id": base_id,
            "name": row["name"],
            "block_size": int(row["block_size"]),
            "block_count": int(row["block_count"]),
            "status": row["status"],
            "created_at": row["created_at"],
            "retirement_prepared_at": row["retirement_prepared_at"],
            "retired_at": row["retired_at"],
            "blocks_still_stored": stored,
            "distinct_payloads": distinct_payloads,
            "inheriting_volumes": inheriting,
        }

    # ---------------------------------------------------------------- volumes
    def create_volume(self, name: str, base_id: str) -> dict:
        validate_name(name, "volume name")
        now = utcnow()
        volume_id = str(uuid.uuid4())
        try:
            with self.db.write() as conn:
                base = conn.execute(
                    "SELECT * FROM bases WHERE id = ?", (base_id,)
                ).fetchone()
                if base is None:
                    raise ServiceError(
                        404, f"base '{base_id}' not found", "base_not_found"
                    )
                if base["status"] != "active":
                    raise ServiceError(
                        409,
                        f"base is '{base['status']}'; new volumes require an active base",
                        "base_not_active",
                    )
                if conn.execute(
                    "SELECT 1 FROM recoveries WHERE volume_name = ? AND status = 'recovering'",
                    (name,),
                ).fetchone():
                    raise ServiceError(
                        409,
                        f"volume name '{name}' is reserved by an in-progress recovery",
                        "name_conflict",
                    )
                blocks = conn.execute(
                    "SELECT block_index, payload_id FROM base_blocks "
                    "WHERE base_id = ? ORDER BY block_index",
                    (base_id,),
                ).fetchall()
                if len(blocks) != int(base["block_count"]):
                    raise ServiceError(
                        500,
                        "base is incomplete; refusing to create a volume",
                        "internal_invariant",
                    )

                conn.execute(
                    "INSERT INTO volumes(id, name, base_id, base_name, block_size, "
                    "block_count, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        volume_id,
                        name,
                        base_id,
                        base["name"],
                        int(base["block_size"]),
                        int(base["block_count"]),
                        now,
                    ),
                )
                # Copy references, not bytes: every block stays deduplicated.
                payload_counts: Counter[int] = Counter()
                rows = []
                for block in blocks:
                    payload_id = int(block["payload_id"])
                    rows.append((volume_id, int(block["block_index"]), payload_id, "base"))
                    payload_counts[payload_id] += 1
                conn.executemany(
                    "INSERT INTO volume_blocks(volume_id, block_index, payload_id, origin) "
                    "VALUES (?, ?, ?, ?)",
                    rows,
                )
                for payload_id, count in payload_counts.items():
                    _add_ref(conn, payload_id, count, now, "volume_inherit")
        except sqlite3.IntegrityError as exc:
            if "volumes.name" in str(exc):
                raise ServiceError(
                    409, f"volume name '{name}' already exists", "name_conflict"
                )
            raise
        return self.get_volume(volume_id)

    def list_volumes(self) -> list[dict]:
        with self.db.read() as conn:
            rows = conn.execute("SELECT * FROM volumes ORDER BY created_at").fetchall()
            return [self._volume_summary(conn, row) for row in rows]

    def get_volume(self, volume_id: str) -> dict:
        with self.db.read() as conn:
            row = conn.execute(
                "SELECT * FROM volumes WHERE id = ?", (volume_id,)
            ).fetchone()
            if row is None:
                raise ServiceError(
                    404, f"volume '{volume_id}' not found", "volume_not_found"
                )
            detail = self._volume_summary(conn, row)
            orig = conn.execute(
                "SELECT origin, COUNT(*) AS n FROM volume_blocks WHERE volume_id = ? "
                "GROUP BY origin",
                (volume_id,),
            ).fetchall()
            detail["blocks_by_origin"] = {r["origin"]: int(r["n"]) for r in orig}
            return detail

    def read_volume_block(self, volume_id: str, block_index: int) -> dict:
        with self.db.read() as conn:
            volume = conn.execute(
                "SELECT block_size, block_count, base_id, base_name, name FROM volumes "
                "WHERE id = ?",
                (volume_id,),
            ).fetchone()
            if volume is None:
                raise ServiceError(
                    404, f"volume '{volume_id}' not found", "volume_not_found"
                )
            self._check_index(block_index, int(volume["block_count"]))
            row = conn.execute(
                "SELECT p.data, vb.origin FROM volume_blocks vb "
                "JOIN payloads p ON p.id = vb.payload_id "
                "WHERE vb.volume_id = ? AND vb.block_index = ?",
                (volume_id, block_index),
            ).fetchone()
            if row is None:
                raise ServiceError(
                    500,
                    "volume block is missing; storage is inconsistent",
                    "internal_invariant",
                )
            return {
                "volume_id": volume_id,
                "block_index": block_index,
                "block_size": int(volume["block_size"]),
                "origin": row["origin"],
                "data": bytes(row["data"]),
            }

    def list_volume_blocks(
        self, volume_id: str, *, include_data: bool = True
    ) -> dict:
        """Return every block of a volume in index order (bounded payload)."""
        with self.db.read() as conn:
            volume = conn.execute(
                "SELECT block_size, block_count, name, base_name FROM volumes WHERE id = ?",
                (volume_id,),
            ).fetchone()
            if volume is None:
                raise ServiceError(
                    404, f"volume '{volume_id}' not found", "volume_not_found"
                )
            rows = conn.execute(
                "SELECT vb.block_index, vb.origin, p.content_hash, p.data "
                "FROM volume_blocks vb JOIN payloads p ON p.id = vb.payload_id "
                "WHERE vb.volume_id = ? ORDER BY vb.block_index",
                (volume_id,),
            ).fetchall()
        total_bytes = len(rows) * int(volume["block_size"])
        if include_data and total_bytes > 8 * 1024 * 1024:
            raise ServiceError(
                413,
                f"volume holds {total_bytes} bytes; pass include_data=false or read "
                "blocks individually",
                "response_too_large",
            )
        blocks = []
        for row in rows:
            entry = {
                "block_index": int(row["block_index"]),
                "origin": row["origin"],
                "sha256": row["content_hash"],
            }
            if include_data:
                entry["data_base64"] = base64.b64encode(bytes(row["data"])).decode("ascii")
            blocks.append(entry)
        return {
            "volume_id": volume_id,
            "name": volume["name"],
            "base_name": volume["base_name"] or None,
            "block_size": int(volume["block_size"]),
            "block_count": int(volume["block_count"]),
            "blocks": blocks,
        }

    def write_volume_block(self, volume_id: str, block_index: int, data: bytes) -> dict:
        now = utcnow()
        with self.db.write() as conn:
            volume = conn.execute(
                "SELECT block_size, block_count FROM volumes WHERE id = ?",
                (volume_id,),
            ).fetchone()
            if volume is None:
                raise ServiceError(
                    404, f"volume '{volume_id}' not found", "volume_not_found"
                )
            self._check_index(block_index, int(volume["block_count"]))
            if len(data) != int(volume["block_size"]):
                raise ServiceError(
                    422,
                    f"block must be exactly {volume['block_size']} bytes, got {len(data)}",
                    "invalid_block_length",
                )
            old = conn.execute(
                "SELECT payload_id FROM volume_blocks "
                "WHERE volume_id = ? AND block_index = ?",
                (volume_id, block_index),
            ).fetchone()
            if old is None:
                raise ServiceError(
                    500, "volume block is missing; storage is inconsistent",
                    "internal_invariant",
                )
            old_payload = int(old["payload_id"])
            new_payload = _acquire_payload(conn, int(volume["block_size"]), data, now)
            _add_ref(conn, new_payload, 1, now, "volume_write")
            conn.execute(
                "UPDATE volume_blocks SET payload_id = ?, origin = 'written' "
                "WHERE volume_id = ? AND block_index = ?",
                (new_payload, volume_id, block_index),
            )
            _add_ref(conn, old_payload, -1, now, "volume_overwrite")
            return {
                "volume_id": volume_id,
                "block_index": block_index,
                "block_size": int(volume["block_size"]),
                "origin": "written",
                "acknowledged": True,
            }

    def zero_volume_block(self, volume_id: str, block_index: int) -> dict:
        now = utcnow()
        with self.db.write() as conn:
            volume = conn.execute(
                "SELECT block_size, block_count FROM volumes WHERE id = ?",
                (volume_id,),
            ).fetchone()
            if volume is None:
                raise ServiceError(
                    404, f"volume '{volume_id}' not found", "volume_not_found"
                )
            self._check_index(block_index, int(volume["block_count"]))
            block_size = int(volume["block_size"])
            old = conn.execute(
                "SELECT payload_id FROM volume_blocks "
                "WHERE volume_id = ? AND block_index = ?",
                (volume_id, block_index),
            ).fetchone()
            if old is None:
                raise ServiceError(
                    500, "volume block is missing; storage is inconsistent",
                    "internal_invariant",
                )
            old_payload = int(old["payload_id"])
            zero_payload = _acquire_payload(conn, block_size, bytes(block_size), now)
            _add_ref(conn, zero_payload, 1, now, "volume_zero")
            conn.execute(
                "UPDATE volume_blocks SET payload_id = ?, origin = 'zeroed' "
                "WHERE volume_id = ? AND block_index = ?",
                (zero_payload, volume_id, block_index),
            )
            _add_ref(conn, old_payload, -1, now, "volume_zero_replace")
            return {
                "volume_id": volume_id,
                "block_index": block_index,
                "block_size": block_size,
                "origin": "zeroed",
                "acknowledged": True,
            }

    def delete_volume(self, volume_id: str) -> dict:
        now = utcnow()
        with self.db.write() as conn:
            volume = conn.execute(
                "SELECT name FROM volumes WHERE id = ?", (volume_id,)
            ).fetchone()
            if volume is None:
                raise ServiceError(
                    404, f"volume '{volume_id}' not found", "volume_not_found"
                )
            rows = conn.execute(
                "SELECT payload_id, COUNT(*) AS n FROM volume_blocks "
                "WHERE volume_id = ? GROUP BY payload_id",
                (volume_id,),
            ).fetchall()
            counts = Counter({int(r["payload_id"]): int(r["n"]) for r in rows})
            conn.execute("DELETE FROM volumes WHERE id = ?", (volume_id,))
            freed_rows, freed_bytes = _release_payloads(
                conn, counts, now, "volume_delete"
            )
            return {
                "volume_id": volume_id,
                "name": volume["name"],
                "deleted": True,
                "payloads_reclaimed": freed_rows,
                "bytes_reclaimed": freed_bytes,
            }

    # -------------------------------------------------------------- recovery
    def create_recovery(
        self, name: str, volume_name: str, block_size: int, manifest: Sequence[str]
    ) -> dict:
        """Open a remote-backup recovery session for a not-yet-existing volume.

        The manifest is the ordered SHA-256 digest per block index.  A block is
        considered *present* without any transfer when the content-addressed
        store already holds its bytes (same size and digest).  Only blocks the
        client explicitly uploads pin a payload reference; store-matched blocks
        stay dynamically resolved so ordinary reclamation (volume deletion,
        base retirement) can reclaim them and turn them missing again.
        """
        validate_name(name, "recovery name")
        validate_name(volume_name, "volume name")
        validate_manifest(block_size, len(manifest), list(manifest))

        now = utcnow()
        recovery_id = str(uuid.uuid4())
        try:
            with self.db.write() as conn:
                if conn.execute(
                    "SELECT 1 FROM recoveries WHERE name = ?", (name,)
                ).fetchone():
                    raise ServiceError(
                        409, f"recovery name '{name}' already exists", "name_conflict"
                    )
                if conn.execute(
                    "SELECT 1 FROM volumes WHERE name = ?", (volume_name,)
                ).fetchone() or conn.execute(
                    "SELECT 1 FROM recoveries WHERE volume_name = ?", (volume_name,)
                ).fetchone():
                    raise ServiceError(
                        409,
                        f"volume name '{volume_name}' is already in use or reserved",
                        "name_conflict",
                    )
                conn.execute(
                    "INSERT INTO recoveries(id, name, volume_name, block_size, "
                    "block_count, status, created_at) VALUES (?, ?, ?, ?, ?, "
                    "'recovering', ?)",
                    (
                        recovery_id,
                        name,
                        volume_name,
                        block_size,
                        len(manifest),
                        now,
                    ),
                )
                # Every manifest entry starts unlinked; presence of store-held
                # blocks is derived dynamically, never pinned at creation time.
                conn.executemany(
                    "INSERT INTO recovery_blocks(recovery_id, block_index, "
                    "expected_hash, payload_id) VALUES (?, ?, ?, NULL)",
                    [
                        (recovery_id, index, digest)
                        for index, digest in enumerate(manifest)
                    ],
                )
        except sqlite3.IntegrityError as exc:
            if "recoveries.name" in str(exc) or "recoveries.volume_name" in str(exc):
                raise ServiceError(
                    409, "recovery or volume name already exists", "name_conflict"
                )
            raise
        return self.get_recovery(recovery_id)

    def upload_recovery_block(
        self,
        recovery_id: str,
        block_index: int,
        expected_hash: str,
        data: bytes,
    ) -> dict:
        """Supply one missing block of a recovery.

        The manifest entry, the block size and the digest claimed by the client
        are all checked before anything is stored.  When the bytes already live
        in the store the block resolves through content addressing without a
        new reference; otherwise the uploaded payload is acquired and pinned by
        the recovery so it survives reclamation and restart.  Re-uploading an
        already linked block is a harmless no-op.
        """
        if not _SHA256_RE.match(expected_hash or ""):
            raise ServiceError(
                422,
                "sha256 must be a 64-character lowercase hex SHA-256 digest",
                "invalid_manifest_digest",
            )
        now = utcnow()
        with self.db.write() as conn:
            recovery = self._get_recovery_row(conn, recovery_id)
            if recovery["status"] != "recovering":
                raise ServiceError(
                    409,
                    "recovery is already published; no more blocks are accepted",
                    "recovery_published",
                )
            block_size = int(recovery["block_size"])
            row = conn.execute(
                "SELECT expected_hash, payload_id FROM recovery_blocks "
                "WHERE recovery_id = ? AND block_index = ?",
                (recovery_id, block_index),
            ).fetchone()
            if row is None:
                raise ServiceError(
                    404,
                    f"block index {block_index} is not part of this recovery's "
                    f"manifest (0..{int(recovery['block_count']) - 1})",
                    "block_out_of_range",
                )
            if row["expected_hash"] != expected_hash:
                raise ServiceError(
                    422,
                    "supplied sha256 does not match this block's manifest entry",
                    "manifest_digest_mismatch",
                )
            if len(data) != block_size:
                raise ServiceError(
                    422,
                    f"block must be exactly {block_size} bytes, got {len(data)}",
                    "invalid_block_length",
                )
            actual_hash = hashlib.sha256(data).hexdigest()
            if actual_hash != expected_hash:
                raise ServiceError(
                    422,
                    "block bytes hash to a different digest than the manifest entry",
                    "block_hash_mismatch",
                )

            already_uploaded = row["payload_id"] is not None
            # Was this block already resolvable before this request stored it?
            was_present = already_uploaded or self._store_has_payload(
                conn, block_size, expected_hash
            )
            if not already_uploaded:
                # Acquire (or recycle) a content-addressed payload and pin it for
                # the recovery.  When the same bytes are already live in the
                # store, _acquire_payload returns that row and the extra ref is
                # what keeps it from being reclaimed before publish.
                payload_id = _acquire_payload(conn, block_size, data, now)
                _add_ref(conn, payload_id, 1, now, "recovery_upload")
                conn.execute(
                    "UPDATE recovery_blocks SET payload_id = ? "
                    "WHERE recovery_id = ? AND block_index = ?",
                    (payload_id, recovery_id, block_index),
                )
            return {
                "recovery_id": recovery_id,
                "block_index": block_index,
                "block_size": block_size,
                "sha256": expected_hash,
                "already_present": was_present,
                "acknowledged": True,
                **self._recovery_progress(conn, recovery_id),
            }

    def commit_recovery(self, recovery_id: str) -> dict:
        """Publish the writable volume once every manifest block is present.

        Presence is re-evaluated inside one ``BEGIN IMMEDIATE`` transaction, so
        a payload cannot be reclaimed between the check and the publish.  If
        blocks are still missing (for example because a concurrent delete or
        retirement reclaimed a store-matched block that was never uploaded), no
        volume is created, already received data is retained, and the latest
        missing-block list is returned with a 409.  Repeating a successful
        commit returns the original volume id instead of creating a second one.
        """
        now = utcnow()
        with self.db.write() as conn:
            recovery = self._get_recovery_row(conn, recovery_id)
            if recovery["status"] == "published":
                volume_id = recovery["volume_id"]
                if volume_id is None:
                    raise ServiceError(
                        410,
                        "recovery was published but its volume has since been deleted",
                        "recovery_volume_deleted",
                    )
                return {
                    "recovery_id": recovery_id,
                    "status": "published",
                    "already_published": True,
                    "volume_id": volume_id,
                    "volume_name": recovery["volume_name"],
                }

            missing = self._missing_digests(conn, recovery_id)
            if missing:
                raise ServiceError(
                    409,
                    f"recovery is still missing {len(missing)} block(s); upload them "
                    "and commit again",
                    "recovery_incomplete",
                    extra={
                        "recovery_id": recovery_id,
                        "status": "recovering",
                        "missing_count": len(missing),
                        "missing_digests": missing,
                    },
                )

            block_size = int(recovery["block_size"])
            block_count = int(recovery["block_count"])
            # Resolve every manifest entry to a live payload inside the write
            # lock.  Uploaded entries keep their row; store-matched entries are
            # looked up by content address.
            entries = conn.execute(
                "SELECT block_index, expected_hash, payload_id FROM recovery_blocks "
                "WHERE recovery_id = ? ORDER BY block_index",
                (recovery_id,),
            ).fetchall()
            resolved: list[tuple[int, int]] = []
            for entry in entries:
                payload_id = entry["payload_id"]
                if payload_id is None:
                    match = conn.execute(
                        "SELECT id FROM payloads WHERE block_size = ? AND "
                        "content_hash = ? AND free = 0",
                        (block_size, entry["expected_hash"]),
                    ).fetchone()
                    if match is None:  # vanished since the missing check
                        raise ServiceError(
                            409,
                            "a required block was reclaimed before publish; upload "
                            "it and commit again",
                            "recovery_incomplete",
                            extra={
                                "recovery_id": recovery_id,
                                "status": "recovering",
                                "missing_count": 1,
                                "missing_digests": [entry["expected_hash"]],
                            },
                        )
                    payload_id = int(match["id"])
                resolved.append((int(entry["block_index"]), int(payload_id)))

            volume_id = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO volumes(id, name, base_id, base_name, block_size, "
                "block_count, created_at) VALUES (?, ?, NULL, ?, ?, ?, ?)",
                (
                    volume_id,
                    recovery["volume_name"],
                    RESTORED_BASE_NAME,
                    block_size,
                    block_count,
                    now,
                ),
            )
            conn.executemany(
                "INSERT INTO volume_blocks(volume_id, block_index, payload_id, origin) "
                "VALUES (?, ?, ?, ?)",
                [
                    (volume_id, block_index, payload_id, RESTORED_ORIGIN)
                    for block_index, payload_id in resolved
                ],
            )
            # Every resolved payload gains one reference from the new volume.
            volume_counts: Counter[int] = Counter(
                payload_id for _, payload_id in resolved
            )
            for payload_id, count in volume_counts.items():
                _add_ref(conn, payload_id, count, now, "recovery_publish")
            # Uploaded entries also carry a recovery reference; drop those now
            # that the volume owns its own.  Store-matched entries had none.
            upload_counts = Counter(
                int(row["payload_id"])
                for row in entries
                if row["payload_id"] is not None
            )
            conn.execute(
                "DELETE FROM recovery_blocks WHERE recovery_id = ?", (recovery_id,)
            )
            for payload_id, count in upload_counts.items():
                _add_ref(conn, payload_id, -count, now, "recovery_publish")
            conn.execute(
                "UPDATE recoveries SET status = 'published', published_at = ?, "
                "volume_id = ? WHERE id = ?",
                (now, volume_id, recovery_id),
            )
        detail = self.get_volume(volume_id)
        detail.update(
            {
                "recovery_id": recovery_id,
                "status": "published",
                "already_published": False,
            }
        )
        return detail

    def delete_recovery(self, recovery_id: str) -> dict:
        """Abandon a recovery session and release the blocks it was holding."""
        now = utcnow()
        with self.db.write() as conn:
            recovery = self._get_recovery_row(conn, recovery_id)
            if recovery["status"] == "published":
                raise ServiceError(
                    409,
                    "a published recovery cannot be deleted; delete the volume instead",
                    "recovery_published",
                )
            rows = conn.execute(
                "SELECT payload_id, COUNT(*) AS n FROM recovery_blocks "
                "WHERE recovery_id = ? AND payload_id IS NOT NULL GROUP BY payload_id",
                (recovery_id,),
            ).fetchall()
            counts = Counter({int(r["payload_id"]): int(r["n"]) for r in rows})
            conn.execute("DELETE FROM recoveries WHERE id = ?", (recovery_id,))
            freed_rows, freed_bytes = _release_payloads(
                conn, counts, now, "recovery_abandon"
            )
            return {
                "recovery_id": recovery_id,
                "name": recovery["name"],
                "deleted": True,
                "payloads_reclaimed": freed_rows,
                "bytes_reclaimed": freed_bytes,
            }

    def list_recoveries(self) -> list[dict]:
        with self.db.read() as conn:
            rows = conn.execute(
                "SELECT * FROM recoveries ORDER BY created_at"
            ).fetchall()
            return [self._recovery_summary(conn, row) for row in rows]

    def get_recovery(self, recovery_id: str) -> dict:
        with self.db.read() as conn:
            recovery = self._get_recovery_row(conn, recovery_id)
            return self._recovery_summary(conn, recovery)

    def _get_recovery_row(
        self, conn: sqlite3.Connection, recovery_id: str
    ) -> sqlite3.Row:
        recovery = conn.execute(
            "SELECT * FROM recoveries WHERE id = ?", (recovery_id,)
        ).fetchone()
        if recovery is None:
            raise ServiceError(
                404, f"recovery '{recovery_id}' not found", "recovery_not_found"
            )
        return recovery

    @staticmethod
    def _store_has_payload(
        conn: sqlite3.Connection, block_size: int, digest: str
    ) -> bool:
        """Whether a live payload with this size and content hash is stored."""
        return conn.execute(
            "SELECT 1 FROM payloads WHERE block_size = ? AND content_hash = ? "
            "AND free = 0",
            (block_size, digest),
        ).fetchone() is not None

    @staticmethod
    def _missing_digests(conn: sqlite3.Connection, recovery_id: str) -> list[str]:
        """Distinct digests of blocks that are neither uploaded nor store-held.

        Each digest is listed once, ordered by the first block index where it
        occurs.  A block counts as present when the recovery uploaded it
        (``payload_id`` set) or when a live payload with the matching size and
        hash currently exists in the content-addressed store.
        """
        rows = conn.execute(
            """
            SELECT rb.expected_hash, MIN(rb.block_index) AS first_index
            FROM recovery_blocks rb
            JOIN recoveries r ON r.id = rb.recovery_id
            WHERE rb.recovery_id = ?
              AND rb.payload_id IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM payloads p
                  WHERE p.block_size = r.block_size
                    AND p.content_hash = rb.expected_hash
                    AND p.free = 0
              )
            GROUP BY rb.expected_hash
            ORDER BY first_index
            """,
            (recovery_id,),
        ).fetchall()
        return [row["expected_hash"] for row in rows]

    def _recovery_progress(
        self, conn: sqlite3.Connection, recovery_id: str
    ) -> dict:
        recovery = conn.execute(
            "SELECT block_count, status FROM recoveries WHERE id = ?",
            (recovery_id,),
        ).fetchone()
        total = int(recovery["block_count"])
        if recovery["status"] == "published":
            # Manifest rows are released at publish: the published volume now
            # owns every block, so the session reports nothing missing.
            distinct_row = conn.execute(
                "SELECT COUNT(DISTINCT p.content_hash) AS n "
                "FROM volume_blocks vb JOIN payloads p ON p.id = vb.payload_id "
                "JOIN recoveries rec ON rec.volume_id = vb.volume_id "
                "WHERE rec.id = ?",
                (recovery_id,),
            ).fetchone()
            distinct = int(distinct_row["n"]) if distinct_row is not None else total
            return {
                "block_count": total,
                "blocks_present": total,
                "blocks_missing": 0,
                "blocks_uploaded": 0,
                "distinct_digests": distinct,
                "missing_count": 0,
                "missing_digests": [],
            }
        # A block is present when uploaded (pinned) or currently resolvable
        # against a live payload in the store.
        totals = conn.execute(
            """
            SELECT COUNT(*) AS total,
                   COALESCE(SUM(
                       rb.payload_id IS NOT NULL
                       OR EXISTS (
                           SELECT 1 FROM payloads p
                           WHERE p.block_size = r.block_size
                             AND p.content_hash = rb.expected_hash
                             AND p.free = 0
                       )
                   ), 0) AS present,
                   COUNT(DISTINCT rb.expected_hash) AS distinct_total,
                   COALESCE(SUM(rb.payload_id IS NOT NULL), 0) AS uploaded
            FROM recovery_blocks rb
            JOIN recoveries r ON r.id = rb.recovery_id
            WHERE rb.recovery_id = ?
            """,
            (recovery_id,),
        ).fetchone()
        present = int(totals["present"])
        missing_digests = self._missing_digests(conn, recovery_id)
        return {
            "block_count": total,
            "blocks_present": present,
            "blocks_missing": total - present,
            "blocks_uploaded": int(totals["uploaded"]),
            "distinct_digests": int(totals["distinct_total"]),
            "missing_count": len(missing_digests),
            "missing_digests": missing_digests,
        }

    def _recovery_summary(
        self, conn: sqlite3.Connection, row: sqlite3.Row
    ) -> dict:
        summary = {
            "id": row["id"],
            "name": row["name"],
            "volume_name": row["volume_name"],
            "block_size": int(row["block_size"]),
            "block_count": int(row["block_count"]),
            "status": row["status"],
            "created_at": row["created_at"],
            "published_at": row["published_at"],
            "volume_id": row["volume_id"],
        }
        summary.update(self._recovery_progress(conn, row["id"]))
        return summary

    def _volume_summary(self, conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
        distinct = int(
            conn.execute(
                "SELECT COUNT(DISTINCT payload_id) FROM volume_blocks WHERE volume_id = ?",
                (row["id"],),
            ).fetchone()[0]
        )
        return {
            "id": row["id"],
            "name": row["name"],
            "source": "recovery" if row["base_name"] == RESTORED_BASE_NAME else "base",
            "base_id": row["base_id"],
            "base_name": row["base_name"] or None,
            "block_size": int(row["block_size"]),
            "block_count": int(row["block_count"]),
            "created_at": row["created_at"],
            "distinct_payloads": distinct,
        }

    @staticmethod
    def _check_index(block_index: int, block_count: int) -> None:
        if not (0 <= block_index < block_count):
            raise ServiceError(
                404,
                f"block index {block_index} out of range (0..{block_count - 1})",
                "block_out_of_range",
            )

    # ------------------------------------------------------------ operations
    def stats(self) -> dict:
        with self.db.read() as conn:
            bases = {
                r["status"]: int(r["n"])
                for r in conn.execute(
                    "SELECT status, COUNT(*) AS n FROM bases GROUP BY status"
                )
            }
            volumes = int(
                conn.execute("SELECT COUNT(*) FROM volumes").fetchone()[0]
            )
            payload_row = conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(SUM(length(data)), 0) AS bytes, "
                "COALESCE(SUM(refcount), 0) AS refs FROM payloads WHERE free = 0"
            ).fetchone()
            free_row = conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(SUM(length(data)), 0) AS bytes "
                "FROM payloads WHERE free = 1"
            ).fetchone()
            volume_blocks = {
                r["origin"]: int(r["n"])
                for r in conn.execute(
                    "SELECT origin, COUNT(*) AS n FROM volume_blocks GROUP BY origin"
                )
            }
            base_blocks = int(
                conn.execute("SELECT COUNT(*) FROM base_blocks").fetchone()[0]
            )
            reclaim = conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(SUM(data_bytes), 0) AS bytes, "
                "COALESCE(SUM(reused_at IS NOT NULL), 0) AS reused "
                "FROM reclaim_events"
            ).fetchone()
            recovery_rows = conn.execute(
                "SELECT status, COUNT(*) AS n FROM recoveries GROUP BY status"
            ).fetchall()
            recovery_counts = {r["status"]: int(r["n"]) for r in recovery_rows}
            recovery_refs = int(
                conn.execute(
                    "SELECT COUNT(*) FROM recovery_blocks WHERE payload_id IS NOT NULL"
                ).fetchone()[0]
            )
            distinct_recovery_digests = int(
                conn.execute(
                    "SELECT COUNT(DISTINCT expected_hash) FROM recovery_blocks "
                    "WHERE payload_id IS NOT NULL"
                ).fetchone()[0]
            )
            page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
            freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        return {
            "bases": bases,
            "base_blocks_stored": base_blocks,
            "volumes": volumes,
            "volume_blocks_by_origin": volume_blocks,
            "payloads": {
                "distinct": int(payload_row["n"]),
                "stored_bytes": int(payload_row["bytes"]),
                "total_references": int(payload_row["refs"]),
            },
            "free_slots": {
                "count": int(free_row["n"]),
                "bytes": int(free_row["bytes"]),
            },
            "reclaim": {
                "events": int(reclaim["n"]),
                "bytes_released": int(reclaim["bytes"]),
                "slots_reused": int(reclaim["reused"]),
            },
            "recoveries": {
                "total": sum(recovery_counts.values()),
                "recovering": recovery_counts.get("recovering", 0),
                "published": recovery_counts.get("published", 0),
                "block_references_held": recovery_refs,
                "distinct_digests_held": distinct_recovery_digests,
            },
            "database": {
                "page_size": page_size,
                "page_count": page_count,
                "freelist_pages": freelist_count,
                "freelist_bytes": freelist_count * page_size,
                "size_bytes": page_count * page_size,
            },
        }

    def reclaim_events(self, limit: int = 50) -> list[dict]:
        with self.db.read() as conn:
            rows = conn.execute(
                "SELECT * FROM reclaim_events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(row) for row in rows]

    def verify(self) -> dict:
        """Full consistency audit; used by tests and the ``/verify`` endpoint."""
        issues: list[dict] = []
        with self.db.read() as conn:
            # 1. refcounts must equal actual references and live rows must never
            #    have a zero refcount.
            rows = conn.execute(
                """
                SELECT p.id, p.refcount, p.free,
                       (SELECT COUNT(*) FROM base_blocks b WHERE b.payload_id = p.id)
                     + (SELECT COUNT(*) FROM volume_blocks v WHERE v.payload_id = p.id)
                     + (SELECT COUNT(*) FROM recovery_blocks r WHERE r.payload_id = p.id)
                       AS actual
                FROM payloads p
                """
            ).fetchall()
            for row in rows:
                if int(row["refcount"]) != int(row["actual"]):
                    issues.append(
                        {
                            "check": "refcount",
                            "payload_id": int(row["id"]),
                            "stored": int(row["refcount"]),
                            "actual": int(row["actual"]),
                        }
                    )
                if int(row["free"]) == 0 and int(row["refcount"]) == 0:
                    issues.append(
                        {"check": "live_zero_ref_payload", "payload_id": int(row["id"])}
                    )
                if int(row["free"]) == 1 and int(row["actual"]) != 0:
                    issues.append(
                        {"check": "free_payload_referenced", "payload_id": int(row["id"])}
                    )
            # 2. no dangling references.
            dangling = conn.execute(
                "SELECT COUNT(*) FROM volume_blocks vb "
                "LEFT JOIN payloads p ON p.id = vb.payload_id WHERE p.id IS NULL"
            ).fetchone()[0]
            if int(dangling):
                issues.append({"check": "dangling_volume_blocks", "count": int(dangling)})
            dangling_base = conn.execute(
                "SELECT COUNT(*) FROM base_blocks b "
                "LEFT JOIN payloads p ON p.id = b.payload_id WHERE p.id IS NULL"
            ).fetchone()[0]
            if int(dangling_base):
                issues.append(
                    {"check": "dangling_base_blocks", "count": int(dangling_base)}
                )
            dangling_recovery = conn.execute(
                "SELECT COUNT(*) FROM recovery_blocks rb "
                "LEFT JOIN payloads p ON p.id = rb.payload_id "
                "WHERE rb.payload_id IS NOT NULL AND p.id IS NULL"
            ).fetchone()[0]
            if int(dangling_recovery):
                issues.append(
                    {"check": "dangling_recovery_blocks", "count": int(dangling_recovery)}
                )
            # 3. volumes must be complete.
            incomplete = conn.execute(
                "SELECT v.id, v.name, v.block_count, "
                "(SELECT COUNT(*) FROM volume_blocks vb WHERE vb.volume_id = v.id) AS actual "
                "FROM volumes v"
            ).fetchall()
            for row in incomplete:
                if int(row["actual"]) != int(row["block_count"]):
                    issues.append(
                        {
                            "check": "volume_complete",
                            "volume": row["name"],
                            "expected": int(row["block_count"]),
                            "actual": int(row["actual"]),
                        }
                    )
            # 4. active bases must be complete; retired bases must have no content.
            bases = conn.execute(
                "SELECT b.id, b.name, b.status, b.block_count, "
                "(SELECT COUNT(*) FROM base_blocks bb WHERE bb.base_id = b.id) AS actual "
                "FROM bases b"
            ).fetchall()
            for row in bases:
                if row["status"] in ("active", "retiring"):
                    if int(row["actual"]) != int(row["block_count"]):
                        issues.append(
                            {
                                "check": "base_complete",
                                "base": row["name"],
                                "status": row["status"],
                                "expected": int(row["block_count"]),
                                "actual": int(row["actual"]),
                            }
                        )
                elif int(row["actual"]) != 0:
                    issues.append(
                        {
                            "check": "retired_base_empty",
                            "base": row["name"],
                            "stored_blocks": int(row["actual"]),
                        }
                    )
            # 5. free slots must be zero-filled and unaddressable.
            bad_free = conn.execute(
                "SELECT COUNT(*) FROM payloads "
                "WHERE free = 1 AND (content_hash IS NOT NULL OR data <> zeroblob(length(data)))"
            ).fetchone()[0]
            if int(bad_free):
                issues.append({"check": "free_slots_zeroed", "count": int(bad_free)})
            # 6. recovery sessions: complete manifest, linked payloads live,
            #    published sessions keep no block rows and point at a volume.
            recoveries = conn.execute("SELECT * FROM recoveries").fetchall()
            for recovery in recoveries:
                actual = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM recovery_blocks WHERE recovery_id = ?",
                        (recovery["id"],),
                    ).fetchone()[0]
                )
                if recovery["status"] == "recovering" and actual != int(
                    recovery["block_count"]
                ):
                    issues.append(
                        {
                            "check": "recovery_manifest_complete",
                            "recovery": recovery["name"],
                            "expected": int(recovery["block_count"]),
                            "actual": actual,
                        }
                    )
                if recovery["status"] == "published" and actual != 0:
                    issues.append(
                        {
                            "check": "published_recovery_empty",
                            "recovery": recovery["name"],
                            "stored_blocks": actual,
                        }
                    )
                bad_hash = conn.execute(
                    "SELECT COUNT(*) FROM recovery_blocks rb "
                    "JOIN payloads p ON p.id = rb.payload_id "
                    "WHERE rb.recovery_id = ? AND p.content_hash <> rb.expected_hash",
                    (recovery["id"],),
                ).fetchone()[0]
                if int(bad_hash):
                    issues.append(
                        {
                            "check": "recovery_payload_hash",
                            "recovery": recovery["name"],
                            "count": int(bad_hash),
                        }
                    )
                if recovery["status"] == "published":
                    volume = conn.execute(
                        "SELECT block_count FROM volumes WHERE id = ?",
                        (recovery["volume_id"],),
                    ).fetchone()
                    if recovery["volume_id"] is None or volume is None:
                        # volume_id may legitimately be NULL only when the
                        # published volume was deleted afterwards.
                        if recovery["volume_id"] is not None:
                            issues.append(
                                {
                                    "check": "published_recovery_volume",
                                    "recovery": recovery["name"],
                                }
                            )
                    elif int(volume["block_count"]) != int(recovery["block_count"]):
                        issues.append(
                            {
                                "check": "published_recovery_volume_size",
                                "recovery": recovery["name"],
                            }
                        )
            counts = {
                "payloads": int(
                    conn.execute(
                        "SELECT COUNT(*) FROM payloads WHERE free = 0"
                    ).fetchone()[0]
                ),
                "free_slots": int(
                    conn.execute(
                        "SELECT COUNT(*) FROM payloads WHERE free = 1"
                    ).fetchone()[0]
                ),
                "base_blocks": int(
                    conn.execute("SELECT COUNT(*) FROM base_blocks").fetchone()[0]
                ),
                "volumes": int(
                    conn.execute("SELECT COUNT(*) FROM volumes").fetchone()[0]
                ),
                "volume_blocks": int(
                    conn.execute("SELECT COUNT(*) FROM volume_blocks").fetchone()[0]
                ),
                "bases": int(conn.execute("SELECT COUNT(*) FROM bases").fetchone()[0]),
                "recoveries": int(
                    conn.execute("SELECT COUNT(*) FROM recoveries").fetchone()[0]
                ),
                "recovery_blocks": int(
                    conn.execute(
                        "SELECT COUNT(*) FROM recovery_blocks"
                    ).fetchone()[0]
                ),
            }
        return {"ok": not issues, "counts": counts, "issues": issues}
