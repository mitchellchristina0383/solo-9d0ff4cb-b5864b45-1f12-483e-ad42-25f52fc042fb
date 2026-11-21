"""Remote-backup recovery: manifest-driven restore of a writable volume."""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.db import Database
from app.service import BlockStoreService
from conftest import b64, make_base, make_volume, read_block

BLOCK = 8


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def manifest_of(blocks: list[bytes]) -> list[str]:
    return [sha(block) for block in blocks]


def open_recovery(client, name: str, volume_name: str, blocks: list[bytes]) -> dict:
    response = client.post(
        "/recoveries",
        json={
            "name": name,
            "volume_name": volume_name,
            "block_size": len(blocks[0]),
            "manifest": manifest_of(blocks),
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def upload_block(client, recovery_id: str, index: int, data: bytes,
                 digest: str | None = None) -> dict:
    response = client.put(
        f"/recoveries/{recovery_id}/blocks/{index}",
        json={"sha256": digest or sha(data), "data_base64": b64(data)},
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_create_recovery_lists_only_distinct_missing_digests(client):
    blocks = [b"AAAAAAAA", b"BBBBBBBB", b"AAAAAAAA"]  # digest of block 0 == block 2
    # One of the manifest blocks already lives in the store.
    make_base(client, "base-a", [b"AAAAAAAA", b"CCCCCCCC"])

    recovery = open_recovery(client, "rec-1", "restored-1", blocks)
    assert recovery["status"] == "recovering"
    assert recovery["block_count"] == 3
    # AAAAAAAA resolves through the store (both index 0 and 2); BBBBBBBB is missing.
    assert recovery["blocks_present"] == 2
    assert recovery["blocks_uploaded"] == 0  # store-matched blocks are not pinned
    assert recovery["blocks_missing"] == 1
    # The missing digest appears once even though only one distinct block is missing.
    assert recovery["missing_digests"] == [sha(b"BBBBBBBB")]
    assert recovery["missing_count"] == 1


def test_missing_list_collapses_duplicate_missing_digests(client):
    blocks = [b"AAAAAAAA", b"AAAAAAAA", b"BBBBBBBB", b"BBBBBBBB"]
    recovery = open_recovery(client, "rec-1", "restored-1", blocks)
    # Nothing in the store yet: four block slots missing, two distinct digests.
    assert recovery["blocks_missing"] == 4
    assert recovery["missing_count"] == 2
    assert recovery["missing_digests"] == [sha(b"AAAAAAAA"), sha(b"BBBBBBBB")]


def test_upload_verifies_manifest_size_and_digest(client):
    blocks = [b"AAAAAAAA", b"BBBBBBBB"]
    recovery = open_recovery(client, "rec-1", "restored-1", blocks)

    # Wrong block size.
    response = client.put(
        f"/recoveries/{recovery['id']}/blocks/0",
        json={"sha256": sha(b"AAAAAAAA"), "data_base64": b64(b"short")},
    )
    assert response.status_code == 422
    assert response.json()["code"] == "invalid_block_length"

    # Bytes whose digest does not match the claimed sha256 (the claim itself
    # does match the manifest entry, isolating the byte-level check).
    response = client.put(
        f"/recoveries/{recovery['id']}/blocks/0",
        json={"sha256": sha(b"AAAAAAAA"), "data_base64": b64(b"WRONG!!!")},
    )
    assert response.status_code == 422
    assert response.json()["code"] == "block_hash_mismatch"

    # Claimed digest differs from the manifest entry for the index.
    response = client.put(
        f"/recoveries/{recovery['id']}/blocks/0",
        json={"sha256": sha(b"BBBBBBBB"), "data_base64": b64(b"BBBBBBBB")},
    )
    assert response.status_code == 422
    assert response.json()["code"] == "manifest_digest_mismatch"

    # Block index outside the manifest.
    response = client.put(
        f"/recoveries/{recovery['id']}/blocks/9",
        json={"sha256": sha(b"AAAAAAAA"), "data_base64": b64(b"AAAAAAAA")},
    )
    assert response.status_code == 404
    assert response.json()["code"] == "block_out_of_range"

    # Nothing was stored: the recovery still needs both blocks.
    fresh = client.get(f"/recoveries/{recovery['id']}").json()
    assert fresh["blocks_present"] == 0


def test_invalid_manifest_inputs_rejected(client):
    good = sha(b"AAAAAAAA")
    bad_geometry = client.post(
        "/recoveries",
        json={
            "name": "rec-bad",
            "volume_name": "vol-bad",
            "block_size": 0,
            "manifest": [good],
        },
    )
    assert bad_geometry.status_code == 422
    bad_digest = client.post(
        "/recoveries",
        json={
            "name": "rec-bad2",
            "volume_name": "vol-bad2",
            "block_size": BLOCK,
            "manifest": ["not-a-sha256"],
        },
    )
    assert bad_digest.status_code == 422
    assert bad_digest.json()["code"] == "invalid_manifest_digest"
    bad_length = client.post(
        "/recoveries",
        json={
            "name": "rec-bad3",
            "volume_name": "vol-bad3",
            "block_size": BLOCK,
            "block_count": 2,
            "manifest": [good],
        },
    )
    assert bad_length.status_code == 422
    assert bad_length.json()["code"] == "manifest_length_mismatch"


def test_recovery_name_conflicts(client):
    blocks = [b"AAAAAAAA"]
    open_recovery(client, "rec-1", "vol-1", blocks)
    # Same recovery name.
    response = client.post(
        "/recoveries",
        json={
            "name": "rec-1",
            "volume_name": "vol-2",
            "block_size": BLOCK,
            "manifest": [sha(b"AAAAAAAA")],
        },
    )
    assert response.status_code == 409
    assert response.json()["code"] == "name_conflict"
    # Reserved volume name.
    response = client.post(
        "/recoveries",
        json={
            "name": "rec-2",
            "volume_name": "vol-1",
            "block_size": BLOCK,
            "manifest": [sha(b"AAAAAAAA")],
        },
    )
    assert response.status_code == 409
    assert response.json()["code"] == "name_conflict"

    # A normal volume cannot steal the name reserved by the recovery either.
    base = make_base(client, "base-z", [b"AAAAAAAA"])
    response = client.post("/volumes", json={"name": "vol-1", "base_id": base["id"]})
    assert response.status_code == 409
    assert response.json()["code"] == "name_conflict"


def test_commit_publishes_volume_and_repeated_commit_returns_same_id(client):
    blocks = [b"AAAAAAAA", b"BBBBBBBB", b"AAAAAAAA"]
    recovery = open_recovery(client, "rec-1", "restored-1", blocks)

    # Commit before completion: 409 plus the refreshed distinct missing list,
    # received data retained (none yet).
    response = client.post(f"/recoveries/{recovery['id']}/commit")
    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "recovery_incomplete"
    assert body["missing_count"] == 2
    assert set(body["missing_digests"]) == {sha(b"AAAAAAAA"), sha(b"BBBBBBBB")}

    upload_block(client, recovery["id"], 0, b"AAAAAAAA")
    # The duplicate manifest entry (index 2) resolves itself.
    mid = client.get(f"/recoveries/{recovery['id']}").json()
    assert mid["blocks_present"] == 2
    assert mid["missing_digests"] == [sha(b"BBBBBBBB")]

    upload_block(client, recovery["id"], 1, b"BBBBBBBB")
    commit = client.post(f"/recoveries/{recovery['id']}/commit")
    assert commit.status_code == 200, commit.text
    published = commit.json()
    assert published["status"] == "published"
    assert published["already_published"] is False
    assert published["name"] == "restored-1"
    assert published["source"] == "recovery"
    assert published["base_name"] is None
    assert published["blocks_by_origin"] == {"restored": 3}
    volume_id = published["id"]

    # The new volume reads back the backup content.
    assert read_block(client, volume_id, 0) == b"AAAAAAAA"
    assert read_block(client, volume_id, 1) == b"BBBBBBBB"
    assert read_block(client, volume_id, 2) == b"AAAAAAAA"
    # It is a normal writable volume.
    client.put(
        f"/volumes/{volume_id}/blocks/1", json={"data_base64": b64(b"WRITTEN!")}
    )
    assert read_block(client, volume_id, 1) == b"WRITTEN!"

    # Repeating the commit returns the original volume id, never a second volume.
    again = client.post(f"/recoveries/{recovery['id']}/commit")
    assert again.status_code == 200
    assert again.json()["already_published"] is True
    assert again.json()["volume_id"] == volume_id
    volumes = client.get("/volumes").json()
    assert [v["id"] for v in volumes if v["name"] == "restored-1"] == [volume_id]


def test_raw_upload_endpoint(client):
    blocks = [b"AAAAAAAA"]
    recovery = open_recovery(client, "rec-1", "restored-1", blocks)
    response = client.put(
        f"/recoveries/{recovery['id']}/blocks/0/raw?sha256={sha(b'AAAAAAAA')}",
        content=b"AAAAAAAA",
        headers={"content-type": "application/octet-stream"},
    )
    assert response.status_code == 200, response.text
    commit = client.post(f"/recoveries/{recovery['id']}/commit")
    assert commit.status_code == 200
    assert read_block(client, commit.json()["id"], 0) == b"AAAAAAAA"


def test_commit_after_published_volume_deleted_returns_410(client):
    blocks = [b"AAAAAAAA"]
    recovery = open_recovery(client, "rec-1", "restored-1", blocks)
    upload_block(client, recovery["id"], 0, b"AAAAAAAA")
    published = client.post(f"/recoveries/{recovery['id']}/commit").json()
    volume_id = published["id"]

    assert client.delete(f"/volumes/{volume_id}").status_code == 200
    # The session remains as an audit row, but its volume is gone.
    audit = client.get(f"/recoveries/{recovery['id']}").json()
    assert audit["status"] == "published"
    assert audit["volume_id"] is None
    assert audit["block_count"] == 1
    assert audit["missing_count"] == 0
    response = client.post(f"/recoveries/{recovery['id']}/commit")
    assert response.status_code == 410
    assert response.json()["code"] == "recovery_volume_deleted"
    assert client.get("/verify").json()["ok"] is True


def test_redundant_upload_is_idempotent_noop(client):
    blocks = [b"AAAAAAAA"]
    recovery = open_recovery(client, "rec-1", "restored-1", blocks)
    first = upload_block(client, recovery["id"], 0, b"AAAAAAAA")
    assert first["already_present"] is False
    second = upload_block(client, recovery["id"], 0, b"AAAAAAAA")
    assert second["already_present"] is True
    assert second["blocks_present"] == 1


def test_blocks_already_in_store_need_no_transfer(client):
    # A base and a volume already hold the backup's blocks in the store.
    blocks = [b"AAAAAAAA", b"BBBBBBBB"]
    base = make_base(client, "base-a", blocks)
    make_volume(client, "vol-a", base["id"])

    recovery = open_recovery(client, "rec-1", "restored-1", blocks)
    assert recovery["blocks_missing"] == 0
    assert recovery["missing_digests"] == []
    # Commit publishes without a single block upload.
    commit = client.post(f"/recoveries/{recovery['id']}/commit")
    assert commit.status_code == 200
    volume_id = commit.json()["id"]
    assert read_block(client, volume_id, 0) == b"AAAAAAAA"
    assert read_block(client, volume_id, 1) == b"BBBBBBBB"
    # The restored volume shares the stored payloads.
    stats = client.get("/stats").json()
    assert stats["payloads"]["distinct"] == 2


def test_recovery_pins_uploaded_blocks_against_volume_deletion(client):
    blocks = [b"AAAAAAAA", b"BBBBBBBB"]
    base = make_base(client, "base-a", blocks)
    volume = make_volume(client, "vol-a", base["id"])
    recovery = open_recovery(client, "rec-1", "restored-1", blocks)
    assert recovery["blocks_missing"] == 0

    # Explicitly upload one block: only uploaded blocks pin a reference.
    upload_block(client, recovery["id"], 0, b"AAAAAAAA")
    # Retire the base (the volume still holds every reference, so nothing is
    # freed yet), then delete the volume: BBBBBBBB has no holder left, while
    # the uploaded AAAAAAAA stays pinned by the recovery.
    client.post(f"/bases/{base['id']}/retirement/prepare")
    client.post(f"/bases/{base['id']}/retirement/commit")
    client.delete(f"/volumes/{volume['id']}")
    stats = client.get("/stats").json()
    assert stats["free_slots"]["count"] == 1          # BBBBBBBB reclaimed
    assert stats["payloads"]["distinct"] == 1         # only pinned AAAAAAAA
    detail = client.get(f"/recoveries/{recovery['id']}").json()
    assert detail["blocks_uploaded"] == 1
    assert detail["missing_digests"] == [sha(b"BBBBBBBB")]
    assert client.get("/verify").json()["ok"] is True

    upload_block(client, recovery["id"], 1, b"BBBBBBBB")  # reuses the free slot
    commit = client.post(f"/recoveries/{recovery['id']}/commit")
    assert commit.status_code == 200
    assert read_block(client, commit.json()["id"], 1) == b"BBBBBBBB"


def test_reclamation_proceeds_for_blocks_recovery_does_not_need(client):
    # The base holds three blocks; the recovery manifest needs only one of them
    # and never uploads anything, so it pins nothing.
    base = make_base(client, "base-a", [b"AAAAAAAA", b"BBBBBBBB", b"CCCCCCCC"])
    volume = make_volume(client, "vol-a", base["id"])
    recovery = open_recovery(client, "rec-1", "restored-1", [b"AAAAAAAA"])
    assert recovery["blocks_missing"] == 0
    assert recovery["blocks_uploaded"] == 0

    client.post(f"/bases/{base['id']}/retirement/prepare")
    commit = client.post(f"/bases/{base['id']}/retirement/commit").json()
    # Retirement reclaims nothing yet: the volume still references all three.
    assert commit["payloads_reclaimed"] == 0
    # Deleting the volume releases every block: the store-matched recovery
    # holds no reference, so reclamation proceeds normally.
    client.delete(f"/volumes/{volume['id']}")
    stats = client.get("/stats").json()
    assert stats["payloads"]["distinct"] == 0
    assert stats["free_slots"]["count"] == 3

    # The block is genuinely missing now; the commit must not publish a volume.
    response = client.post(f"/recoveries/{recovery['id']}/commit")
    assert response.status_code == 409
    assert response.json()["missing_digests"] == [sha(b"AAAAAAAA")]

    # Uploading reuses one of the reclaimed slots and the restore completes.
    upload_block(client, recovery["id"], 0, b"AAAAAAAA")
    published = client.post(f"/recoveries/{recovery['id']}/commit")
    assert published.status_code == 200
    assert read_block(client, published.json()["id"], 0) == b"AAAAAAAA"
    assert client.get("/verify").json()["ok"] is True


def test_reclaimed_block_reappears_as_missing_until_reuploaded(client):
    blocks = [b"AAAAAAAA", b"BBBBBBBB"]
    base = make_base(client, "base-a", blocks)
    volume = make_volume(client, "vol-a", base["id"])
    recovery = open_recovery(client, "rec-1", "restored-1", blocks)
    # Everything looked available...
    assert recovery["blocks_missing"] == 0

    # ...then the only other holders disappear and both payloads are reclaimed.
    client.post(f"/bases/{base['id']}/retirement/prepare")
    client.post(f"/bases/{base['id']}/retirement/commit")
    client.delete(f"/volumes/{volume['id']}")

    # The commit must fail with the refreshed missing list instead of publishing
    # a broken volume; already received data (nothing here) is retained.
    response = client.post(f"/recoveries/{recovery['id']}/commit")
    assert response.status_code == 409
    body = response.json()
    assert body["missing_count"] == 2
    assert set(body["missing_digests"]) == {sha(b"AAAAAAAA"), sha(b"BBBBBBBB")}

    # Re-upload from the backup and finish; the reclaimed free slots are reused.
    stats_before = client.get("/stats").json()
    assert stats_before["free_slots"]["count"] == 2
    upload_block(client, recovery["id"], 0, b"AAAAAAAA")
    upload_block(client, recovery["id"], 1, b"BBBBBBBB")
    stats_after = client.get("/stats").json()
    assert stats_after["free_slots"]["count"] == 0
    assert stats_after["reclaim"]["slots_reused"] == 2

    commit = client.post(f"/recoveries/{recovery['id']}/commit")
    assert commit.status_code == 200
    volume_id = commit.json()["id"]
    assert read_block(client, volume_id, 0) == b"AAAAAAAA"
    assert read_block(client, volume_id, 1) == b"BBBBBBBB"
    assert client.get("/verify").json()["ok"] is True


def test_partial_upload_survives_reclamation_then_commit(client):
    blocks = [b"AAAAAAAA", b"BBBBBBBB", b"CCCCCCCC"]
    base = make_base(client, "base-a", blocks)
    volume = make_volume(client, "vol-a", base["id"])
    recovery = open_recovery(client, "rec-1", "restored-1", blocks)
    # Upload one block directly (it already existed; this adds a recovery ref).
    upload_block(client, recovery["id"], 0, b"AAAAAAAA")

    # Reclaim the other two blocks via retirement + volume deletion. Block 0 is
    # pinned by the recovery and must survive.
    client.post(f"/bases/{base['id']}/retirement/prepare")
    client.post(f"/bases/{base['id']}/retirement/commit")
    client.delete(f"/volumes/{volume['id']}")

    detail = client.get(f"/recoveries/{recovery['id']}").json()
    assert detail["blocks_present"] == 1
    assert set(detail["missing_digests"]) == {sha(b"BBBBBBBB"), sha(b"CCCCCCCC")}
    stats = client.get("/stats").json()
    assert stats["payloads"]["distinct"] == 1  # only AAAAAAAA remains

    upload_block(client, recovery["id"], 1, b"BBBBBBBB")
    upload_block(client, recovery["id"], 2, b"CCCCCCCC")
    commit = client.post(f"/recoveries/{recovery['id']}/commit")
    assert commit.status_code == 200
    for index, expected in enumerate(blocks):
        assert read_block(client, commit.json()["id"], index) == expected
    assert client.get("/verify").json()["ok"] is True


def test_abandon_recovery_releases_held_blocks(client):
    blocks = [b"AAAAAAAA", b"BBBBBBBB"]
    recovery = open_recovery(client, "rec-1", "restored-1", blocks)
    upload_block(client, recovery["id"], 0, b"AAAAAAAA")
    assert client.get("/stats").json()["payloads"]["distinct"] == 1

    result = client.delete(f"/recoveries/{recovery['id']}")
    assert result.status_code == 200
    assert result.json()["payloads_reclaimed"] == 1
    assert client.get(f"/recoveries/{recovery['id']}").status_code == 404
    assert client.get("/stats").json()["free_slots"]["count"] == 1
    assert client.get("/verify").json()["ok"] is True


def test_cannot_upload_or_commit_after_publish(client):
    blocks = [b"AAAAAAAA"]
    recovery = open_recovery(client, "rec-1", "restored-1", blocks)
    upload_block(client, recovery["id"], 0, b"AAAAAAAA")
    client.post(f"/recoveries/{recovery['id']}/commit")

    response = client.put(
        f"/recoveries/{recovery['id']}/blocks/0",
        json={"sha256": sha(b"AAAAAAAA"), "data_base64": b64(b"AAAAAAAA")},
    )
    assert response.status_code == 409
    assert response.json()["code"] == "recovery_published"
    assert client.delete(f"/recoveries/{recovery['id']}").status_code == 409


def test_recovery_stats_and_audit(client):
    blocks = [b"AAAAAAAA", b"AAAAAAAA"]
    recovery = open_recovery(client, "rec-1", "restored-1", blocks)
    upload_block(client, recovery["id"], 0, b"AAAAAAAA")  # one pinned reference

    stats = client.get("/stats").json()
    assert stats["recoveries"]["total"] == 1
    assert stats["recoveries"]["recovering"] == 1
    assert stats["recoveries"]["block_references_held"] == 1
    assert stats["recoveries"]["distinct_digests_held"] == 1
    verify = client.get("/verify").json()
    assert verify["ok"] is True
    assert verify["counts"]["recoveries"] == 1
    assert verify["counts"]["recovery_blocks"] == 2

    client.post(f"/recoveries/{recovery['id']}/commit")
    after = client.get("/stats").json()
    assert after["recoveries"]["published"] == 1
    assert after["recoveries"]["block_references_held"] == 0
    assert client.get("/verify").json()["ok"] is True


def test_shared_content_across_recoveries_transfers_once(client):
    blocks_a = [b"AAAAAAAA", b"BBBBBBBB"]
    blocks_b = [b"BBBBBBBB", b"CCCCCCCC"]  # BBBBBBBB overlaps
    rec_a = open_recovery(client, "rec-a", "vol-a", blocks_a)
    rec_b = open_recovery(client, "rec-b", "vol-b", blocks_b)

    upload_block(client, rec_a["id"], 1, b"BBBBBBBB")
    # rec_b's block 0 now resolves against the payload rec_a just uploaded.
    detail_b = client.get(f"/recoveries/{rec_b['id']}").json()
    assert detail_b["blocks_present"] == 1
    assert sha(b"BBBBBBBB") not in detail_b["missing_digests"]

    # Finish rec_a and publish it; rec_b still holds its own reference.
    upload_block(client, rec_a["id"], 0, b"AAAAAAAA")
    client.post(f"/recoveries/{rec_a['id']}/commit")
    upload_block(client, rec_b["id"], 1, b"CCCCCCCC")
    commit_b = client.post(f"/recoveries/{rec_b['id']}/commit")
    assert commit_b.status_code == 200
    volume_b = commit_b.json()["id"]
    assert read_block(client, volume_b, 0) == b"BBBBBBBB"
    assert read_block(client, volume_b, 1) == b"CCCCCCCC"
    assert client.get("/verify").json()["ok"] is True


@pytest.mark.parametrize("bad_digest", ["", "A" * 64, "z" * 64, "a" * 63])
def test_digest_format_validation(client, bad_digest):
    blocks = [b"AAAAAAAA"]
    recovery = open_recovery(client, "rec-1", "restored-1", blocks)
    response = client.put(
        f"/recoveries/{recovery['id']}/blocks/0",
        json={"sha256": bad_digest or sha(b"AAAAAAAA"),
              "data_base64": b64(b"AAAAAAAA")},
    )
    if not bad_digest:
        assert response.status_code == 200
    else:
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# service-level concurrency: uploads racing volume deletion and retirement
# ---------------------------------------------------------------------------
def test_concurrent_uploads_and_reclamation_leave_consistent_store(tmp_path):
    db_path = tmp_path / "recovery-race.db"
    db = Database(str(db_path), busy_timeout=30)
    db.initialise()
    service = BlockStoreService(db)

    count = 16
    blocks = [f"BK{i:06d}".encode() for i in range(count)]
    base = service.import_base("base", BLOCK, blocks)
    volume = service.create_volume("vol", base["id"])
    recovery = service.create_recovery(
        "rec", "restored", BLOCK, manifest_of(blocks)
    )
    recovery_id = recovery["id"]

    def upload(index: int) -> None:
        service.upload_recovery_block(recovery_id, index, sha(blocks[index]), blocks[index])

    errors: list[str] = []

    def reclaim() -> None:
        try:
            service.prepare_retirement(base["id"])
            service.commit_retirement(base["id"])
            service.delete_volume(volume["id"])
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(str(exc))

    with ThreadPoolExecutor(max_workers=9) as pool:
        futures = [pool.submit(upload, index) for index in range(count)]
        futures.append(pool.submit(reclaim))
        for future in futures:
            future.result(timeout=30)
    assert not errors, errors

    # Every acknowledged upload is pinned; blocks only store-matched before the
    # reclamation may be missing, in which case commit reports them honestly.
    detail = service.get_recovery(recovery_id)
    report = service.verify()
    assert report["ok"], report

    # Re-upload anything reclaimed, then publish and read every block back.
    missing = set(detail["missing_digests"])
    for index, block in enumerate(blocks):
        if sha(block) in missing:
            service.upload_recovery_block(recovery_id, index, sha(block), block)
    published = service.commit_recovery(recovery_id)
    assert published["status"] == "published"
    for index, block in enumerate(blocks):
        assert service.read_volume_block(published["id"], index)["data"] == block
    assert service.verify()["ok"] is True
