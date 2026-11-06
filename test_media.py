"""Minimal tests for the presigned-media route (Tigris migration)."""

import os
from unittest.mock import patch

# The app reads env at import time — set stubs before importing.
os.environ.setdefault("MINIO_ACCESS_KEY", "test-access")
os.environ.setdefault("MINIO_SECRET_KEY", "test-secret")
os.environ.setdefault("STREAM_MEDIA_HOST", "localhost:9000")

from fastapi.testclient import TestClient

import app


def test_media_route_video_gets_24h_ttl():
    with patch.object(app, "presign", return_value="https://cdn/presigned") as mock:
        client = TestClient(app.app)
        r = client.get("/media/videos/foo.mp4", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "https://cdn/presigned"
    assert mock.call_args[0][1] == app.VIDEO_PRESIGN_TTL


def test_media_route_transcript_gets_1h_ttl():
    with patch.object(app, "presign", return_value="https://cdn/presigned") as mock:
        client = TestClient(app.app)
        r = client.get("/media/transcripts/foo.json", follow_redirects=False)
    assert r.status_code == 302
    assert mock.call_args[0][1] == app.FILE_PRESIGN_TTL


def test_tigris_backend_presign_uses_boto3():
    import storage

    backend = storage.TigrisBackend("https://fly.storage.tigris.dev", "ak", "sk", "kb")
    with patch.object(backend._client, "generate_presigned_url", return_value="https://signed") as mock:
        url = backend.presign("videos/foo.mp4", app.VIDEO_PRESIGN_TTL)
    assert url == "https://signed"
    mock.assert_called_once()


def test_make_storage_selects_tigris_when_endpoint_set():
    import storage

    s = storage.make_storage(
        list_host="lh", public_host="ph", access="a", secret="s",
        region="r", bucket="b", endpoint="https://fly.storage.tigris.dev",
        tigris_access="ak", tigris_secret="sk", tigris_bucket="kb",
    )
    assert isinstance(s, storage.TigrisBackend)
    assert s.bucket == "kb"


def test_make_storage_defaults_to_minio():
    import storage

    s = storage.make_storage(
        list_host="lh", public_host="ph", access="a", secret="s",
        region="r", bucket="b",
    )
    assert isinstance(s, storage.MinioBackend)
    assert s.bucket == "b"
