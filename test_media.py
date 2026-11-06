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


def test_presign_tigris_uses_boto3(monkeypatch):
    import boto3

    monkeypatch.setattr(app, "TIGRIS_ENDPOINT", "https://fly.storage.tigris.dev")
    monkeypatch.setattr(app, "TIGRIS_BUCKET", "kb")
    with patch.object(boto3, "client") as mock_client:
        mock_client.return_value.generate_presigned_url.return_value = "https://signed"
        url = app.presign("videos/foo.mp4", app.VIDEO_PRESIGN_TTL)
    assert url == "https://signed"
    mock_client.assert_called_once()
