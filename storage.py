"""Unified object-storage backend: MinIO (default) or Tigris/S3 (boto3).

Switches to Tigris when AWS_ENDPOINT_URL_S3 is set; otherwise uses the MinIO
SDK. All media reads (list/stat/bytes/range/stream/presign) go through this
module so the reader is storage-agnostic.
"""

from minio import Minio


class MinioBackend:
    def __init__(self, list_host, access, secret, public_host, region, bucket):
        self._client = Minio(list_host, access, secret, secure=False, region=region)
        # public host signs for the browser; region set explicitly so the SDK
        # skips the GetBucketLocation call (which would hit media.sevend.io
        # through the Access gate → 403).
        self._public = Minio(public_host, access, secret, secure=True, region=region)
        self.bucket = bucket

    def list_objects(self, prefix):
        for o in self._client.list_objects(self.bucket, prefix=prefix, recursive=True):
            yield o.object_name, o.size

    def stat(self, key):
        return self._client.stat_object(self.bucket, key).size

    def get_bytes(self, key):
        resp = self._client.get_object(self.bucket, key)
        try:
            return resp.read()
        finally:
            resp.close()
            resp.release_conn()

    def get_range(self, key, offset, length):
        resp = self._client.get_object(self.bucket, key, offset=offset, length=length)
        try:
            return resp.read()
        finally:
            resp.close()
            resp.release_conn()

    def stream(self, key, chunk_size=1024 * 1024):
        resp = self._client.get_object(self.bucket, key)
        try:
            for chunk in resp.stream(amt=chunk_size):
                yield chunk
        finally:
            resp.close()
            resp.release_conn()

    def presign(self, key, ttl):
        return self._public.presigned_get_object(self.bucket, key, expires=ttl)


class TigrisBackend:
    def __init__(self, endpoint, access, secret, bucket):
        import boto3
        from botocore.config import Config

        self.bucket = bucket
        self._client = boto3.client(
            "s3",
            region_name="auto",
            endpoint_url=endpoint,
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"}),
        )

    def list_objects(self, prefix):
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"], obj["Size"]

    def stat(self, key):
        return self._client.head_object(Bucket=self.bucket, Key=key)["ContentLength"]

    def get_bytes(self, key):
        return self._client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def get_range(self, key, offset, length):
        rng = f"bytes={offset}-{offset + length - 1}"
        return self._client.get_object(Bucket=self.bucket, Key=key, Range=rng)["Body"].read()

    def stream(self, key, chunk_size=1024 * 1024):
        body = self._client.get_object(Bucket=self.bucket, Key=key)["Body"]
        while True:
            chunk = body.read(chunk_size)
            if not chunk:
                break
            yield chunk

    def presign(self, key, ttl):
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=int(ttl.total_seconds()),
        )


def make_storage(*, list_host, public_host, access, secret, region, bucket,
                 endpoint="", tigris_access="", tigris_secret="", tigris_bucket=""):
    """Return the active backend: Tigris when endpoint is set, else MinIO."""
    if endpoint:
        return TigrisBackend(endpoint, tigris_access, tigris_secret, tigris_bucket or bucket)
    return MinioBackend(list_host, access, secret, public_host, region, bucket)
