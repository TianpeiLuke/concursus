"""Concrete ObjectStore implementations: S3Store (production) and FileStore (local/test).

Both implement the ObjectStore protocol defined in harness.py:
    async def get_object(self, uri: str) -> bytes
    async def put_object(self, uri: str, data: bytes, content_type: str) -> str
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class FileStore:
    """Local filesystem ObjectStore. Uses file:// URIs.

    Suitable for testing, offline development, and local runs.
    Maps URIs like file:///path/to/artifact to disk paths.
    Also handles bare paths (no scheme) as absolute filesystem paths.

    Args:
        root: Optional root directory. If set, relative paths are resolved under this root.
    """

    def __init__(self, root: Optional[str] = None):
        self.root = Path(root) if root else None

    async def get_object(self, uri: str) -> bytes:
        """Read bytes from a local file."""
        path = self._resolve_path(uri)
        if not path.exists():
            raise FileNotFoundError(f"No object at {uri} (resolved to {path})")
        return await asyncio.get_event_loop().run_in_executor(
            None, path.read_bytes
        )

    async def put_object(self, uri: str, data: bytes, content_type: str) -> str:
        """Write bytes to a local file. Creates parent directories."""
        path = self._resolve_path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.get_event_loop().run_in_executor(
            None, path.write_bytes, data
        )
        return uri

    def _resolve_path(self, uri: str) -> Path:
        """Resolve a URI to a local filesystem path."""
        if uri.startswith("file://"):
            # file:///absolute/path or file://relative/path
            parsed = urlparse(uri)
            path = Path(parsed.path)
        elif uri.startswith("s3://"):
            # For testing: map s3:// URIs to local paths under root
            # s3://bucket/prefix/file → root/bucket/prefix/file
            parsed = urlparse(uri)
            path = Path(parsed.netloc) / parsed.path.lstrip("/")
            if self.root:
                path = self.root / path
        elif uri.startswith("/"):
            path = Path(uri)
        else:
            path = Path(uri)
            if self.root:
                path = self.root / path

        return path


class S3Store:
    """AWS S3 ObjectStore. Uses s3:// URIs.

    Production implementation. Lazy-imports boto3.

    Args:
        client: Optional pre-built boto3 S3 client. If None, created on first use.
        region: AWS region for client creation (default: us-west-2).
    """

    def __init__(self, client=None, region: str = "us-west-2"):
        self._client = client
        self._region = region

    async def get_object(self, uri: str) -> bytes:
        """Fetch bytes from S3."""
        bucket, key = self._parse_s3_uri(uri)
        client = self._get_client()

        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: client.get_object(Bucket=bucket, Key=key),
        )
        return await loop.run_in_executor(None, response["Body"].read)

    async def put_object(self, uri: str, data: bytes, content_type: str) -> str:
        """Write bytes to S3."""
        bucket, key = self._parse_s3_uri(uri)
        client = self._get_client()

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: client.put_object(
                Bucket=bucket, Key=key, Body=data, ContentType=content_type
            ),
        )
        return uri

    def _parse_s3_uri(self, uri: str) -> tuple[str, str]:
        """Parse s3://bucket/key into (bucket, key)."""
        if not uri.startswith("s3://"):
            raise ValueError(f"Not an S3 URI: {uri}")
        parsed = urlparse(uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise ValueError(f"Invalid S3 URI (missing bucket or key): {uri}")
        return bucket, key

    def _get_client(self):
        """Lazy-load boto3 S3 client."""
        if self._client is None:
            try:
                import boto3
            except ImportError:
                raise RuntimeError(
                    "boto3 is required for S3Store. "
                    "Install with: pip install 'concursus[agentcore]'"
                )
            self._client = boto3.client("s3", region_name=self._region)
        return self._client
