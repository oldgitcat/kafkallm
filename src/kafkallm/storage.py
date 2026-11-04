from __future__ import annotations

import logging
import time
import uuid
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from kafkallm.models import FileRef
from kafkallm.settings import Settings

logger = logging.getLogger(__name__)


class S3Storage:
    def __init__(self, settings: Settings) -> None:
        if not settings.s3_endpoint_url:
            raise ValueError("S3_ENDPOINT_URL is not configured.")
        if not settings.s3_access_key_id or not settings.s3_secret_access_key:
            raise ValueError("S3 credentials are incomplete.")

        self.bucket = settings.s3_bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            region_name=settings.s3_region,
            aws_access_key_id=settings.s3_access_key_id,
            aws_secret_access_key=settings.s3_secret_access_key,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"total_max_attempts": 1, "mode": "standard"},
                connect_timeout=10,
                read_timeout=30,
            ),
        )
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in {"404", "NoSuchBucket", "NotFound"}:
                raise

        try:
            self.client.create_bucket(Bucket=self.bucket)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}:
                raise

    def download(self, reference: FileRef, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(reference.bucket, reference.key, str(destination))

    def upload_input(self, filename: str, data: bytes) -> FileRef:
        """Store an uploaded input file and return its bucket/key reference."""
        name = _safe_filename(filename)
        key = f"uploads/{uuid.uuid4().hex}/{name}"
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return FileRef(bucket=self.bucket, key=key)

    def upload_outputs(
        self,
        outputs: Path,
        job_id: str,
        run_id: str,
        deadline: float,
    ) -> list[FileRef]:
        paths = [path for path in sorted(outputs.rglob("*")) if path.is_file()]
        if len(paths) > 100:
            raise RuntimeError("A job may produce at most 100 result files.")

        artifacts: list[FileRef] = []
        try:
            for path in paths:
                if time.monotonic() > deadline:
                    raise TimeoutError("The job timeout was reached while uploading.")
                relative = path.relative_to(outputs).as_posix()
                key = f"{job_id}/{run_id}/{relative}"
                for attempt in range(3):
                    if time.monotonic() > deadline:
                        raise TimeoutError(
                            "The job timeout was reached while uploading."
                        )
                    try:
                        self.client.upload_file(str(path), self.bucket, key)
                        break
                    except Exception:
                        if attempt == 2:
                            raise
                        time.sleep(min(0.5 * (2**attempt), 2.0))
                artifacts.append(FileRef(bucket=self.bucket, key=key))
            return artifacts
        except Exception:
            if artifacts:
                try:
                    self.client.delete_objects(
                        Bucket=self.bucket,
                        Delete={
                            "Objects": [
                                {"Key": artifact.key} for artifact in artifacts
                            ],
                            "Quiet": True,
                        },
                    )
                except Exception:
                    logger.exception("Failed to clean up uploaded artifacts")
            raise


def _safe_filename(filename: str) -> str:
    name = Path(filename.replace("\\", "/")).name.strip()
    name = "".join(
        "_" if ord(char) < 32 or char in {"/", "\\"} else char for char in name
    )
    name = name[:200]
    return name if name and name not in {".", ".."} else "upload.bin"
