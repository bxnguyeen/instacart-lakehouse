import json
import os
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


BUCKET = "landing"


def minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def state_key(layer):
    if layer not in {"silver", "gold"}:
        raise ValueError(f"Unsupported layer: {layer}")

    return f"instacart/state/{layer}_catalog.json"


def read_catalog_state(layer):
    """Read catalog state, returning None if the object does not exist."""
    try:
        response = minio_client().get_object(
            Bucket=BUCKET,
            Key=state_key(layer),
        )
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise

    body = response["Body"]
    try:
        return json.loads(body.read().decode("utf-8"))
    finally:
        body.close()


def write_catalog_state(layer, catalog_version, status, run_id):
    """Write the current catalog processing state."""
    if status not in {"building", "completed"}:
        raise ValueError(f"Unsupported status: {status}")

    state = {
        "catalog_version": catalog_version,
        "status": status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
    }

    minio_client().put_object(
        Bucket=BUCKET,
        Key=state_key(layer),
        Body=json.dumps(state, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    return state