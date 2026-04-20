import os
import re
import io
import httpx
import boto3
import hmac
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import FastAPI, Request, Response, Header, HTTPException, Depends
from PIL import Image
from supabase import create_client

# Supabase — used ONLY for reading `inventory` and writing `storage_files_railway`.
# All storage operations have moved to Railway Bucket (S3-compatible) via boto3.
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

# Railway Bucket (S3-compatible) config
R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
R2_BUCKET = os.environ["R2_BUCKET"]

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",  # placeholder; endpoint_url overrides
)

app = FastAPI()


def verify_webhook_secret(x_webhook_secret: str = Header(None)):
    """
    Require WEBHOOK_SECRET header on protected endpoints.
    If env var is empty (e.g. local dev), skip the check — so deploy without the var doesn't break anything,
    but in production we'll always set it.
    Uses hmac.compare_digest for timing-safe comparison.
    """
    if not WEBHOOK_SECRET:
        return  # not configured, allow through (dev mode)
    if not x_webhook_secret or not hmac.compare_digest(x_webhook_secret, WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="Invalid or missing webhook secret")


def sku_to_folder(sku: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "", sku)


def convert_image(raw_bytes: bytes, source_url: str, content_type: str) -> tuple[bytes, str, str]:
    """Convert any image to 8-bit JPEG. Returns (bytes, extension, mime_type)."""
    img = Image.open(io.BytesIO(raw_bytes))

    # Handle high bit-depth modes (10-bit/16-bit) → 8-bit
    if img.mode in ("I;16", "I;16L", "I;16B", "I;16N"):
        import numpy as np
        arr = np.array(img, dtype=np.uint16)
        arr = (arr >> 8).astype(np.uint8)
        img = Image.fromarray(arr, mode="L")
    elif img.mode in ("I", "F"):
        img = img.convert("L")

    # Handle transparency → composite on white background
    if img.mode in ("RGBA", "P", "LA", "PA"):
        if img.mode in ("P", "PA"):
            img = img.convert("RGBA")
        background = Image.new("RGB", img.size, (255, 255, 255))
        background.paste(img, mask=img.split()[-1])
        img = background
    elif img.mode != "RGB":
        img = img.convert("RGB")

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    return buf.getvalue(), ".jpeg", "image/jpeg"


async def download_image(client: httpx.AsyncClient, url: str) -> tuple[bytes, str]:
    """Download image and return (bytes, content_type)."""
    resp = await client.get(url, follow_redirects=True, timeout=30.0)
    resp.raise_for_status()
    return resp.content, resp.headers.get("content-type", "")


def file_exists_in_bucket(path: str) -> bool:
    """Check if an object exists in the Railway bucket. Idempotency gate."""
    try:
        s3.head_object(Bucket=R2_BUCKET, Key=path)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def upload_image(path: str, data: bytes, content_type: str) -> None:
    """Upload to Railway bucket."""
    s3.put_object(
        Bucket=R2_BUCKET,
        Key=path,
        Body=data,
        ContentType=content_type,
        CacheControl="3600",
    )


def list_folder_files(folder: str) -> list[str]:
    """List all object keys in a bucket folder (prefix)."""
    keys = []
    prefix = folder.rstrip("/") + "/"
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                keys.append(obj["Key"])
    except ClientError:
        return []
    return keys


def delete_files(paths: list[str]) -> None:
    """Delete objects from the Railway bucket (batched, S3 caps at 1000 per call)."""
    if not paths:
        return
    for i in range(0, len(paths), 1000):
        batch = [{"Key": p} for p in paths[i:i + 1000]]
        s3.delete_objects(Bucket=R2_BUCKET, Delete={"Objects": batch, "Quiet": True})


def upsert_storage_file_row(sku: str, path: str) -> None:
    """Insert/update a row in storage_files_railway for this file.
    Keyed on `path` so repeat uploads of the same key are idempotent on the DB side too.
    """
    folder, _, file_name = path.rpartition("/")
    row = {
        "product_id": sku,
        "path": path,
        "folder": folder,
        "file_name": file_name,
    }
    # Upsert on `path` — requires a unique constraint on storage_files_railway.path
    # (falls back to insert if the row doesn't exist)
    supabase.table("storage_files_railway").upsert(row, on_conflict="path").execute()


def delete_storage_file_rows(paths: list[str]) -> None:
    """Delete rows from storage_files_railway whose `path` matches (orphan cleanup)."""
    if not paths:
        return
    # Supabase `in_` filter — batch by 200 to keep URL length sane
    for i in range(0, len(paths), 200):
        batch = paths[i:i + 200]
        try:
            supabase.table("storage_files_railway").delete().in_("path", batch).execute()
        except Exception:
            pass  # don't fail the whole request on a DB-side cleanup error


@app.get("/health")
def health():
    """Unprotected — used by Railway health check."""
    return {"status": "ok"}


@app.post("/sync-product-images")
async def sync_product_images(request: Request, _: None = Depends(verify_webhook_secret)):
    payload = await request.json()
    sku = payload.get("sku")
    product_images = payload.get("product_images") or []
    showroom_images = payload.get("showroom_images") or []

    if not sku:
        return {"error": "SKU is required"}

    folder = sku_to_folder(sku)
    results = {"uploaded": [], "already_present": [], "deleted": [], "errors": []}

    # Collect every image in the payload — the Railway convertor processes ALL urls,
    # not just "external" ones, because the Railway bucket starts empty and we're
    # building the full mirror from scratch.
    items = []
    for i, url in enumerate(product_images):
        if url:
            items.append({"url": url, "base": f"{folder}/product_{i+1}", "type": "product"})
    for i, url in enumerate(showroom_images):
        if url:
            items.append({"url": url, "base": f"{folder}/showroom_{i+1}", "type": "showroom"})

    # Track which bucket keys SHOULD exist for this SKU after this sync.
    # Anything in the folder NOT in this set is an orphan.
    expected_keys = set()

    async with httpx.AsyncClient() as client:
        for item in items:
            # We don't yet know the file extension, but the current convertor
            # always outputs .jpeg. Compute the target key up front.
            target_key = item["base"] + ".jpeg"
            expected_keys.add(target_key)

            # Step-by-step with labeled errors so we know exactly which host/op failed.
            try:
                exists = file_exists_in_bucket(target_key)
            except Exception as e:
                results["errors"].append(f"{item['base']} [head_object on Railway bucket]: {type(e).__name__}: {str(e)}")
                continue

            if exists:
                results["already_present"].append(target_key)
                try:
                    upsert_storage_file_row(sku, target_key)
                except Exception as e:
                    results["errors"].append(f"{item['base']} [db upsert on Supabase]: {type(e).__name__}: {str(e)}")
                continue

            try:
                raw_bytes, content_type = await download_image(client, item["url"])
            except Exception as e:
                results["errors"].append(f"{item['base']} [download from {item['url']}]: {type(e).__name__}: {str(e)}")
                continue

            try:
                converted, ext, mime = convert_image(raw_bytes, item["url"], content_type)
            except Exception as e:
                results["errors"].append(f"{item['base']} [image conversion]: {type(e).__name__}: {str(e)}")
                continue

            full_path = item["base"] + ext
            expected_keys.discard(target_key)
            expected_keys.add(full_path)

            try:
                upload_image(full_path, converted, mime)
            except Exception as e:
                results["errors"].append(f"{item['base']} [put_object on Railway bucket]: {type(e).__name__}: {str(e)}")
                continue

            try:
                upsert_storage_file_row(sku, full_path)
            except Exception as e:
                results["errors"].append(f"{item['base']} [db upsert on Supabase]: {type(e).__name__}: {str(e)}")
                # File is already uploaded, so count it as uploaded but flag the db error
                results["uploaded"].append(full_path)
                continue

            results["uploaded"].append(full_path)

    # Orphan cleanup — list everything in the SKU folder, delete anything not in expected_keys.
    existing = list_folder_files(folder)
    to_delete = [k for k in existing if k not in expected_keys]
    if to_delete:
        delete_files(to_delete)
        delete_storage_file_rows(to_delete)
        results["deleted"].extend(to_delete)

    return {"success": True, "results": results}


@app.post("/sync-all-images")
async def sync_all_images(request: Request, _: None = Depends(verify_webhook_secret)):
    """Process every inventory row, sequentially. Used for bulk catch-up migration."""
    # Fetch all products — we process every SKU that has ANY images in the payload,
    # regardless of where those images currently live (vendor URL or Supabase).
    offset = 0
    batch_size = 100
    all_products = []
    while True:
        resp = supabase.table("inventory").select(
            "sku, product_images, showroom_images"
        ).range(offset, offset + batch_size - 1).execute()
        if not resp.data:
            break
        all_products.extend(resp.data)
        if len(resp.data) < batch_size:
            break
        offset += batch_size

    # Filter to products with at least one image URL
    to_process = [
        p for p in all_products
        if (p.get("product_images") or []) or (p.get("showroom_images") or [])
    ]

    summary = {"total": len(to_process), "processed": 0, "failed": 0, "errors": []}

    async with httpx.AsyncClient() as client:
        for product in to_process:
            sku = product["sku"]
            try:
                product_images = product.get("product_images") or []
                showroom_images = product.get("showroom_images") or []
                folder = sku_to_folder(sku)

                items = []
                for i, url in enumerate(product_images):
                    if url:
                        items.append({"url": url, "base": f"{folder}/product_{i+1}", "type": "product"})
                for i, url in enumerate(showroom_images):
                    if url:
                        items.append({"url": url, "base": f"{folder}/showroom_{i+1}", "type": "showroom"})

                expected_keys = set()

                for item in items:
                    target_key = item["base"] + ".jpeg"
                    expected_keys.add(target_key)

                    if file_exists_in_bucket(target_key):
                        upsert_storage_file_row(sku, target_key)
                        continue

                    raw_bytes, content_type = await download_image(client, item["url"])
                    converted, ext, mime = convert_image(raw_bytes, item["url"], content_type)
                    full_path = item["base"] + ext
                    expected_keys.discard(target_key)
                    expected_keys.add(full_path)

                    upload_image(full_path, converted, mime)
                    upsert_storage_file_row(sku, full_path)

                # Orphan cleanup per SKU folder
                existing = list_folder_files(folder)
                to_delete = [k for k in existing if k not in expected_keys]
                if to_delete:
                    delete_files(to_delete)
                    delete_storage_file_rows(to_delete)

                summary["processed"] += 1
            except Exception as e:
                summary["failed"] += 1
                summary["errors"].append(f"{sku}: {str(e)}")

    return {"success": True, "summary": summary}
