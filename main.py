import os
import re
import io
import json
import httpx
from fastapi import FastAPI, Request, Response
from PIL import Image
from supabase import create_client

app = FastAPI()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
BUCKET = "product-images"

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def sku_to_folder(sku: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "", sku)


def is_supabase_url(url: str) -> bool:
    return "supabase.co/storage/" in url


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


def upload_image(path: str, data: bytes, content_type: str) -> str:
    """Upload to Supabase storage and return public URL."""
    supabase.storage.from_(BUCKET).upload(
        path, data,
        file_options={"content-type": content_type, "upsert": "true", "cache-control": "3600"}
    )
    res = supabase.storage.from_(BUCKET).get_public_url(path)
    # Strip trailing '?' that supabase-py adds
    return res.rstrip("?")


def list_folder_files(folder: str) -> list[str]:
    """List all files in a storage folder."""
    try:
        files = supabase.storage.from_(BUCKET).list(folder, {"limit": 1000})
        return [f"{folder}/{f['name']}" for f in files if f.get("id")]
    except Exception:
        return []


def delete_files(paths: list[str]):
    """Delete files from storage."""
    if paths:
        supabase.storage.from_(BUCKET).remove(paths)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/sync-product-images")
async def sync_product_images(request: Request):
    # Validate webhook secret
    if WEBHOOK_SECRET and request.headers.get("x-webhook-secret") != WEBHOOK_SECRET:
        return Response(status_code=401, content="Unauthorized")

    payload = await request.json()
    sku = payload.get("sku")
    product_images = payload.get("product_images") or []
    showroom_images = payload.get("showroom_images") or []

    if not sku:
        return {"error": "SKU is required"}

    folder = sku_to_folder(sku)
    results = {"uploaded": [], "deleted": [], "skipped": [], "errors": []}

    # Collect items to process
    items = []
    for i, url in enumerate(product_images):
        if url and not is_supabase_url(url):
            items.append({"url": url, "base": f"{folder}/product_{i+1}", "type": "product"})
        elif url and is_supabase_url(url):
            results["skipped"].append(url)

    for i, url in enumerate(showroom_images):
        if url and not is_supabase_url(url):
            items.append({"url": url, "base": f"{folder}/showroom_{i+1}", "type": "showroom"})
        elif url and is_supabase_url(url):
            results["skipped"].append(url)

    if not items:
        return {"success": True, "message": "No new images to sync", "results": results}

    # Download, convert, upload
    new_product_urls = []
    new_showroom_urls = []
    uploaded_paths = set()

    async with httpx.AsyncClient() as client:
        for item in items:
            try:
                raw_bytes, content_type = await download_image(client, item["url"])
                converted, ext, mime = convert_image(raw_bytes, item["url"], content_type)
                full_path = item["base"] + ext
                public_url = upload_image(full_path, converted, mime)

                results["uploaded"].append(full_path)
                uploaded_paths.add(full_path)

                if item["type"] == "product":
                    new_product_urls.append(public_url)
                else:
                    new_showroom_urls.append(public_url)
            except Exception as e:
                results["errors"].append(f"{item['base']}: {str(e)}")

    # Clean up orphaned files
    existing = list_folder_files(folder)
    to_delete = []
    for path in existing:
        if path not in uploaded_paths:
            is_kept = any(path in url for url in results["skipped"])
            if not is_kept:
                to_delete.append(path)
    if to_delete:
        delete_files(to_delete)
        results["deleted"].extend(to_delete)

    # Update inventory record
    update_data = {}
    if new_product_urls:
        final = []
        url_iter = iter(new_product_urls)
        for url in product_images:
            if is_supabase_url(url):
                final.append(url)
            else:
                final.append(next(url_iter, url))
        update_data["product_images"] = final

    if new_showroom_urls:
        final = []
        url_iter = iter(new_showroom_urls)
        for url in showroom_images:
            if is_supabase_url(url):
                final.append(url)
            else:
                final.append(next(url_iter, url))
        update_data["showroom_images"] = final

    if update_data:
        try:
            supabase.table("inventory").update(update_data).eq("sku", sku).execute()
        except Exception as e:
            results["errors"].append(f"DB update failed: {str(e)}")

    return {"success": True, "results": results}


@app.post("/sync-all-images")
async def sync_all_images(request: Request):
    """Process all inventory products with external images, sequentially."""
    if WEBHOOK_SECRET and request.headers.get("x-webhook-secret") != WEBHOOK_SECRET:
        return Response(status_code=401, content="Unauthorized")

    # Fetch all products with images
    offset = 0
    batch_size = 100
    all_products = []
    while True:
        resp = supabase.table("inventory").select("sku, product_images, showroom_images").range(offset, offset + batch_size - 1).execute()
        if not resp.data:
            break
        all_products.extend(resp.data)
        if len(resp.data) < batch_size:
            break
        offset += batch_size

    # Filter to products with external images
    to_process = []
    for p in all_products:
        has_external = False
        for img in (p.get("product_images") or []):
            if img and not is_supabase_url(img):
                has_external = True
                break
        if not has_external:
            for img in (p.get("showroom_images") or []):
                if img and not is_supabase_url(img):
                    has_external = True
                    break
        if has_external:
            to_process.append(p)

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
                    if url and not is_supabase_url(url):
                        items.append({"url": url, "base": f"{folder}/product_{i+1}", "type": "product"})
                for i, url in enumerate(showroom_images):
                    if url and not is_supabase_url(url):
                        items.append({"url": url, "base": f"{folder}/showroom_{i+1}", "type": "showroom"})

                new_product_urls = []
                new_showroom_urls = []

                for item in items:
                    raw_bytes, content_type = await download_image(client, item["url"])
                    converted, ext, mime = convert_image(raw_bytes, item["url"], content_type)
                    full_path = item["base"] + ext
                    public_url = upload_image(full_path, converted, mime)
                    if item["type"] == "product":
                        new_product_urls.append(public_url)
                    else:
                        new_showroom_urls.append(public_url)

                update_data = {}
                if new_product_urls:
                    final = []
                    url_iter = iter(new_product_urls)
                    for url in product_images:
                        if is_supabase_url(url):
                            final.append(url)
                        else:
                            final.append(next(url_iter, url))
                    update_data["product_images"] = final
                if new_showroom_urls:
                    final = []
                    url_iter = iter(new_showroom_urls)
                    for url in showroom_images:
                        if is_supabase_url(url):
                            final.append(url)
                        else:
                            final.append(next(url_iter, url))
                    update_data["showroom_images"] = final

                if update_data:
                    supabase.table("inventory").update(update_data).eq("sku", sku).execute()

                summary["processed"] += 1
            except Exception as e:
                summary["failed"] += 1
                summary["errors"].append(f"{sku}: {str(e)}")

    return {"success": True, "summary": summary}
