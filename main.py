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
    """Convert image to JPEG or PNG. Returns (bytes, extension, mime_type)."""
    url_lower = source_url.lower()

    # Source is PNG - keep as PNG
    if url_lower.endswith(".png") or "png" in content_type:
        img = Image.open(io.BytesIO(raw_bytes))
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue(), ".png", "image/png"

    # Source is already JPEG - pass through
    if any(url_lower.endswith(ext) for ext in (".jpg", ".jpeg")) or "jpeg" in content_type:
        return raw_bytes, ".jpeg", "image/jpeg"

    # Everything else (webp, gif, bmp, tiff, avif) - convert to JPEG
    img = Image.open(io.BytesIO(raw_bytes))
    if img.mode in ("RGBA", "P", "LA"):
        # JPEG doesn't support transparency - composite on white background
        background = Image.new("RGB", img.size, (255, 255, 255))
        if img.mode == "P":
            img = img.convert("RGBA")
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
    return res


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
