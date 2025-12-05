#!/usr/bin/env python3
"""
download_images_parallel.py (updated URL output + resize for URLs without size params)

Only changes:
--------------------------------
1. Still writes:
   https://intvschedule.com/wp-content/uploads/downloaded-images/...
2. If image URL does NOT contain "lock=", we download full image and THEN:
      - Resize to width 250 (preserve aspect ratio)
      - Save as webp
Everything else remains identical.
"""

import os
import sys
import json
import hashlib
import logging
import threading
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, ParseResult, quote, unquote
from io import BytesIO

import requests
from requests.adapters import HTTPAdapter, Retry
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ----- CONFIG -----
FOLDERS = [
    "/Users/rishipandey/Projects/dishtv-scrapper/today",
    "/Users/rishipandey/Projects/dishtv-scrapper/tomorrow",
]

OUTPUT_BASE = Path("./downloaded-images")

WP_PREFIX = "https://intvschedule.com/wp-content/uploads/downloaded-images"

TARGET_WIDTH = 250
MAX_WORKERS = 30
REQUEST_TIMEOUT = 20
RETRIES = 3
BACKOFF_FACTOR = 0.5

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("img-downloader")

def make_session():
    session = requests.Session()
    retries = Retry(
        total=RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

SESSION = make_session()
SESSION.headers.update({"User-Agent": "dishtv-image-downloader/2.0"})


def slug_from_filename(json_path: Path) -> str:
    return json_path.stem.lower().replace(" ", "-")


def parse_and_adjust_size(url: str, target_width: int) -> str:
    """If URL has lock=W×H, adjust it. If not, leave unchanged."""
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)

        # NEW LOGIC → If no lock param, leave URL unchanged
        if "lock" not in qs:
            return url

        lock_vals = qs["lock"]
        lock = lock_vals[0]

        if "x" in lock:
            w, h = lock.split("x")
            try:
                w = int(w)
                h = int(h)
                new_h = max(1, round(h * (target_width / w)))
                qs["lock"] = [f"{target_width}x{new_h}"]
            except:
                qs["lock"] = [f"{target_width}x{target_width}"]

        new_query = urlencode(qs, doseq=True)

        new_parsed = ParseResult(
            scheme=parsed.scheme,
            netloc=parsed.netloc,
            path=parsed.path,
            params=parsed.params,
            query=new_query,
            fragment=parsed.fragment
        )

        return urlunparse(new_parsed)

    except:
        return url


def url_basename(url: str) -> str:
    p = urlparse(url)
    name = os.path.basename(unquote(p.path))
    if not name:
        name = hashlib.md5(url.encode("utf-8")).hexdigest() + ".img"
    return name


def unique_filename_for(url: str, basename: str) -> str:
    base, _ = os.path.splitext(basename)
    h = hashlib.md5(url.encode("utf-8")).hexdigest()[:10]
    return f"{base}_{h}.webp".replace(" ", "_")


def ensure_dir(p: Path):
    if not p.exists():
        p.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------------------------
# ✔️ ONLY FUNCTION MODIFIED BELOW (RESIZE LOCAL IF URL HAS NO lock=)
# -------------------------------------------------------------------
def download_and_convert_to_webp(norm_url: str, save_path: Path, session: requests.Session) -> bool:
    tmp = save_path.with_suffix(".part")

    try:
        resp = session.get(norm_url, stream=True, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        content = BytesIO()
        for chunk in resp.iter_content(8192):
            if chunk:
                content.write(chunk)
        content.seek(0)

        try:
            img = Image.open(content)

            # ---------------------------
            # ✔️ NEW LOGIC: Resize if URL has NO lock param
            # ---------------------------
            if "lock=" not in norm_url:
                w, h = img.size
                new_w = TARGET_WIDTH
                new_h = int((h / w) * new_w)
                img = img.resize((new_w, new_h), Image.LANCZOS)

            if img.mode in ("RGBA", "P"):
                img = img.convert("RGBA")
            else:
                img = img.convert("RGB")

            ensure_dir(save_path.parent)
            img.save(tmp, "WEBP", quality=80, method=6)

            tmp.replace(save_path)
            return True

        except Exception as e:
            logger.warning(f"Image convert failed {norm_url}: {e}")
            tmp.unlink(missing_ok=True)
            return False

    except Exception as e:
        logger.warning(f"Download failed {norm_url}: {e}")
        tmp.unlink(missing_ok=True)
        return False


def process_json_file(json_path: Path, session: requests.Session):

    per_file_downloaded = {}  # dedupe inside file only

    try:
        data = json.load(open(json_path, "r", encoding="utf-8"))
    except Exception as e:
        logger.error(f"Cannot read JSON {json_path}: {e}")
        return

    slug = slug_from_filename(json_path)
    day = "today" if "today" in str(json_path).lower() else "tomorrow"

    schedule = data.get("schedule", [])
    if not isinstance(schedule, list):
        return

    to_update = []
    for idx, item in enumerate(schedule):
        if isinstance(item, dict) and item.get("show_logo"):
            to_update.append((idx, item["show_logo"]))

    if not to_update:
        return

    norm_entries = {}
    orig_to_norm = {}

    for idx, url in to_update:
        norm = parse_and_adjust_size(url, TARGET_WIDTH)
        orig_to_norm[url] = norm
        norm_entries.setdefault(norm, []).append(idx)

    tasks = []
    for norm_url in norm_entries:
        basename = url_basename(norm_url)
        filename = unique_filename_for(norm_url, basename)

        local_dir = OUTPUT_BASE / slug / day
        ensure_dir(local_dir)
        local_path = local_dir / filename

        if norm_url in per_file_downloaded:
            continue

        if local_path.exists():
            per_file_downloaded[norm_url] = str(local_path)
            continue

        tasks.append((norm_url, local_path))

    if tasks:
        logger.info(f"{json_path.name}: downloading {len(tasks)} images...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {
                ex.submit(download_and_convert_to_webp, url, path, session): (url, path)
                for url, path in tasks
            }

            for fut in tqdm(as_completed(futures), total=len(futures), unit="img", desc=f"{json_path.name}"):
                pass

        for norm_url, local_path in tasks:
            if local_path.exists():
                per_file_downloaded[norm_url] = str(local_path)
            else:
                logger.warning(f"Missing after download: {norm_url}")

    for idx, orig_url in to_update:
        norm_url = orig_to_norm[orig_url]
        local = per_file_downloaded.get(norm_url)

        if local:
            filename = os.path.basename(local)
            new_val = f"{WP_PREFIX}/{slug}/{day}/{filename}"
            data["schedule"][idx]["show_logo"] = new_val
        else:
            logger.warning(f"Failed image for {orig_url}")

    json.dump(data, open(json_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    logger.info(f"Updated JSON: {json_path}")


def gather_json_files(folders):
    files = []
    for folder in folders:
        p = Path(folder)
        if not p.exists():
            continue
        files.extend(list(p.glob("*.json")))
    return files


def main():
    files = gather_json_files(FOLDERS)
    logger.info(f"Processing {len(files)} JSON files...")

    for file in files:
        process_json_file(file, SESSION)

    logger.info("All done.")


if __name__ == "__main__":
    main()
