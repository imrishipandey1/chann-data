#!/usr/bin/env python3
"""
dishtv_multi_scrape.py

Reads channel list from 'channel.txt' (one per line, format: "144478= Zee TV" or "144478")
Fetches today's and tomorrow's EPG for each channel in parallel, saves JSON files into:
  ./today/<channel-slug>.json
  ./tomorrow/<channel-slug>.json

Creates a detailed log file: scrape_log_<timestamp>.log

Configurable constants:
  - MAX_WORKERS: number of parallel threads
  - MAX_RETRIES: number of retries per request
  - RETRY_BACKOFF: base backoff seconds (exponential)

No external dependencies required.
"""

import requests
import json
import os
import sys
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- CONFIG ----------
SIGNIN_URL = "https://www.dishtv.in/services/epg/signin"
PROGRAMS_URL = "https://epg.mysmartstick.com/dishtv/api/v1/epg/entities/programs"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://www.dishtv.in",
    "Referer": "https://www.dishtv.in/",
}

CHANNELS_FILE = "channel.txt"   # file containing channel ids (one per line)
OUT_DIR_TODAY = "today"
OUT_DIR_TOMORROW = "tomorrow"

MAX_WORKERS = 10       # number of parallel threads
MAX_RETRIES = 3        # number of attempts per API call
RETRY_BACKOFF = 1.2    # base backoff multiplier (seconds) for exponential backoff

# ---------- END CONFIG ----------

lock = threading.Lock()

# Progress tracking
progress = {
    "total": 0,
    "done": 0
}

# Log file
LOG_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = f"scrape_log_{LOG_TS}.log"


def write_log(line: str):
    """Append a line to the log file (thread-safe)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{ts}] {line}\n"
    with lock:
        with open(LOG_FILE, "a", encoding="utf-8") as lf:
            lf.write(entry)


def simple_progress_bar():
    """Print a simple progress bar (thread-safe)."""
    with lock:
        total = progress["total"]
        done = progress["done"]
        pct = (done / total * 100) if total else 100
        bar_len = 40
        filled = int(bar_len * done / total) if total else bar_len
        bar = "#" * filled + "-" * (bar_len - filled)
        sys.stdout.write(f"\rProgress: |{bar}| {done}/{total} ({pct:.1f}%)")
        sys.stdout.flush()
        if done == total:
            sys.stdout.write("\n")


def ensure_dirs():
    os.makedirs(OUT_DIR_TODAY, exist_ok=True)
    os.makedirs(OUT_DIR_TOMORROW, exist_ok=True)


def parse_channel_file(filename: str):
    """
    Read channel file and return list of tuples: (channelid, channel_name_or_empty)
    Accepts:
      144478= Zee TV
      143831= Star Plus
      54022
    """
    channels = []
    with open(filename, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                left, right = line.split("=", 1)
                channelid = left.strip()
                name = right.strip()
            else:
                channelid = line.strip()
                name = ""
            channels.append((channelid, name))
    return channels


def get_token_session(session: requests.Session):
    resp = session.post(SIGNIN_URL, headers=HEADERS, json={}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if "token" not in data:
        raise RuntimeError(f"signin response missing token: {data}")
    return data["token"]


def fetch_epg_for_date(session: requests.Session, channelid: str, date_ddmmyyyy: str):
    payload = {
        "channelid": channelid,
        "date": date_ddmmyyyy,
        "allowPastEvents": True
    }
    resp = session.post(PROGRAMS_URL, headers=HEADERS, json=payload, timeout=20)
    resp.raise_for_status()
    return resp.json()


def time_12h_no_tz(iso_str):
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return dt.strftime("%I:%M %p")


def make_slug_from_txt_name(name: str):
    """Make slug from NAME IN channel.txt — EXACTLY what you requested."""
    if not name:
        return "channel.json"
    slug = name.lower().strip().replace(" ", "-")
    slug = "".join(c for c in slug if c.isalnum() or c in "-")
    return slug + ".json"


def format_output_from_epg(epg_data):
    """Formats final output exactly as required."""
    programs = epg_data if isinstance(epg_data, list) else epg_data.get("programs", [])
    channel_name = programs[0].get("channelname", "Unknown Channel") if programs else "Unknown Channel"

    # date based on first program
    if programs:
        dt = datetime.fromisoformat(programs[0]["start"].replace("Z", "+00:00"))
        date_str = dt.strftime("%B %d, %Y")
    else:
        date_str = datetime.now().strftime("%B %d, %Y")

    schedule = []
    for p in programs:
        schedule.append({
            "show_name": p.get("title", ""),
            "start_time": time_12h_no_tz(p["start"]),
            "end_time": time_12h_no_tz(p["stop"]),
            "show_logo": p.get("programmeurl", "")
        })

    return {
        "channel_name": channel_name,
        "date": date_str,
        "schedule": schedule
    }


def save_json_out(data: dict, out_dir: str, txt_channel_name: str):
    """Saves JSON using NAME FROM CHANNEL.TXT (your request)"""
    filename = make_slug_from_txt_name(txt_channel_name)
    path = os.path.join(out_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return path


def attempt_fetch_channel(channel_tuple, date_ddmmyyyy, out_dir):
    channelid, txt_name = channel_tuple
    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = requests.Session()
            session.headers.update(HEADERS)

            token = get_token_session(session)
            session.headers.update({"Authorization": token})

            epg = fetch_epg_for_date(session, channelid, date_ddmmyyyy)

            formatted = format_output_from_epg(epg)

            saved_path = save_json_out(formatted, out_dir, txt_name)

            return True, saved_path

        except Exception as e:
            last_err = e
            write_log(f"Attempt {attempt} failed for {channelid} on {date_ddmmyyyy}: {repr(e)}")
            time.sleep(RETRY_BACKOFF ** attempt)

    return False, str(last_err)


def worker_task(channel_tuple, date_ddmmyyyy, out_dir):
    ok, info = attempt_fetch_channel(channel_tuple, date_ddmmyyyy, out_dir)

    channelid, txt_name = channel_tuple
    if ok:
        write_log(f"SUCCESS: {channelid} ({txt_name}) date={date_ddmmyyyy} saved={info}")
    else:
        write_log(f"FAIL: {channelid} ({txt_name}) date={date_ddmmyyyy} reason={info}")

    with lock:
        progress["done"] += 1
    simple_progress_bar()

    return ok


def main():
    ensure_dirs()

    with open(LOG_FILE, "w") as lf:
        lf.write(f"Scrape started {datetime.now().isoformat()}\n")

    if not os.path.exists(CHANNELS_FILE):
        print("channel.txt not found")
        sys.exit(1)

    channels = parse_channel_file(CHANNELS_FILE)

    today_ddmmyyyy = datetime.now().strftime("%d/%m/%Y")
    tomorrow_ddmmyyyy = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")

    progress["total"] = len(channels) * 2
    progress["done"] = 0

    print(f"[+] Scraping {len(channels)} channels (today + tomorrow) in parallel...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []

        # Today
        for ch in channels:
            futures.append(executor.submit(worker_task, ch, today_ddmmyyyy, OUT_DIR_TODAY))

        # Tomorrow
        for ch in channels:
            futures.append(executor.submit(worker_task, ch, tomorrow_ddmmyyyy, OUT_DIR_TOMORROW))

        for f in as_completed(futures):
            pass

    print("\n[+] Done.")


if __name__ == "__main__":
    main()
