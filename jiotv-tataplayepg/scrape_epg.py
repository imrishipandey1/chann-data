#!/usr/bin/env python3
"""
Scrape two gzipped EPG XMLs (JioTV and Tata), prefer JioTV schedules,
and write one JSON per channel for today and tomorrow (IST) into:
  today/{channel-slug}.json
  tomorrow/{channel-slug}.json

If no schedule is found for both days → DO NOT create JSON files.

Also writes jiotv-tataplayepg/missing_channels.log listing channels that
were not found in either EPG or had no programmes for both days.

Requirements: requests
"""
import os
import gzip
import io
import re
import json
from datetime import datetime, timedelta, timezone
import requests
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple

# ---------- CONFIG ----------
BASE_DIR = "jiotv-tataplayepg"
TODAY_DIR = "today"
TOMORROW_DIR = "tomorrow"
FILTER_FILE = os.path.join(BASE_DIR, "filter_list.txt")
MISSING_LOG = os.path.join(BASE_DIR, "missing_channels.log")

JIO_URL = "https://avkb.short.gy/jioepg.xml.gz"
TATA_URL = "https://avkb.short.gy/tsepg.xml.gz"
# ----------------------------

# IST timezone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))


def slugify(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[\s]+", "-", name)
    name = re.sub(r"[^a-z0-9\-]", "", name)
    name = re.sub(r"-{2,}", "-", name)
    name = name.strip("-")
    return name or "channel"


def ensure_dirs():
    os.makedirs(TODAY_DIR, exist_ok=True)
    os.makedirs(TOMORROW_DIR, exist_ok=True)
    os.makedirs(BASE_DIR, exist_ok=True)


def download_and_parse_gz_xml(url: str) -> ET.Element:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.content
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
        xml_bytes = f.read()
    root = ET.fromstring(xml_bytes)
    return root


def extract_channels(root: ET.Element) -> Dict[str, str]:
    mapping = {}
    for ch in root.findall(".//channel"):
        ch_id = ch.get("id") or ch.get("utid") or ch.get("channel") or ""
        display_name = None
        for child in ch:
            tag = child.tag.lower()
            if "display" in tag or "name" in tag:
                txt = (child.text or "").strip()
                if txt:
                    display_name = txt
                    break
        if not display_name:
            for child in ch:
                txt = (child.text or "").strip()
                if txt:
                    display_name = txt
                    break
        if ch_id and display_name:
            mapping[ch_id] = display_name
    return mapping


def parse_programmes(root: ET.Element) -> List[Dict]:
    items = []
    for prog in root.findall(".//programme"):
        ch = prog.get("channel")
        start_attr = prog.get("start", "")
        stop_attr = prog.get("stop", "")

        def parse_dt(s: str):
            m = re.match(r"(\d{14})", s.strip())
            if not m:
                return None
            dt = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=timezone.utc)

        start_dt = parse_dt(start_attr)
        stop_dt = parse_dt(stop_attr)

        title = ""
        icon_url = ""

        for child in prog:
            tag = child.tag.lower()
            if tag.endswith("title") and (child.text and child.text.strip()):
                title = (child.text or "").strip()
            if tag.endswith("icon"):
                icon_url = child.get("src") or child.get("href") or ""

        if not icon_url:
            icon_elem = prog.find(".//icon")
            if icon_elem is not None:
                icon_url = icon_elem.get("src") or icon_elem.get("href") or ""

        if ch and start_dt and stop_dt:
            items.append({
                "channel_id": ch,
                "start_utc": start_dt,
                "stop_utc": stop_dt,
                "title": title,
                "icon": icon_url
            })
    return items


def group_by_channel(programmes: List[Dict]) -> Dict[str, List[Dict]]:
    d = {}
    for p in programmes:
        d.setdefault(p["channel_id"], []).append(p)
    return d


def to_ist(dt_utc: datetime) -> datetime:
    return dt_utc.astimezone(IST)


def format_date(dt_ist: datetime) -> str:
    return dt_ist.strftime("%B %d, %Y")


def format_time_12h(dt_ist: datetime) -> str:
    return dt_ist.strftime("%I:%M %p").lstrip("0") if dt_ist.strftime("%I").startswith("0") else dt_ist.strftime("%I:%M %p")


def build_schedule_for_day(channel_progs: List[Dict], day_date: datetime.date) -> List[Dict]:
    out = []
    for p in channel_progs:
        s_ist = to_ist(p["start_utc"])
        e_ist = to_ist(p["stop_utc"])
        if s_ist.date() == day_date:
            out.append({
                "show_name": p["title"] or "",
                "start_time": format_time_12h(s_ist),
                "end_time": format_time_12h(e_ist),
                "show_logo": p["icon"] or ""
            })
    out.sort(key=lambda x: datetime.strptime(x["start_time"], "%I:%M %p"))
    return out


def main():
    ensure_dirs()

    if not os.path.exists(FILTER_FILE):
        print(f"Filter file not found: {FILTER_FILE}")
        return

    with open(FILTER_FILE, "r", encoding="utf-8") as f:
        filters = [line.strip() for line in f if line.strip()]

    lower_filters = {f.lower(): f for f in filters}

    print("Downloading JioTV EPG...")
    jio_root = download_and_parse_gz_xml(JIO_URL)

    print("Downloading Tata EPG...")
    tata_root = download_and_parse_gz_xml(TATA_URL)

    print("Extracting channels...")
    jio_channels = extract_channels(jio_root)
    tata_channels = extract_channels(tata_root)

    jio_name_to_id = {v.lower(): k for k, v in jio_channels.items()}
    tata_name_to_id = {v.lower(): k for k, v in tata_channels.items()}

    print("Parsing programmes...")
    jio_programmes = parse_programmes(jio_root)
    tata_programmes = parse_programmes(tata_root)

    jio_by_channel = group_by_channel(jio_programmes)
    tata_by_channel = group_by_channel(tata_programmes)

    now_ist = datetime.now(IST)
    today_date = now_ist.date()
    tomorrow_date = (now_ist + timedelta(days=1)).date()

    missing_lines = []

    for filt_lower, filt_original in lower_filters.items():
        ch_id = None
        source = None

        if filt_lower in jio_name_to_id:
            ch_id = jio_name_to_id[filt_lower]
            source = "jio"
        elif filt_lower in tata_name_to_id:
            ch_id = tata_name_to_id[filt_lower]
            source = "tata"
        else:
            missing_lines.append(f"{filt_original} - Channel not found in JioTV or Tata")
            continue  # NO JSON should be created
        

        channel_progs = jio_by_channel.get(ch_id, []) if source == "jio" else tata_by_channel.get(ch_id, [])

        schedule_today = build_schedule_for_day(channel_progs, today_date)
        schedule_tomorrow = build_schedule_for_day(channel_progs, tomorrow_date)

        # ❗ SKIP saving files if both schedules empty
        if not schedule_today and not schedule_tomorrow:
            missing_lines.append(f"{filt_original} - Found in {source} but no programmes for today & tomorrow")
            continue

        # Save schedule files (only if non-empty)
        for day_dir, day_date, schedule in (
            (TODAY_DIR, today_date, schedule_today),
            (TOMORROW_DIR, tomorrow_date, schedule_tomorrow)
        ):
            outpath = os.path.join(day_dir, f"{slugify(filt_original)}.json")
            payload = {
                "channel_name": filt_original,
                "date": format_date(datetime.combine(day_date, datetime.min.time())),
                "schedule": schedule
            }
            with open(outpath, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=False)

    # Write missing channel log
    with open(MISSING_LOG, "w", encoding="utf-8") as f:
        f.write("\n".join(missing_lines))

    print("Done.")


if __name__ == "__main__":
    main()
