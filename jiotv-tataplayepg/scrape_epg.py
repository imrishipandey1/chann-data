#!/usr/bin/env python3
"""
Scrape two gzipped EPG XMLs (JioTV and Tata), prefer JioTV schedules,
and write one JSON per channel for today and tomorrow (IST) into:
  today/{channel-slug}.json
  tomorrow/{channel-slug}.json

If no schedule is found for a given day => DO NOT save JSON for that day.
If no schedule found for both days => skip both + log missing.

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
from typing import Dict, List

# ---------- CONFIG ----------
BASE_DIR = "jiotv-tataplayepg"
TODAY_DIR = "today"
TOMORROW_DIR = "tomorrow"
FILTER_FILE = os.path.join(BASE_DIR, "filter_list.txt")
MISSING_LOG = os.path.join(BASE_DIR, "missing_channels.log")

JIO_URL = "https://avkb.short.gy/jioepg.xml.gz"
TATA_URL = "https://avkb.short.gy/tsepg.xml.gz"
# ----------------------------

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
    return ET.fromstring(xml_bytes)


def extract_channels(root: ET.Element) -> Dict[str, str]:
    mapping = {}
    for ch in root.findall(".//channel"):
        ch_id = ch.get("id") or ""
        name = None
        for child in ch:
            t = child.tag.lower()
            if "display" in t or "name" in t:
                txt = (child.text or "").strip()
                if txt:
                    name = txt
                    break
        if not name:
            for child in ch:
                txt = (child.text or "").strip()
                if txt:
                    name = txt
                    break
        if ch_id and name:
            mapping[ch_id] = name
    return mapping


def parse_programmes(root: ET.Element) -> List[Dict]:
    items = []
    for prog in root.findall(".//programme"):
        ch = prog.get("channel")
        start_attr = prog.get("start", "")
        stop_attr = prog.get("stop", "")

        def parse_dt(s):
            m = re.match(r"(\d{14})", s.strip())
            if not m:
                return None
            dt = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=timezone.utc)

        sdt = parse_dt(start_attr)
        edt = parse_dt(stop_attr)

        title = ""
        icon = ""

        for child in prog:
            tg = child.tag.lower()
            if tg.endswith("title") and (child.text and child.text.strip()):
                title = child.text.strip()
            if tg.endswith("icon"):
                icon = child.get("src") or child.get("href") or ""

        if not icon:
            icon_el = prog.find(".//icon")
            if icon_el is not None:
                icon = icon_el.get("src") or icon_el.get("href") or ""

        if ch and sdt and edt:
            items.append({
                "channel_id": ch,
                "start_utc": sdt,
                "stop_utc": edt,
                "title": title,
                "icon": icon
            })

    return items


def group_by_channel(programmes: List[Dict]) -> Dict[str, List[Dict]]:
    d = {}
    for p in programmes:
        d.setdefault(p["channel_id"], []).append(p)
    return d


def to_ist(dt):
    return dt.astimezone(IST)


def format_date(dt):
    return dt.strftime("%B %d, %Y")


def format_time(dt):
    return dt.strftime("%I:%M %p").lstrip("0") if dt.strftime("%I").startswith("0") else dt.strftime("%I:%M %p")


def build_schedule_for_day(channel_progs, date):
    out = []
    for p in channel_progs:
        s = to_ist(p["start_utc"])
        e = to_ist(p["stop_utc"])
        if s.date() == date:
            out.append({
                "show_name": p["title"],
                "start_time": format_time(s),
                "end_time": format_time(e),
                "show_logo": p["icon"]
            })
    out.sort(key=lambda x: datetime.strptime(x["start_time"], "%I:%M %p"))
    return out


def main():
    ensure_dirs()

    if not os.path.exists(FILTER_FILE):
        print("filter_list.txt missing")
        return

    filters = [x.strip() for x in open(FILTER_FILE, "r", encoding="utf-8") if x.strip()]
    filter_map = {f.lower(): f for f in filters}

    print("Downloading EPGs...")
    jio_root = download_and_parse_gz_xml(JIO_URL)
    tata_root = download_and_parse_gz_xml(TATA_URL)

    print("Extracting channels...")
    jio_ch = extract_channels(jio_root)
    tata_ch = extract_channels(tata_root)

    jio_map = {v.lower(): k for k, v in jio_ch.items()}
    tata_map = {v.lower(): k for k, v in tata_ch.items()}

    print("Parsing programmes...")
    jio_prog = group_by_channel(parse_programmes(jio_root))
    tata_prog = group_by_channel(parse_programmes(tata_root))

    today = datetime.now(IST).date()
    tomorrow = today + timedelta(days=1)

    missing = []

    for filt_lower, filt_original in filter_map.items():

        # determine channel_id
        ch_id = None
        source = None

        if filt_lower in jio_map:
            ch_id = jio_map[filt_lower]
            source = "jio"
        elif filt_lower in tata_map:
            ch_id = tata_map[filt_lower]
            source = "tata"
        else:
            missing.append(f"{filt_original} - Channel not found in JioTV or Tata")
            continue

        progs = jio_prog.get(ch_id, []) if source == "jio" else tata_prog.get(ch_id, [])

        sch_today = build_schedule_for_day(progs, today)
        sch_tomorrow = build_schedule_for_day(progs, tomorrow)

        # NEW FIX: Skip saving if empty for that day
        nothing_today = len(sch_today) == 0
        nothing_tomorrow = len(sch_tomorrow) == 0

        # if both empty → skip completely + log
        if nothing_today and nothing_tomorrow:
            missing.append(f"{filt_original} - Found in {source} but no programmes for today & tomorrow")
            continue

        # Save TODAY only if not empty
        if not nothing_today:
            path = os.path.join(TODAY_DIR, f"{slugify(filt_original)}.json")
            payload = {
                "channel_name": filt_original,
                "date": format_date(datetime.combine(today, datetime.min.time())),
                "schedule": sch_today
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)

        # Save TOMORROW only if not empty
        if not nothing_tomorrow:
            path = os.path.join(TOMORROW_DIR, f"{slugify(filt_original)}.json")
            payload = {
                "channel_name": filt_original,
                "date": format_date(datetime.combine(tomorrow, datetime.min.time())),
                "schedule": sch_tomorrow
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)

    # Write missing log
    with open(MISSING_LOG, "w", encoding="utf-8") as f:
        f.write("\n".join(missing))

    print("Done. No empty schedule will be saved.")


if __name__ == "__main__":
    main()
