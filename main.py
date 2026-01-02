import os
import csv
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

DATA_DIR = "data"
OUTPUT_DIR = "output"


@dataclass
class FundSnapshot:
    timestamp_utc: str
    fund_name: str
    as_of: str
    nav: str | None
    returns: dict[int, str]  # period -> "X,XX%"


def load_config() -> dict:
    cfg_path = "config.json" if os.path.exists("config.json") else "config.example.json"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)


def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def http_get(url: str, retries: int = 3, backoff: float = 1.7) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; uniqa-report-bot/1.0; +https://github.com/)",
        "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
        "Referer": url,
    }
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception:
            if attempt == retries:
                raise
            time.sleep(backoff ** attempt)
    raise RuntimeError("HTTP failed")


def parse_uniqa_fund_page(html: str, fund_name: str, periods: list[int]) -> FundSnapshot:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    # dd.mm.yyyy
    date_match = re.search(r"\b(\d{2}\.\d{2}\.\d{4})\b", text)
    as_of = date_match.group(1) if date_match else datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # NAV in PLN (heurystyka)
    nav = None
    nav_match = re.search(r"(\d{1,3}(?:[ \u00A0]\d{3})*,\d{2,4})\s*PLN", text)
    if nav_match:
        nav = nav_match.group(1).replace("\u00A0", " ").strip()

    # stopy zwrotu (heurystyka)
    returns = {}
    for p in periods:
        patterns = [
            rf"\b{p}\s*miesi[aą]c(?:e|y)?\b.*?([-+]?\d+,\d+)\s*%",
            rf"\b{p}\s*M\b.*?([-+]?\d+,\d+)\s*%",
        ]
        for pat in patterns:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m:
                returns[p] = m.group(1) + "%"
                break

    timestamp_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return FundSnapshot(timestamp_utc, fund_name, as_of, nav, returns)


def headers(periods: list[int]) -> list[str]:
    return ["timestamp_utc", "fund_name", "as_of", "nav"] + [f"ret_{p}m" for p in periods]


def append_history(history_path: str, snap: FundSnapshot, periods: list[int]):
    exists = os.path.exists(history_path)
    cols = headers(periods)

    row = {
        "timestamp_utc": snap.timestamp_utc,
        "fund_name": snap.fund_name,
        "as_of": snap.as_of,
        "nav": snap.nav,
    }
    for p in periods:
        row[f"ret_{p}m"] = snap.returns.get(p)

    with open(history_path, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        if not exists:
            w.writeheader()
        w.writerow(row)


def write_latest(latest_path: str, snaps: list[FundSnapshot], periods: list[int]):
    cols = headers(periods)
    with open(latest_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for s in snaps:
            row = {
                "timestamp_utc": s.timestamp_utc,
                "fund_name": s.fund_name,
                "as_of": s.as_of,
                "nav": s.nav,
            }
            for p in periods:
                row[f"ret_{p}m"] = s.returns.get(p)
            w.writerow(row)


def write_report(report_path: str, snaps: list[FundSnapshot], periods: list[int]):
    lines = []
    lines.append("UNIQA – snapshot danych (miesięczny)")
    lines.append(f"Wygenerowano (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    for s in snaps:
        lines.append(f"- {s.fund_name}")
        lines.append(f"  As of: {s.as_of}")
        lines.append(f"  NAV: {s.nav or 'brak'} PLN")
        for p in periods:
            lines.append(f"  {p}M: {s.returns.get(p, 'brak danych')}")
        lines.append("")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    ensure_dirs()
    cfg = load_config()
    periods = cfg.get("periods", [1, 3, 6, 12])

    snaps: list[FundSnapshot] = []
    for fund in cfg["funds"]:
        html = http_get(fund["url"])
        snap = parse_uniqa_fund_page(html, fund["name"], periods)
        snaps.append(snap)

        append_history(os.path.join(DATA_DIR, "history.csv"), snap, periods)

    write_latest(os.path.join(DATA_DIR, "latest.csv"), snaps, periods)
    write_report(os.path.join(OUTPUT_DIR, "report.txt"), snaps, periods)

    print("OK: data/history.csv updated; data/latest.csv written; output/report.txt created")


if __name__ == "__main__":
    main()
