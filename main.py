import os
import re
import csv
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO

import requests
from bs4 import BeautifulSoup

import pandas as pd


DATA_DIR = "data"
OUTPUT_DIR = "output"


@dataclass
class FundSnapshot:
    timestamp_utc: str
    fund_name: str
    as_of: str | None
    nav: str | None
    returns: dict[int, str]  # period -> "X,XX%"


def load_config() -> dict:
    cfg_path = "config.json" if os.path.exists("config.json") else "config.example.json"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)


def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def http_get(url: str, session: requests.Session, retries: int = 3, backoff: float = 1.7) -> requests.Response:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; uniqa-report-bot/1.0; +https://github.com/)",
        "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
        "Referer": url,
    }
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            return r
        except Exception:
            if attempt == retries:
                raise
            time.sleep(backoff ** attempt)
    raise RuntimeError("HTTP failed")


def find_download_links(html: str, base_url: str) -> dict[int, str]:
    """
    Szuka linków 'Pobierz' dla okresów i mapuje:
    1 -> url, 3 -> url, 6 -> url, 12 -> url

    Na stronie UNIQA linki mają parametry typu:
    fundId=..&fundType=tfi&period=1&type=700002&currency=PLN
    """
    soup = BeautifulSoup(html, "lxml")
    links = soup.find_all("a", href=True)

    out: dict[int, str] = {}
    for a in links:
        href = a["href"]
        if "fundId=" in href and "fundType=tfi" in href and "period=" in href:
            m = re.search(r"[?&]period=(\d+)", href)
            if not m:
                continue
            p = int(m.group(1))
            if p in (1, 3, 6, 12):
                # href bywa względny
                if href.startswith("/"):
                    out[p] = "https://www.uniqa.pl" + href
                elif href.startswith("http"):
                    out[p] = href
                else:
                    out[p] = base_url.rstrip("/") + "/" + href.lstrip("/")
    return out


def parse_download_file(content: bytes, content_type: str) -> pd.DataFrame:
    """
    Uniwersalny parser: jeśli CSV -> read_csv, jeśli XLS/XLSX -> read_excel.
    """
    ct = (content_type or "").lower()
    if "text/csv" in ct or "application/csv" in ct or content[:50].decode("utf-8", errors="ignore").count(",") > 0:
        # CSV: UNIQA może mieć separator ; lub ,
        for sep in [";", ",", "\t"]:
            try:
                df = pd.read_csv(BytesIO(content), sep=sep, engine="python")
                if df.shape[1] > 1:
                    return df
            except Exception:
                continue
        # fallback
        return pd.read_csv(BytesIO(content), engine="python")
    else:
        # Excel
        return pd.read_excel(BytesIO(content), engine="openpyxl")


def normalize_snapshot_from_dfs(fund_name: str, dfs_by_period: dict[int, pd.DataFrame]) -> FundSnapshot:
    """
    Minimalna normalizacja:
    - Dla każdego period mamy tabelę z wartościami (zależnie od formatu UNIQA).
    - Wyciągamy stopy zwrotu, a datę/NAV bierzemy z najbardziej informacyjnego pliku (jeśli jest).
    """
    returns: dict[int, str] = {}
    as_of = None
    nav = None

    # heurystyki nazw kolumn (UNIQA może zmieniać)
    date_cols = ["Data", "Date", "Aktualizacja", "data"]
    nav_cols = ["Wartość jednostki", "Wartosc jednostki", "NAV", "Wartość", "Wartosc"]

    for p, df in dfs_by_period.items():
        # zredukuj puste kolumny
        df = df.dropna(axis=1, how="all")

        # Szukamy w df wartości procentowej; często jest kolumna "Stopa zwrotu" albo podobna
        # Najpierw spróbuj znaleźć kolumnę zawierającą słowo "zwrot"
        ret_val = None
        for col in df.columns:
            col_str = str(col).lower()
            if "zwrot" in col_str or "return" in col_str or "%”" in col_str or "%" in col_str:
                # weź pierwszą niepustą wartość liczbową
                series = df[col].dropna()
                if not series.empty:
                    ret_val = str(series.iloc[0]).strip()
                    break

        # Jeżeli nie znaleźliśmy, spróbuj poszukać liczby w całej tabeli (pierwsza sensowna)
        if ret_val is None:
            flat = df.astype(str).stack().tolist()
            for cell in flat:
                s = str(cell).strip()
                if re.fullmatch(r"[-+]?\d+([.,]\d+)?%?", s):
                    ret_val = s
                    break

        if ret_val:
            ret_val = ret_val.replace(" ", "")
            if not ret_val.endswith("%"):
                ret_val = ret_val + "%"
            returns[p] = ret_val

        # Ustal as_of i nav z pierwszego df, w którym to znajdziemy
        if as_of is None:
            for col in df.columns:
                if str(col) in date_cols or str(col).lower() in [c.lower() for c in date_cols]:
                    series = df[col].dropna()
                    if not series.empty:
                        as_of = str(series.iloc[0]).strip()
                        break

        if nav is None:
            for col in df.columns:
                if str(col) in nav_cols or str(col).lower() in [c.lower() for c in nav_cols]:
                    series = df[col].dropna()
                    if not series.empty:
                        nav = str(series.iloc[0]).strip()
                        nav = nav.replace("PLN", "").strip()
                        break

    timestamp_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return FundSnapshot(timestamp_utc=timestamp_utc, fund_name=fund_name, as_of=as_of, nav=nav, returns=returns)


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
            row = {"timestamp_utc": s.timestamp_utc, "fund_name": s.fund_name, "as_of": s.as_of, "nav": s.nav}
            for p in periods:
                row[f"ret_{p}m"] = s.returns.get(p)
            w.writerow(row)


def write_report(report_path: str, snaps: list[FundSnapshot], periods: list[int]):
    lines = []
    lines.append("UNIQA – snapshot danych (miesięczny) [download-first]")
    lines.append(f"Wygenerowano (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    for s in snaps:
        lines.append(f"- {s.fund_name}")
        lines.append(f"  As of: {s.as_of or 'brak'}")
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

    session = requests.Session()

    snaps: list[FundSnapshot] = []

    for fund in cfg["funds"]:
        page = http_get(fund["url"], session=session)
        download_links = find_download_links(page.text, base_url=fund["url"])

        dfs_by_period: dict[int, pd.DataFrame] = {}
        for p in periods:
            url = download_links.get(p)
            if not url:
                continue
            r = http_get(url, session=session)
            df = parse_download_file(r.content, r.headers.get("Content-Type", ""))
            dfs_by_period[p] = df

        snap = normalize_snapshot_from_dfs(fund["name"], dfs_by_period)
        snaps.append(snap)

        append_history(os.path.join(DATA_DIR, "history.csv"), snap, periods)

    write_latest(os.path.join(DATA_DIR, "latest.csv"), snaps, periods)
    write_report(os.path.join(OUTPUT_DIR, "report.txt"), snaps, periods)

    print("OK: download-first pipeline completed")


if __name__ == "__main__":
    main()
