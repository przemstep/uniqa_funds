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
    1 -> url, 3 -> url, 6 -> url, 12 -> url, 24 -> url

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
            if p in (1, 3, 6, 12, 24):
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


def _to_float_pl(x) -> float | None:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    # usuń spacje tysięcy i zamień przecinek na kropkę
    s = s.replace("\u00A0", " ").replace(" ", "").replace(",", ".")
    # usuń ewentualne "PLN"
    s = s.replace("PLN", "").strip()
    try:
        return float(s)
    except Exception:
        return None


def _clean_date(x) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    return s


def _extract_series(df: pd.DataFrame) -> tuple[list[str], list[float]]:
    """
    Oczekujemy pliku jak na Twoim screenie:
    - wiersz 2: 'Data:' / 'Wartość:' (nagłówki)
    - od wiersza 3: data i wartość
    Czasem pandas zaczyta nagłówki inaczej, więc robimy robust:
    - szukamy kolumn zawierających 'Data' i 'Warto'
    - jeśli nie ma, bierzemy pierwsze 2 kolumny
    """
    # normalizacja nazw kolumn
    cols = [str(c).strip() for c in df.columns]
    df2 = df.copy()
    df2.columns = cols

    date_col = None
    val_col = None
    for c in cols:
        lc = c.lower()
        if date_col is None and "data" in lc:
            date_col = c
        if val_col is None and ("wart" in lc or "value" in lc):
            val_col = c

    if date_col is None or val_col is None:
        # fallback: pierwsze dwie kolumny
        if len(cols) >= 2:
            date_col = cols[0]
            val_col = cols[1]
        else:
            return [], []

    # usuń wiersze bez wartości
    dates = []
    vals = []
    for _, row in df2.iterrows():
        d = _clean_date(row.get(date_col))
        v = _to_float_pl(row.get(val_col))
        # ignoruj wiersze nagłówkowe typu "Data:" / "Wartość:"
        if d and d.lower().startswith("data"):
            continue
        if v is None:
            continue
        if d is None:
            continue
        dates.append(d)
        vals.append(v)

    return dates, vals


def normalize_snapshot_from_dfs(fund_name: str, dfs_by_period: dict[int, pd.DataFrame]) -> FundSnapshot:
    """
    Z każdego pliku periodowego liczymy stopę zwrotu z serii:
    return = (last/first - 1) * 100
    NAV i as_of bierzemy z najkrótszego okresu, który istnieje (preferuj 1M).
    """
    returns: dict[int, str] = {}
    nav = None
    as_of = None

    # preferuj 1M do NAV, a jak go nie ma to 3M,6M,12M, 24M
    nav_preference = [1, 3, 6, 12, 24]

    for p, df in dfs_by_period.items():
        # zredukuj puste kolumny
        df = df.dropna(axis=1, how="all")
        dates, vals = _extract_series(df)

        if len(vals) >= 2:
            first = vals[0]
            last = vals[-1]
            ret = (last / first - 1.0) * 100.0
            # format PL z przecinkiem i 2 miejscami
            ret_str = f"{ret:.2f}".replace(".", ",") + "%"
            returns[p] = ret_str

        # ustaw NAV/as_of z preferowanego okresu
        if (nav is None or as_of is None) and p in nav_preference:
            if len(vals) >= 1:
                nav = f"{vals[-1]:.2f}".replace(".", ",")  # bez PLN w polu
                as_of = dates[-1]

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

import json

def validate_fund(
    fund_name: str,
    download_links: dict[int, str],
    dfs_by_period: dict[int, "pd.DataFrame"],
    series_by_period: dict[int, list[tuple["datetime", float]]],
    computed_returns: dict[int, str],
    periods_expected: list[int],
) -> dict:
    """
    Zwraca strukturę walidacji dla pojedynczego funduszu.
    """
    missing_links = [p for p in periods_expected if p not in download_links]
    missing_files = [p for p in periods_expected if p not in dfs_by_period]
    empty_series = [p for p in periods_expected if p in dfs_by_period and len(series_by_period.get(p, [])) < 2]
    missing_returns = [p for p in periods_expected if p not in computed_returns]

    # Dodatkowe info diagnostyczne: zakres dat serii bazowej (preferuj 24, potem 12, 6...)
    base_period = None
    base_range = None
    for p in [24, 12, 6, 3, 1]:
        s = series_by_period.get(p)
        if s and len(s) >= 2:
            base_period = p
            base_range = {
                "start": s[0][0].strftime("%Y-%m-%d"),
                "end": s[-1][0].strftime("%Y-%m-%d"),
                "points": len(s),
            }
            break

    ok = (len(missing_links) == 0) and (len(missing_files) == 0) and (len(empty_series) == 0)

    return {
        "fund": fund_name,
        "ok": ok,
        "missing_links": missing_links,
        "missing_files": missing_files,
        "empty_series": empty_series,
        "missing_returns": missing_returns,
        "base_period_used": base_period,
        "base_series_range": base_range,
    }


def write_validation_reports(path_txt: str, path_json: str, results: list[dict]):
    summary = {
        "funds_total": len(results),
        "funds_ok": sum(1 for r in results if r.get("ok")),
        "funds_with_issues": sum(1 for r in results if not r.get("ok")),
        "issues": results,
    }

    # JSON (do automatycznej analizy)
    with open(path_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # TXT (czytelny dla człowieka)
    lines = []
    lines.append("WALIDACJA – pobranie i obliczenia (UNIQA)")
    lines.append("")
    lines.append(f"Fundusze łącznie: {summary['funds_total']}")
    lines.append(f"OK: {summary['funds_ok']}")
    lines.append(f"Z problemami: {summary['funds_with_issues']}")
    lines.append("")

    for r in results:
        status = "OK" if r["ok"] else "PROBLEM"
        lines.append(f"- {r['fund']} [{status}]")
        if r.get("base_period_used"):
            br = r.get("base_series_range") or {}
            lines.append(f"  Seria bazowa: {r['base_period_used']}M | {br.get('start')} → {br.get('end')} | punktów: {br.get('points')}")
        if r["missing_links"]:
            lines.append(f"  Brak linków 'Pobierz' dla okresów: {r['missing_links']}")
        if r["missing_files"]:
            lines.append(f"  Nie pobrano plików dla okresów: {r['missing_files']}")
        if r["empty_series"]:
            lines.append(f"  Pusta/za krótka seria (Data/Wartość) dla okresów: {r['empty_series']}")
        if r["missing_returns"]:
            lines.append(f"  Nie policzono trailing returns dla: {r['missing_returns']}")
        lines.append("")

    with open(path_txt, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    ensure_dirs()
    cfg = load_config()
    periods = cfg.get("periods", [1, 3, 6, 12, 24])

    session = requests.Session()

    snaps: list[FundSnapshot] = []
    validation_results: list[dict] = []

    for fund in cfg["funds"]:
        page = http_get(fund["url"], session=session)
        download_links = find_download_links(page.text, base_url=fund["url"])
        
        dfs_by_period: dict[int, pd.DataFrame] = {}
        series_by_period: dict[int, list[tuple[datetime, float]]] = {}
        
        for p in periods:
            url = download_links.get(p)
            if not url:
                continue
                
            r = http_get(url, session=session)
            df = parse_download_file(r.content, r.headers.get("Content-Type", ""))
            dfs_by_period[p] = df

            # seria do walidacji (i później do obliczeń rolling)
            try:
                series_by_period[p] = extract_nav_series(df)
            except Exception:
                series_by_period[p] = []        

        snap = normalize_snapshot_from_dfs(fund["name"], dfs_by_period)
        snaps.append(snap)

        # WALIDACJA per fundusz
        validation_results.append(
            validate_fund(
                fund_name=fund["name"],
                download_links=download_links,
                dfs_by_period=dfs_by_period,
                series_by_period=series_by_period,
                computed_returns=snap.returns,
                periods_expected=periods,
            )
        )

        append_history(os.path.join(DATA_DIR, "history.csv"), snap, periods)

    write_latest(os.path.join(DATA_DIR, "latest.csv"), snaps, periods)
    write_report(os.path.join(OUTPUT_DIR, "report.txt"), snaps, periods)

    # Zapis walidacji
    write_validation_reports(
        path_txt=os.path.join(OUTPUT_DIR, "validation.txt"),
        path_json=os.path.join(OUTPUT_DIR, "validation.json"),
        results=validation_results,
    )
    
    # print("OK: download-first pipeline completed")


if __name__ == "__main__":
    main()
