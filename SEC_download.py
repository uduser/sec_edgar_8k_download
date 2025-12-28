"""
SEC EDGAR Form 8-K downloader

需求摘要（對應使用者描述）：
- 依公司清單（CIK）查詢該公司所有 filings
- 篩選 Form 8-K（時間 All；可選含 8-K/A）
- 下載每筆 8-K 的主文件與附件（同一 accession 目錄下全部檔案）

資料來源（SEC 官方）：
- 公司提交資料總表（較新資料；部分公司可能不含很早期 filings）：https://data.sec.gov/submissions/CIK##########.json
- 公司 filings 列表（可回溯較早期）：https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=##########&type=8-K
- Filing 目錄索引：https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodashes}/index.json
"""

from __future__ import annotations

import argparse
import datetime as _dt
import gzip
import hashlib
import io
import json
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Optional

import requests


SEC_DATA_BASE = "https://data.sec.gov"
SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives"
SEC_BROWSE_BASE = "https://www.sec.gov/cgi-bin/browse-edgar"


def normalize_cik(cik: str) -> str:
    digits = re.sub(r"\D", "", cik or "")
    if not digits:
        raise ValueError(f"Invalid CIK: {cik!r}")
    if len(digits) > 10:
        # Some inputs might include extra leading zeros or junk; keep last 10 if clearly too long
        digits = digits[-10:]
    return digits.zfill(10)


def cik_to_int_str(cik10: str) -> str:
    # Archives URL uses CIK without leading zeros as directory name
    return str(int(cik10))


def accession_no_nodashes(accession_no: str) -> str:
    return accession_no.replace("-", "")


def safe_filename(name: str) -> str:
    # SEC filenames are generally safe; still guard against path traversal
    name = name.replace("\\", "/")
    name = name.split("/")[-1]
    # EDGAR index pages sometimes render as "filename.htm iXBRL" in the Document column.
    # Real filenames do not contain whitespace; keep the first token.
    name = (name or "").strip().split()[0] if (name or "").strip() else ""
    return name


class RateLimiter:
    """Simple global min-interval rate limiter shared across threads."""

    def __init__(self, min_interval_sec: float):
        self.min_interval_sec = float(min_interval_sec)
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        if self.min_interval_sec <= 0:
            return
        with self._lock:
            now = time.monotonic()
            sleep_for = self.min_interval_sec - (now - self._last)
            if sleep_for > 0:
                time.sleep(sleep_for)
            self._last = time.monotonic()


@dataclass(frozen=True)
class FilingRef:
    cik10: str
    accession_no: str  # with dashes
    filing_date: str
    form: str
    primary_document: str

    @property
    def accession_dir(self) -> str:
        return accession_no_nodashes(self.accession_no)


def build_session(user_agent: str) -> requests.Session:
    if not user_agent or "@" not in user_agent:
        raise ValueError(
            "SEC 要求提供可聯絡的 User-Agent（建議含 email）。例如："
            ' --user-agent "Your Name your.email@example.com"'
        )
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.8",
            "Connection": "keep-alive",
        }
    )
    return s


def sec_get_json(
    session: requests.Session,
    url: str,
    *,
    rate_limiter: RateLimiter,
    max_retries: int = 10,
    timeout_sec: float = 60.0,
) -> dict:
    backoff = 1.0
    for attempt in range(max_retries):
        rate_limiter.wait()
        try:
            resp = session.get(url, timeout=(10.0, timeout_sec))
        except requests.exceptions.RequestException:
            # Network hiccup / timeout: backoff and retry
            time.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60.0)
            continue
        if resp.status_code in (403, 429, 500, 502, 503, 504):
            # Respect SEC throttling; exponential backoff.
            time.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60.0)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Failed to fetch JSON after retries: {url}")

def sec_get_text(
    session: requests.Session,
    url: str,
    *,
    rate_limiter: RateLimiter,
    max_retries: int = 10,
    timeout_sec: float = 60.0,
) -> str:
    backoff = 1.0
    for attempt in range(max_retries):
        rate_limiter.wait()
        try:
            resp = session.get(url, timeout=(10.0, timeout_sec))
        except requests.exceptions.RequestException:
            time.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60.0)
            continue
        if resp.status_code in (403, 429, 500, 502, 503, 504):
            time.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60.0)
            continue
        resp.raise_for_status()
        resp.encoding = resp.encoding or "utf-8"
        return resp.text
    raise RuntimeError(f"Failed to fetch text after retries: {url}")


def sec_get_bytes(
    session: requests.Session,
    url: str,
    *,
    rate_limiter: RateLimiter,
    max_retries: int = 10,
    timeout_sec: float = 120.0,
) -> bytes:
    """
    Fetch raw bytes with retries/backoff. Useful for large index files and .gz.
    """
    backoff = 1.0
    for attempt in range(max_retries):
        rate_limiter.wait()
        try:
            resp = session.get(url, timeout=(10.0, timeout_sec))
        except requests.exceptions.RequestException:
            time.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60.0)
            continue
        if resp.status_code in (403, 429, 500, 502, 503, 504):
            time.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60.0)
            continue
        resp.raise_for_status()
        return resp.content
    raise RuntimeError(f"Failed to fetch bytes after retries: {url}")


def sec_download_file(
    session: requests.Session,
    url: str,
    target_path: Path,
    *,
    rate_limiter: RateLimiter,
    max_retries: int = 10,
    timeout_sec: float = 120.0,
) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and target_path.stat().st_size > 0:
        return

    backoff = 1.0
    for attempt in range(max_retries):
        rate_limiter.wait()
        try:
            resp = session.get(url, timeout=(10.0, timeout_sec), stream=True)
        except requests.exceptions.RequestException:
            time.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60.0)
            continue
        if resp.status_code in (403, 429, 500, 502, 503, 504):
            time.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60.0)
            continue
        # Some older accession directories have index listings that reference missing files.
        # Treat 404 as a skip so one missing file doesn't fail the whole filing.
        if resp.status_code == 404:
            return
        resp.raise_for_status()

        tmp_path = target_path.with_suffix(target_path.suffix + ".part")
        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
        os.replace(tmp_path, target_path)
        return

    raise RuntimeError(f"Failed to download after retries: {url}")


def iter_ciks_from_file(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    # allow comma/space/newline separated
    toks = re.split(r"[\s,]+", text.strip())
    return [t for t in toks if t]


def _parse_date_yyyy_mm_dd(s: str) -> _dt.date:
    """
    Accept 'YYYY-MM-DD' or 'YYYY/MM/DD' and return date.
    """
    s = (s or "").strip()
    if not s:
        raise ValueError("Empty date")
    s = s.replace("/", "-")
    try:
        return _dt.date.fromisoformat(s)
    except Exception as e:
        raise ValueError(f"Invalid date (expected YYYY-MM-DD): {s!r}") from e


def _try_parse_date_yyyy_mm_dd(s: str) -> _dt.date | None:
    try:
        return _parse_date_yyyy_mm_dd(s)
    except Exception:
        return None


def _current_year_quarter(today: _dt.date | None = None) -> tuple[int, int]:
    d = today or _dt.date.today()
    q = (d.month - 1) // 3 + 1
    return d.year, int(q)


def _iter_year_quarters(start_year: int, end_year: int, end_quarter: int) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for y in range(int(start_year), int(end_year) + 1):
        for q in (1, 2, 3, 4):
            if y == end_year and q > int(end_quarter):
                break
            out.append((y, q))
    return out


_MASTER_IDX_ACC_RE = re.compile(r"\b(\d{10}-\d{2}-\d{6})\.txt\b", re.IGNORECASE)


def write_targets_manifest(path: Path, targets: list[FilingRef]) -> None:
    """
    Write FilingRef list as JSON Lines for resume/sharding.
    """
    with open(path, "w", encoding="utf-8") as f:
        for t in targets:
            f.write(
                json.dumps(
                    {
                        "cik10": t.cik10,
                        "accession_no": t.accession_no,
                        "filing_date": t.filing_date,
                        "form": t.form,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


def read_targets_manifest(path: Path) -> list[FilingRef]:
    out: list[FilingRef] = []
    text = path.read_text(encoding="utf-8", errors="ignore")
    for ln in text.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            obj = json.loads(ln)
        except Exception:
            continue
        cik10 = normalize_cik(str(obj.get("cik10") or ""))
        accession_no = str(obj.get("accession_no") or "")
        filing_date = str(obj.get("filing_date") or "unknown-date")
        form = str(obj.get("form") or "8-K")
        if not accession_no:
            continue
        out.append(
            FilingRef(
                cik10=cik10,
                accession_no=accession_no,
                filing_date=filing_date,
                form=form,
                primary_document="",
            )
        )
    out.sort(key=lambda x: (x.filing_date, x.accession_no))
    return out


def collect_8k_from_master_index(
    session: requests.Session,
    *,
    cik_filter: set[str] | None,
    start_year: int = 2001,
    start_date: str | None,
    include_amendments: bool,
    rate_limiter: RateLimiter,
) -> list[FilingRef]:
    """
    Use SEC quarterly master index to list filings in bulk (\"season\"/quarterly mode).

    - Source: https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/master.idx
      (fallback to master.gz)
    - Filter: 8-K and optionally 8-K/A
    - If cik_filter is provided, only keep those CIKs (10-digit form).

    Returns FilingRef list (primary_document empty; download uses filing index HTML).
    """
    start_dt = _parse_date_yyyy_mm_dd(start_date) if start_date else None
    end_year, end_q = _current_year_quarter()
    quarters = _iter_year_quarters(int(start_year), int(end_year), int(end_q))

    wanted_forms = {"8-K"}
    if include_amendments:
        wanted_forms.add("8-K/A")

    out: list[FilingRef] = []
    for year, qtr in quarters:
        base = f"{SEC_ARCHIVES_BASE}/edgar/full-index/{year}/QTR{qtr}"
        # Try plain master.idx first; fallback to master.gz (some environments prefer gz).
        data: bytes | None = None
        for name in ("master.idx", "master.gz"):
            url = f"{base}/{name}"
            try:
                data = sec_get_bytes(session, url, rate_limiter=rate_limiter)
                if name.endswith(".gz"):
                    data = gzip.decompress(data)
                break
            except Exception:
                data = None
                continue
        if not data:
            continue

        # master.idx is ASCII; decode with replacement to be robust
        text = data.decode("latin-1", errors="replace")
        lines = text.splitlines()

        # Skip header until the pipe header line
        start_i = 0
        for i, ln in enumerate(lines[:200]):
            if ln.strip().upper().startswith("CIK|COMPANY NAME|FORM TYPE|DATE FILED|FILENAME"):
                start_i = i + 1
                break

        for ln in lines[start_i:]:
            if not ln or "|" not in ln:
                continue
            parts = ln.split("|")
            if len(parts) < 5:
                continue
            cik_raw, _company, form, date_filed, filename = parts[0].strip(), parts[1], parts[2].strip(), parts[3].strip(), parts[4].strip()
            if form not in wanted_forms:
                continue

            dt = _try_parse_date_yyyy_mm_dd(date_filed)
            if not dt:
                continue
            if start_dt and dt < start_dt:
                continue

            try:
                cik10 = normalize_cik(cik_raw)
            except Exception:
                continue
            if cik_filter and cik10 not in cik_filter:
                continue

            m = _MASTER_IDX_ACC_RE.search(filename)
            if not m:
                continue
            accession_no = m.group(1)

            out.append(
                FilingRef(
                    cik10=cik10,
                    accession_no=accession_no,
                    filing_date=date_filed,
                    form=form,
                    primary_document="",
                )
            )

    # de-dup by accession (master index can contain duplicates across corrections)
    seen: set[str] = set()
    deduped: list[FilingRef] = []
    for r in out:
        if r.accession_no in seen:
            continue
        seen.add(r.accession_no)
        deduped.append(r)
    deduped.sort(key=lambda x: (x.filing_date, x.accession_no))
    return deduped


def run_download(
    *,
    ciks: list[str],
    out: str | Path = "downloads",
    user_agent: str,
    include_amendments: bool = False,
    min_interval: float = 0.2,
    max_workers: int = 3,
    save_manifest: bool = False,
    download_mode: str = "all",
    start_date: str | None = None,
    source_mode: str = "cik",
    master_start_year: int = 2001,
    shard: str | None = None,
    targets_manifest: str | Path | None = None,
    reuse_targets_manifest: bool = False,
    manifest_only: bool = False,
    log: Optional[Callable[[str], None]] = None,
) -> dict:
    """
    Run the full pipeline:
    - fetch all filings for each CIK
    - filter 8-K (and optionally 8-K/A)
    - download all files in each filing's accession directory

    Returns a summary dict.
    """

    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(msg)

    ciks10 = [normalize_cik(c) for c in (ciks or [])]
    if not ciks10:
        raise ValueError("No CIKs provided.")

    if download_mode not in ("8k_ex", "primary_ex_htm", "all"):
        raise ValueError(f"Unknown download_mode: {download_mode}")
    if source_mode not in ("cik", "master_index"):
        raise ValueError(f"Unknown source_mode: {source_mode}")

    out_dir = Path(out)
    session = build_session(user_agent)
    rate_limiter = RateLimiter(min_interval)

    total_targets = 0
    ok = 0
    failed = 0

    # ---- master index mode (season/quarter batch) ----
    if source_mode == "master_index":
        manifest_path: Path
        if targets_manifest:
            manifest_path = Path(targets_manifest)
        else:
            sd = (start_date or "all").replace("/", "-")
            shard_tag = (shard or "all").replace("/", "_")
            manifest_path = out_dir / f"master_index_8k_targets_{sd}_{shard_tag}.jsonl"

        if reuse_targets_manifest and manifest_path.exists():
            _log(f"MASTER_INDEX mode: loading targets from manifest {manifest_path} ...")
            targets = read_targets_manifest(manifest_path)
        else:
            _log("MASTER_INDEX mode: building 8-K targets from quarterly index ...")
            targets = collect_8k_from_master_index(
                session,
                cik_filter=set(ciks10),
                start_year=int(master_start_year),
                start_date=start_date,
                include_amendments=include_amendments,
                rate_limiter=rate_limiter,
            )
        # Optional sharding for multi-machine runs: keep only a slice of accessions.
        if shard:
            m = re.match(r"^\s*(\d+)\s*/\s*(\d+)\s*$", shard)
            if not m:
                raise ValueError("Invalid --shard format. Use N/K, e.g. 1/3")
            n = int(m.group(1))
            k = int(m.group(2))
            if k <= 0 or n <= 0 or n > k:
                raise ValueError("Invalid --shard values. Must satisfy 1 <= N <= K and K > 0")
            before = len(targets)
            targets = [
                t
                for t in targets
                if (int(hashlib.sha1(t.accession_no.encode("utf-8")).hexdigest(), 16) % k) == (n - 1)
            ]
            _log(f"MASTER_INDEX shard={n}/{k} targets={len(targets)}/{before}")
        # Always write manifest for resume/debugging
        try:
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            write_targets_manifest(manifest_path, targets)
            _log(f"MASTER_INDEX wrote manifest: {manifest_path}")
        except Exception as e:
            _log(f"MASTER_INDEX manifest write failed: {e}")

        total_targets = len(targets)
        _log(f"MASTER_INDEX targets={total_targets}")

        if manifest_only:
            _log("MASTER_INDEX manifest-only: skip downloads.")
            return {
                "ok": 0,
                "failed": 0,
                "total": total_targets,
                "companies_total": len(ciks10),
                "companies_done": len(ciks10),
                "companies_failed": 0,
                "out": str(out_dir.resolve()),
            }

        if targets:
            _log(f"Downloading {len(targets)} filings ...")
            with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as ex:
                futs = [
                    ex.submit(
                        download_filing,
                        session,
                        filing,
                        out_dir,
                        rate_limiter=rate_limiter,
                        save_manifest=bool(save_manifest),
                        download_mode=download_mode,
                    )
                    for filing in targets
                ]
                for fut in as_completed(futs):
                    try:
                        filing, count = fut.result()
                        ok += 1
                        _log(f"OK  {filing.cik10} {filing.filing_date} {filing.accession_no} files={count}")
                    except Exception as e:
                        failed += 1
                        _log(f"FAIL {e}")
        else:
            _log("No targets found. Done.")

        _log(f"Done. ok={ok} failed={failed} targets={total_targets} out={out_dir.resolve()}")
        return {
            "ok": ok,
            "failed": failed,
            "total": total_targets,
            "companies_total": len(ciks10),
            "companies_done": len(ciks10),
            "companies_failed": 0,
            "out": str(out_dir.resolve()),
        }

    # ---- per-company mode (existing) ----
    total_companies = len(ciks10)
    companies_done = 0
    companies_failed = 0

    for idx, cik10 in enumerate(ciks10, start=1):
        _log(f"[{idx}/{total_companies}] {cik10} SCAN start")
        # Speed strategy for large CIK sets:
        # - Prefer submissions JSON when it already covers the requested start_date (typically fewer requests).
        # - Fall back to browse-edgar when submissions appears too shallow (some issuers miss older years there).
        if start_date:
            start_dt = _parse_date_yyyy_mm_dd(start_date)
            filings = collect_all_filings_for_cik(session, cik10, rate_limiter=rate_limiter)
            sub_targets = filter_8k_filings(cik10, filings, include_amendments=include_amendments)
            sub_targets = [t for t in sub_targets if (_try_parse_date_yyyy_mm_dd(t.filing_date) or _dt.date.max) >= start_dt]
            sub_earliest = min(
                (d for d in (_try_parse_date_yyyy_mm_dd(t.filing_date) for t in sub_targets) if d),
                default=None,
            )
            if sub_earliest and sub_earliest <= start_dt:
                targets = sub_targets
                _log(f"[{idx}/{total_companies}] {cik10} source=submissions earliest={sub_earliest.isoformat()}")
            else:
                targets = collect_8k_targets_for_cik(
                    session,
                    cik10,
                    include_amendments=include_amendments,
                    rate_limiter=rate_limiter,
                    start_date=start_date,
                )
                _log(f"[{idx}/{total_companies}] {cik10} source=browse-edgar")
        else:
            # Full history mode
            targets = collect_8k_targets_for_cik(
                session,
                cik10,
                include_amendments=include_amendments,
                rate_limiter=rate_limiter,
                start_date=None,
            )
        total_targets += len(targets)
        _log(f"[{idx}/{total_companies}] {cik10} 8-K targets={len(targets)}")

        cik_ok = 0
        cik_failed = 0

        if targets:
            _log(f"[{idx}/{total_companies}] {cik10} Downloading {len(targets)} filings ...")
            with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as ex:
                futs = [
                    ex.submit(
                        download_filing,
                        session,
                        filing,
                        out_dir,
                        rate_limiter=rate_limiter,
                        save_manifest=bool(save_manifest),
                        download_mode=download_mode,
                    )
                    for filing in targets
                ]
                for fut in as_completed(futs):
                    try:
                        filing, count = fut.result()
                        ok += 1
                        cik_ok += 1
                        _log(f"OK  {filing.cik10} {filing.filing_date} {filing.accession_no} files={count}")
                    except Exception as e:
                        failed += 1
                        cik_failed += 1
                        _log(f"FAIL {e}")
        else:
            _log(f"[{idx}/{total_companies}] {cik10} no 8-K targets, skip download")

        companies_done += 1
        if cik_failed > 0:
            companies_failed += 1

        _log(
            f"[{idx}/{total_companies}] {cik10} COMPANY_DONE ok={cik_ok} failed={cik_failed} "
            f"companies_done={companies_done} companies_left={total_companies - companies_done}"
        )

    _log(f"Done. ok={ok} failed={failed} companies={total_companies} out={out_dir.resolve()}")
    return {
        "ok": ok,
        "failed": failed,
        "total": total_targets,
        "companies_total": total_companies,
        "companies_done": companies_done,
        "companies_failed": companies_failed,
        "out": str(out_dir.resolve()),
    }


def collect_all_filings_for_cik(
    session: requests.Session,
    cik10: str,
    *,
    rate_limiter: RateLimiter,
) -> list[dict]:
    """Return list of filing dicts merged across recent + older 'files' pages."""
    url = f"{SEC_DATA_BASE}/submissions/CIK{cik10}.json"
    root = sec_get_json(session, url, rate_limiter=rate_limiter)

    filings: list[dict] = []

    recent = (root.get("filings") or {}).get("recent") or {}
    # recent is columnar arrays
    forms = recent.get("form") or []
    accession_numbers = recent.get("accessionNumber") or []
    filing_dates = recent.get("filingDate") or []
    primary_docs = recent.get("primaryDocument") or []

    n = min(len(forms), len(accession_numbers), len(filing_dates), len(primary_docs))
    for i in range(n):
        filings.append(
            {
                "form": forms[i],
                "accessionNumber": accession_numbers[i],
                "filingDate": filing_dates[i],
                "primaryDocument": primary_docs[i],
            }
        )

    # Older filings are paged in filings.files; each 'name' is a JSON under /submissions/
    files = (root.get("filings") or {}).get("files") or []
    for f in files:
        name = f.get("name")
        if not name:
            continue
        page_url = f"{SEC_DATA_BASE}/submissions/{name}"
        page = sec_get_json(session, page_url, rate_limiter=rate_limiter)
        page_recent = (page.get("filings") or {}).get("recent") or {}
        forms = page_recent.get("form") or []
        accession_numbers = page_recent.get("accessionNumber") or []
        filing_dates = page_recent.get("filingDate") or []
        primary_docs = page_recent.get("primaryDocument") or []
        n = min(len(forms), len(accession_numbers), len(filing_dates), len(primary_docs))
        for i in range(n):
            filings.append(
                {
                    "form": forms[i],
                    "accessionNumber": accession_numbers[i],
                    "filingDate": filing_dates[i],
                    "primaryDocument": primary_docs[i],
                }
            )

    return filings


def filter_8k_filings(
    cik10: str,
    filings: list[dict],
    *,
    include_amendments: bool,
) -> list[FilingRef]:
    out: list[FilingRef] = []
    for f in filings:
        form = (f.get("form") or "").strip()
        if not form:
            continue
        if form == "8-K" or (include_amendments and form == "8-K/A"):
            acc = f.get("accessionNumber")
            filing_date = f.get("filingDate") or "unknown-date"
            primary = f.get("primaryDocument") or ""
            if not acc:
                continue
            out.append(
                FilingRef(
                    cik10=cik10,
                    accession_no=acc,
                    filing_date=filing_date,
                    form=form,
                    primary_document=primary,
                )
            )
    # de-dup by accession
    seen: set[str] = set()
    deduped: list[FilingRef] = []
    for r in out:
        if r.accession_no in seen:
            continue
        seen.add(r.accession_no)
        deduped.append(r)
    # sort by date ascending for deterministic output
    deduped.sort(key=lambda x: (x.filing_date, x.accession_no))
    return deduped


def collect_8k_targets_for_cik(
    session: requests.Session,
    cik10: str,
    *,
    include_amendments: bool,
    rate_limiter: RateLimiter,
    page_size: int = 100,
    start_date: str | None = None,
) -> list[FilingRef]:
    """
    Collect all 8-K (and optionally 8-K/A) filings for a company across the full available history.

    We use SEC browse-edgar (getcompany) because some companies' submissions JSON may not include
    very old filings. Returned FilingRef may have empty primary_document; download modes that need
    the primary will fall back to the filing index HTML.
    """
    cik_int = cik_to_int_str(cik10)
    forms = ["8-K"]
    if include_amendments:
        forms.append("8-K/A")

    start_dt: _dt.date | None = _parse_date_yyyy_mm_dd(start_date) if start_date else None

    refs: list[FilingRef] = []
    for form_type in forms:
        start = 0
        while True:
            url = (
                f"{SEC_BROWSE_BASE}?action=getcompany&CIK={cik_int}"
                f"&type={form_type}&owner=exclude&count={int(page_size)}&start={int(start)}"
            )
            html = sec_get_text(session, url, rate_limiter=rate_limiter)
            parser = _CompanyFilingsParser()
            parser.feed(html)
            rows = parser.filings
            if not rows:
                break

            # rows are newest-first on browse-edgar pages; allow early stop when we pass start_dt
            oldest_on_page: _dt.date | None = None
            for form, filing_date, accession_no in rows:
                if start_dt:
                    try:
                        fd = _parse_date_yyyy_mm_dd(filing_date)
                    except Exception:
                        # If SEC changes formatting, don't drop the row silently
                        continue
                    if oldest_on_page is None or fd < oldest_on_page:
                        oldest_on_page = fd
                    if fd < start_dt:
                        continue
                refs.append(
                    FilingRef(
                        cik10=cik10,
                        accession_no=accession_no,
                        filing_date=filing_date,
                        form=form or form_type,
                        primary_document="",
                    )
                )

            if start_dt and oldest_on_page and oldest_on_page < start_dt and all(
                _parse_date_yyyy_mm_dd(r[1]) < start_dt for r in rows
            ):
                # Entire page is older than start_dt; subsequent pages will be even older.
                break
            if len(rows) < int(page_size):
                break
            start += int(page_size)

    # de-dup by accession
    seen: set[str] = set()
    deduped: list[FilingRef] = []
    for r in refs:
        if r.accession_no in seen:
            continue
        seen.add(r.accession_no)
        deduped.append(r)
    deduped.sort(key=lambda x: (x.filing_date, x.accession_no))
    return deduped


def get_filing_index_items(
    session: requests.Session,
    filing: FilingRef,
    *,
    rate_limiter: RateLimiter,
) -> list[dict]:
    cik_int = cik_to_int_str(filing.cik10)
    idx_url = f"{SEC_ARCHIVES_BASE}/edgar/data/{cik_int}/{filing.accession_dir}/index.json"
    idx = sec_get_json(session, idx_url, rate_limiter=rate_limiter)
    # structure: { directory: { item: [ ... ] } }
    items = ((idx.get("directory") or {}).get("item")) or []
    return list(items)


class _EdgarIndexParser(HTMLParser):
    """
    Parse EDGAR filing index page to extract (document_filename, type) rows.
    We use the 'Type' column to find EX-*.
    """

    def __init__(self) -> None:
        super().__init__()
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._cell_text = ""
        self._cells: list[str] = []
        self._row_has_th = False
        self._doc_col_idx: Optional[int] = None
        self._type_col_idx: Optional[int] = None
        self.rows: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag == "table":
            attr_map = {k: v for k, v in attrs}
            if (attr_map.get("class") or "").lower().find("tablefile") >= 0:
                self._in_table = True
        if self._in_table and tag == "tr":
            self._in_row = True
            self._cells = []
            self._row_has_th = False
        if self._in_row and tag in ("td", "th"):
            self._in_cell = True
            self._cell_text = ""
            if tag == "th":
                self._row_has_th = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "table" and self._in_table:
            self._in_table = False
        if tag == "tr" and self._in_row:
            self._in_row = False
            # Header row: detect column indices for 'Document' and 'Type'
            if self._row_has_th and self._cells:
                lower = [c.strip().lower() for c in self._cells]
                # Common layouts:
                # - Seq | Description | Document | Type | Size
                # - Document | Description | Type | Size
                if "document" in lower:
                    self._doc_col_idx = lower.index("document")
                if "type" in lower:
                    self._type_col_idx = lower.index("type")
                return

            # Data row
            if not self._cells:
                return
            doc_idx = self._doc_col_idx
            type_idx = self._type_col_idx
            if doc_idx is None or type_idx is None:
                # Fallback to the old assumption (best-effort)
            if len(self._cells) >= 3:
                doc = self._cells[0].strip()
                typ = self._cells[2].strip()
                else:
                    return
            else:
                if doc_idx >= len(self._cells) or type_idx >= len(self._cells):
                    return
                doc = self._cells[doc_idx].strip()
                typ = self._cells[type_idx].strip()

            if doc and doc.lower() != "document":
                    self.rows.append((doc, typ))
        if tag in ("td", "th") and self._in_cell:
            self._in_cell = False
            self._cells.append(re.sub(r"\s+", " ", self._cell_text).strip())

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_text += data


class _CompanyFilingsParser(HTMLParser):
    """
    Parse SEC browse-edgar company filings page (HTML) to extract (form, filing_date, accession_no).

    We look for the table with class 'tableFile2'. For each row, extract:
    - form type (e.g. 8-K / 8-K/A) from first cell text
    - filing date (YYYY-MM-DD) from any cell
    - accession number from a link that contains "{accession}-index.html"
    """

    _DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
    # browse-edgar uses "-index.htm" (and occasionally "-index.html" on some pages)
    _ACC_RE = re.compile(r"\b(\d{10}-\d{2}-\d{6})-index\.htm(l)?\b", re.IGNORECASE)

    def __init__(self) -> None:
        super().__init__()
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._cell_text = ""
        self._cells: list[str] = []
        self._hrefs: list[str] = []
        self.filings: list[tuple[str, str, str]] = []  # (form, filing_date, accession_no)

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag == "table":
            attr_map = {k: v for k, v in attrs}
            cls = (attr_map.get("class") or "").lower()
            if "tablefile2" in cls:
                self._in_table = True
        if self._in_table and tag == "tr":
            self._in_row = True
            self._cells = []
            self._hrefs = []
        if self._in_row and tag in ("td", "th"):
            self._in_cell = True
            self._cell_text = ""
        if self._in_cell and tag == "a":
            for k, v in attrs:
                if k == "href" and v:
                    self._hrefs.append(v)

    def handle_endtag(self, tag: str) -> None:
        if tag == "table" and self._in_table:
            self._in_table = False
        if tag == "tr" and self._in_row:
            self._in_row = False
            if not self._cells:
                return
            form = (self._cells[0] or "").strip()
            # header row often has 'Filings' in first cell
            if not form or form.lower() == "filings":
                return

            filing_date = ""
            for c in self._cells:
                m = self._DATE_RE.search(c or "")
                if m:
                    filing_date = m.group(1)
                    break

            accession_no = ""
            for h in self._hrefs:
                m = self._ACC_RE.search(h or "")
                if m:
                    accession_no = m.group(1)
                    break

            if form and filing_date and accession_no:
                self.filings.append((form, filing_date, accession_no))

        if tag in ("td", "th") and self._in_cell:
            self._in_cell = False
            self._cells.append(re.sub(r"\s+", " ", self._cell_text).strip())

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_text += data


def _choose_index_html_name(items: list[dict], accession_no: str) -> Optional[str]:
    candidates = {f"{accession_no}-index.html", f"{accession_no}-index.htm", "index.html", "index.htm"}
    names = [it.get("name") for it in items if it.get("name")]
    for c in candidates:
        if c in names:
            return c
    for n in names:
        if isinstance(n, str) and (n.endswith("-index.html") or n.endswith("-index.htm")):
            return n
    return None


def _list_primary_ex_htm_files(
    session: requests.Session,
    filing: FilingRef,
    *,
    rate_limiter: RateLimiter,
) -> list[str]:
    items = get_filing_index_items(session, filing, rate_limiter=rate_limiter)
    cik_int = cik_to_int_str(filing.cik10)
    base_url = f"{SEC_ARCHIVES_BASE}/edgar/data/{cik_int}/{filing.accession_dir}"

    index_name = _choose_index_html_name(items, filing.accession_no)
    if not index_name:
        raise RuntimeError(f"Cannot locate filing index HTML for accession {filing.accession_no}")

    html = sec_get_text(session, f"{base_url}/{index_name}", rate_limiter=rate_limiter)
    parser = _EdgarIndexParser()
    parser.feed(html)

    wanted: list[str] = []
    primary = safe_filename(filing.primary_document)
    if primary and primary.lower().endswith(".htm"):
        wanted.append(primary)
    else:
        # Older filings may not have primaryDocument from submissions JSON.
        # Fall back to the filing index table row whose Type is 8-K/8-K/A, and keep only .htm.
        for doc, typ in parser.rows:
            doc = safe_filename(doc)
            t = (typ or "").strip().upper()
            if t in ("8-K", "8-K/A") and doc.lower().endswith(".htm"):
                wanted.append(doc)
                break

    for doc, typ in parser.rows:
        doc = safe_filename(doc)
        if not doc.lower().endswith(".htm"):
            continue
        if typ.upper().startswith("EX-"):
            wanted.append(doc)

    seen: set[str] = set()
    out: list[str] = []
    for n in wanted:
        if n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def _list_8k_ex_files(
    session: requests.Session,
    filing: FilingRef,
    *,
    rate_limiter: RateLimiter,
) -> list[str]:
    """
    Return filenames to download for GUI:
    - rows in filing index table whose Type is 8-K / 8-K/A
    - rows whose Type starts with EX- (all exhibits)
    No extension restriction.
    """
    items = get_filing_index_items(session, filing, rate_limiter=rate_limiter)
    cik_int = cik_to_int_str(filing.cik10)
    base_url = f"{SEC_ARCHIVES_BASE}/edgar/data/{cik_int}/{filing.accession_dir}"

    index_name = _choose_index_html_name(items, filing.accession_no)
    if not index_name:
        raise RuntimeError(f"Cannot locate filing index HTML for accession {filing.accession_no}")

    html = sec_get_text(session, f"{base_url}/{index_name}", rate_limiter=rate_limiter)
    parser = _EdgarIndexParser()
    parser.feed(html)

    wanted: list[str] = []
    for doc, typ in parser.rows:
        doc = safe_filename(doc)
        t = (typ or "").strip().upper()
        if not doc:
            continue
        if t in ("8-K", "8-K/A") or t.startswith("EX-"):
            wanted.append(doc)

    seen: set[str] = set()
    out: list[str] = []
    for n in wanted:
        if n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def download_filing(
    session: requests.Session,
    filing: FilingRef,
    out_dir: Path,
    *,
    rate_limiter: RateLimiter,
    save_manifest: bool,
    download_mode: str = "all",
) -> tuple[FilingRef, int]:
    base_dir = out_dir / filing.cik10 / f"{filing.filing_date}_{filing.accession_no}"
    base_dir.mkdir(parents=True, exist_ok=True)

    # Write a small manifest for quick lookup
    if save_manifest and download_mode == "all":
        manifest_path = base_dir / "manifest.json"
        if not manifest_path.exists():
            manifest_path.write_text(
                json.dumps(
                    {
                        "cik": filing.cik10,
                        "accessionNumber": filing.accession_no,
                        "filingDate": filing.filing_date,
                        "form": filing.form,
                        "primaryDocument": filing.primary_document,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

    cik_int = cik_to_int_str(filing.cik10)
    base_url = f"{SEC_ARCHIVES_BASE}/edgar/data/{cik_int}/{filing.accession_dir}"

    downloaded = 0
    if download_mode == "8k_ex":
        names = _list_8k_ex_files(session, filing, rate_limiter=rate_limiter)
        for name in names:
            url = f"{base_url}/{name}"
            target = base_dir / name
            sec_download_file(session, url, target, rate_limiter=rate_limiter)
            downloaded += 1
    elif download_mode == "primary_ex_htm":
        names = _list_primary_ex_htm_files(session, filing, rate_limiter=rate_limiter)
        for name in names:
            url = f"{base_url}/{name}"
            target = base_dir / name
            sec_download_file(session, url, target, rate_limiter=rate_limiter)
            downloaded += 1
    else:
        items = get_filing_index_items(session, filing, rate_limiter=rate_limiter)
        for it in items:
            name = it.get("name")
            if not name:
                continue
            name = safe_filename(name)
            url = f"{base_url}/{name}"
            target = base_dir / name
            sec_download_file(session, url, target, rate_limiter=rate_limiter)
            downloaded += 1

    return filing, downloaded


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download SEC EDGAR Form 8-K filings (primary + all attachments).")
    p.add_argument("--ciks", nargs="*", default=None, help="CIK list (e.g. 0000320193 0001652044).")
    p.add_argument("--cik-file", default=None, help="Text file containing CIKs (comma/space/newline separated).")
    p.add_argument("--out", default="downloads", help="Output directory.")
    p.add_argument("--user-agent", required=True, help='SEC required User-Agent, include email. e.g. "Name email@domain.com"')
    p.add_argument("--include-amendments", action="store_true", help="Also include 8-K/A.")
    p.add_argument("--start-date", default=None, help="Filter filings on/after this date (YYYY-MM-DD or YYYY/MM/DD).")
    p.add_argument(
        "--download-mode",
        default="primary_ex_htm",
        choices=["primary_ex_htm", "8k_ex", "all"],
        help="Which files to download per filing. Default: primary_ex_htm (primary + EX-* and only .htm).",
    )
    p.add_argument(
        "--source",
        default="cik",
        choices=["cik", "master_index"],
        help="Target source: per-company (cik) or quarterly master index (master_index).",
    )
    p.add_argument(
        "--master-start-year",
        type=int,
        default=2001,
        help="When --source master_index: first year to scan (default: 2001).",
    )
    p.add_argument(
        "--shard",
        default=None,
        help="Optional sharding like '1/3' to split work across machines (implemented for master_index mode).",
    )
    p.add_argument(
        "--targets-manifest",
        default=None,
        help="When --source master_index: path to write/read targets manifest (jsonl). Default: under --out.",
    )
    p.add_argument(
        "--reuse-targets-manifest",
        action="store_true",
        help="When --source master_index: reuse existing --targets-manifest instead of rescanning quarterly index.",
    )
    p.add_argument(
        "--manifest-only",
        action="store_true",
        help="When --source master_index: build targets + write manifest, then exit without downloading.",
    )
    p.add_argument("--min-interval", type=float, default=0.2, help="Minimum seconds between SEC requests (global).")
    p.add_argument("--max-workers", type=int, default=3, help="Parallel download workers across filings.")
    p.add_argument("--save-manifest", action="store_true", help="Write manifest.json per filing folder.")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    ciks: list[str] = []
    if args.ciks:
        ciks.extend(args.ciks)
    if args.cik_file:
        ciks.extend(iter_ciks_from_file(Path(args.cik_file)))
    ciks = [normalize_cik(c) for c in ciks]
    if not ciks:
        raise SystemExit("No CIKs provided. Use --ciks or --cik-file.")

    summary = run_download(
        ciks=ciks,
        out=args.out,
        user_agent=args.user_agent,
        include_amendments=bool(args.include_amendments),
        start_date=args.start_date,
        source_mode=args.source,
        master_start_year=int(args.master_start_year),
        shard=args.shard,
        targets_manifest=args.targets_manifest,
        reuse_targets_manifest=bool(args.reuse_targets_manifest),
        manifest_only=bool(args.manifest_only),
        min_interval=float(args.min_interval),
        max_workers=int(args.max_workers),
        save_manifest=bool(args.save_manifest),
        download_mode=str(args.download_mode),
    )
    return 0 if summary.get("failed", 0) == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

