"""
Research-parity regression for the heuristic crosswalk (hedge_map v2).

Acceptance criterion #2: for the 135 MF tickers in research
`ticker_classification.csv`, the production crosswalk must reproduce the SAME
sector_etf and industry_etf, OR every divergence must be documented with a reason.

Two layers, because the oracle stores GICS sectors + HUMAN-ANNOTATED industry
strings (e.g. "Application Software (CRM/marketing)") rather than raw Tiingo
fields:

  A. SECTOR parity (offline, deterministic):
     crosswalk.sector_etf(oracle.gics_sector) == oracle.sector_etf
     Expected: 135/135.

  B. INDUSTRY parity on RAW TIINGO (requires the live meta partition from S3):
     for the tickers where the production Tiingo `meta` snapshot has a row,
     crosswalk.classify(tiingo.sector, tiingo.industry).rank1_etf
       == oracle.industry_etf
     Documented divergences (Tiingo taxonomy differs from the oracle's
     hand-curated GICS+annotation) are listed in KNOWN_INDUSTRY_DIVERGENCES and
     do NOT fail the gate.

Layer A runs anywhere (CSV only). Layer B is skipped when S3/creds are absent.
"""

from __future__ import annotations

import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))

import hedge_crosswalk as cw

ORACLE_DEFAULT = (
    "/Users/timhuang/projects/algo-trade/research/motley_fool/etf_hedge/"
    "ticker_classification.csv"
)


def _oracle_path() -> str:
    return os.environ.get("TICKER_CLASSIFICATION_PATH", ORACLE_DEFAULT)


# ---------------------------------------------------------------------------
# Documented industry divergences (raw Tiingo vs oracle's GICS + annotation).
# Each entry: ticker -> (tiingo_industry_seen, oracle_industry_etf, my_etf, reason)
# These are INTENTIONAL: the production crosswalk maps raw Tiingo faithfully; the
# oracle injected company-specific knowledge Tiingo's generic strings don't carry.
# ---------------------------------------------------------------------------
KNOWN_INDUSTRY_DIVERGENCES = {
    "DASH": (
        "Internet Content & Information", "ONLN", "FDN",
        "Tiingo classifies DoorDash sector=Communication Services / "
        "industry='Internet Content & Information' (→FDN). Oracle used Consumer "
        "Discretionary + internet-retail-delivery (→ONLN). Tiingo sector is wrong "
        "for this name; FDN is the defensible mapping from the Tiingo input.",
    ),
    "GEHC": (
        "Health Information Services", "IHI", "XLV",
        "Tiingo industry 'Health Information Services' is not a medical-devices "
        "pure-play, so it falls back to the sector ETF XLV. Oracle hand-knew GE "
        "HealthCare makes imaging devices (→IHI). Generic Tiingo string can't "
        "encode that.",
    ),
    "SNPS": (
        "Software - Infrastructure", "SOXX", "IGV",
        "Tiingo industry 'Software - Infrastructure' (→IGV). Oracle hand-knew "
        "Synopsys is semiconductor EDA (→SOXX). EDA-vs-software is domain "
        "knowledge Tiingo's industry string doesn't capture.",
    ),
    "ABNB": (
        "Travel Services", "XLY", "XLI",
        "Tiingo classifies Airbnb sector=Industrials / industry='Travel Services'. "
        "Both are sector_fallback; the sector ETF differs only because Tiingo "
        "sector=Industrials (→XLI) vs oracle GICS Consumer Discretionary (→XLY). "
        "Tiingo sector misclassification.",
    ),
}


# ---------------------------------------------------------------------------
# Layer A — sector parity (offline)
# ---------------------------------------------------------------------------

def sector_parity(oracle: pd.DataFrame) -> pd.DataFrame:
    """Return per-ticker sector parity rows."""
    rows = []
    for _, r in oracle.iterrows():
        mine = cw.sector_etf(r["gics_sector"])
        rows.append({
            "ticker": r["Symbol"],
            "gics_sector": r["gics_sector"],
            "oracle_sector_etf": r["sector_etf"],
            "my_sector_etf": mine,
            "match": mine == r["sector_etf"],
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Layer B — industry parity on raw Tiingo (needs S3)
# ---------------------------------------------------------------------------

def _load_tiingo_meta():
    """Load the latest Tiingo meta partition keyed by upper-cased ticker. None on failure."""
    try:
        import boto3
        from hedge_map_flow import S3_BUCKET, SYMBOL_REMAP
        s3 = boto3.client("s3", region_name="us-east-1")
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(
            Bucket=S3_BUCKET, Prefix="tiingo/json/fundamentals/meta/date="
        ):
            for o in page.get("Contents", []):
                if o["Key"].endswith("meta.json"):
                    keys.append(o["Key"])
        if not keys:
            return None
        key = sorted(keys)[-1]
        data = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
        out = {}
        for row in data:
            t = str(row.get("ticker", "")).strip().upper()
            t = SYMBOL_REMAP.get(t, t)
            if t:
                out[t] = row
        return out
    except Exception as exc:  # noqa: BLE001
        print(f"  (Tiingo meta unavailable: {exc})")
        return None


def industry_parity(oracle: pd.DataFrame) -> pd.DataFrame | None:
    """Industry parity on raw Tiingo for the meta∩oracle overlap. None if no meta."""
    meta = _load_tiingo_meta()
    if meta is None:
        return None
    rows = []
    for _, r in oracle.iterrows():
        sym = str(r["data_symbol"]).upper()
        m = meta.get(sym)
        if not m:
            continue
        c = cw.classify(m.get("sector"), m.get("industry"))
        mine = c["rank1_etf"]
        oracle_ind = r["industry_etf"]
        documented = sym in KNOWN_INDUSTRY_DIVERGENCES
        rows.append({
            "ticker": sym,
            "tiingo_sector": m.get("sector"),
            "tiingo_industry": m.get("industry"),
            "oracle_industry_etf": oracle_ind,
            "my_industry_etf": mine,
            "match": mine == oracle_ind,
            "documented_divergence": documented,
        })
    return pd.DataFrame(rows)


def run(verbose: bool = True) -> dict:
    oracle = pd.read_csv(_oracle_path())

    sp = sector_parity(oracle)
    sec_match = int(sp["match"].sum())
    sec_total = len(sp)

    ip = industry_parity(oracle)
    if ip is not None and len(ip):
        ind_match = int(ip["match"].sum())
        ind_total = len(ip)
        undoc = ip[(~ip["match"]) & (~ip["documented_divergence"])]
    else:
        ind_match = ind_total = 0
        undoc = pd.DataFrame()

    if verbose:
        print("=" * 60)
        print("RESEARCH PARITY — heuristic crosswalk")
        print("=" * 60)
        print(f"\n[A] SECTOR parity (gics_sector → sector_etf): "
              f"{sec_match}/{sec_total} = {sec_match/sec_total*100:.1f}%")
        if sec_match != sec_total:
            print(sp[~sp["match"]].to_string(index=False))
        if ip is not None and len(ip):
            print(f"\n[B] INDUSTRY parity on raw Tiingo (overlap n={ind_total}): "
                  f"{ind_match}/{ind_total} = {ind_match/ind_total*100:.1f}%")
            div = ip[~ip["match"]]
            if len(div):
                print("\n  Divergences:")
                print(div[["ticker", "tiingo_sector", "tiingo_industry",
                           "oracle_industry_etf", "my_industry_etf",
                           "documented_divergence"]].to_string(index=False))
            if len(undoc):
                print(f"\n  UNDOCUMENTED divergences: {len(undoc)} — GATE FAIL")
            else:
                print("\n  All industry divergences documented — GATE PASS")
        else:
            print("\n[B] INDUSTRY parity: SKIPPED (no Tiingo meta / S3 access)")

    return {
        "sector_match": sec_match,
        "sector_total": sec_total,
        "industry_match": ind_match,
        "industry_total": ind_total,
        "undocumented_divergences": len(undoc),
        "sector_df": sp,
        "industry_df": ip,
    }


if __name__ == "__main__":
    res = run(verbose=True)
    ok = (res["sector_match"] == res["sector_total"]
          and res["undocumented_divergences"] == 0)
    print(f"\nOVERALL PARITY GATE: {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)
