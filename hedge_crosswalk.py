"""
Heuristic hedge crosswalk — the static, version-controlled IP for hedge_map v2.

Replaces the v1 max-R²-over-57-ETFs ETF selection with a deterministic
sector / industry → ETF mapping:

  rank1 = industry pure-play ETF   (when the industry has a curated pure-play)
  rank2 = sector SPDR ETF          (always, from the 11-sector map)
  rank3 = SPY                      (broad-market fallback)

Classification inputs come from the Tiingo `meta` dataset (`sector`, `industry`),
with `sicSector` / `sicIndustry` as a fallback when Tiingo's `sector` is blank.

Design notes
------------
* SECTOR_TO_ETF maps every observed Tiingo sector string *and* the GICS sector
  names used by the research oracle, so the same map serves both the production
  classifier (raw Tiingo strings) and the research-parity test (GICS strings).
* INDUSTRY_TO_ETF maps the ~40 highest-frequency Tiingo industry strings to a
  pure-play ETF drawn from the research `etf_candidates.csv` subindustry list.
  Anything not in this map falls back to the sector ETF
  (`industry_source="sector_fallback"`).
* Matching is case-insensitive and whitespace-normalized. All keys are stored
  lowercased; lookups normalize the query the same way.

This module has NO external dependencies and is import-safe for unit tests.
"""

from __future__ import annotations

# Crosswalk version tag — bump when either map changes. Surfaced in the manifest.
CROSSWALK_VERSION = "v2-heuristic-20260626"

# Broad-market fallback ETF (rank3, and the floor of the quality-gate chain).
SPY = "SPY"

# ---------------------------------------------------------------------------
# Sector → SPDR sector ETF (11 GICS sectors)
# ---------------------------------------------------------------------------
# Keys are lowercased. We include BOTH the Tiingo sector vocabulary
# (e.g. "consumer cyclical", "financial services", "healthcare", "basic materials")
# and the GICS names used by the research oracle
# (e.g. "consumer discretionary", "financials", "health care", "materials"),
# so a single map drives both production and the parity test.
_SECTOR_TO_ETF_RAW: dict[str, str] = {
    # Technology
    "technology": "XLK",
    "information technology": "XLK",
    "tech": "XLK",
    # Financials
    "financials": "XLF",
    "financial services": "XLF",
    "financial": "XLF",
    # Health Care
    "health care": "XLV",
    "healthcare": "XLV",
    # Consumer Discretionary / Cyclical
    "consumer discretionary": "XLY",
    "consumer cyclical": "XLY",
    # Consumer Staples / Defensive
    "consumer staples": "XLP",
    "consumer defensive": "XLP",
    # Energy
    "energy": "XLE",
    # Industrials
    "industrials": "XLI",
    "industrial": "XLI",
    # Materials
    "basic materials": "XLB",
    "materials": "XLB",
    # Utilities
    "utilities": "XLU",
    # Real Estate
    "real estate": "XLRE",
    # Communication Services
    "communication services": "XLC",
    "communications": "XLC",
    "telecommunication services": "XLC",
}

SECTOR_TO_ETF: dict[str, str] = {k.lower(): v for k, v in _SECTOR_TO_ETF_RAW.items()}

# ---------------------------------------------------------------------------
# Industry → pure-play ETF (curated, high-frequency Tiingo industries)
# ---------------------------------------------------------------------------
# Keys are lowercased Tiingo `industry` strings. The right-hand ETF must be in
# the research etf_candidates.csv set. Anything not present here falls back to
# the stock's sector ETF.
#
# Curation principle (PM decision: curate-top-fallback-rest): map the industries
# that (a) appear with meaningful frequency in the liquid+shortable universe and
# (b) have a clean, well-known pure-play ETF. Niche / ambiguous industries are
# left to the sector fallback rather than forced onto a loosely-related ETF.
_INDUSTRY_TO_ETF_RAW: dict[str, str] = {
    # --- Technology: semis & equipment ---
    "semiconductors": "SMH",
    "semiconductor equipment & materials": "SOXX",
    "semiconductor equipment and materials": "SOXX",
    # --- Technology: software ---
    "software - application": "IGV",
    "software - infrastructure": "IGV",
    "software—application": "IGV",
    "software—infrastructure": "IGV",
    "information technology services": "IGV",
    "it services": "IGV",
    # --- Technology: hardware (cloud/internet plays) ---
    "computer hardware": "SKYY",
    # --- Healthcare ---
    "biotechnology": "XBI",
    "drug manufacturers - general": "IBB",
    "drug manufacturers - specialty & generic": "IBB",
    "drug manufacturers—general": "IBB",
    "drug manufacturers—specialty & generic": "IBB",
    "medical devices": "IHI",
    "medical instruments & supplies": "IHI",
    "medical instruments and supplies": "IHI",
    "diagnostics & research": "IHI",
    "diagnostics and research": "IHI",
    # --- Financials ---
    "banks - regional": "KRE",
    "banks—regional": "KRE",
    "banks - diversified": "KBE",
    "banks—diversified": "KBE",
    # --- Industrials ---
    "aerospace & defense": "ITA",
    "aerospace and defense": "ITA",
    "airlines": "JETS",
    "railroads": "IYT",
    "trucking": "IYT",
    "integrated freight & logistics": "IYT",
    "integrated freight and logistics": "IYT",
    # --- Energy ---
    "oil & gas e&p": "XOP",
    "oil and gas e&p": "XOP",
    "oil & gas exploration & production": "XOP",
    "oil & gas equipment & services": "OIH",
    "oil and gas equipment & services": "OIH",
    # --- Consumer Cyclical / Discretionary ---
    "internet retail": "FDN",
    "apparel retail": "XRT",
    "specialty retail": "XRT",
    "discount stores": "XRT",
    "residential construction": "XHB",
    # --- Communication Services ---
    "internet content & information": "FDN",
    "internet content and information": "FDN",
    "electronic gaming & multimedia": "FDN",
    "electronic gaming and multimedia": "FDN",
    "entertainment": "FDN",
    # --- Solar / clean energy (cross-sector: TAN is the canonical pure-play) ---
    "solar": "TAN",
}

INDUSTRY_TO_ETF: dict[str, str] = {k.lower(): v for k, v in _INDUSTRY_TO_ETF_RAW.items()}


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

def _norm(s) -> str:
    """Lowercase + collapse internal whitespace for robust matching."""
    if s is None:
        return ""
    text = str(s).strip().lower()
    if not text or text in {"nan", "none", "null"}:
        return ""
    return " ".join(text.split())


def sector_etf(sector: str | None) -> str | None:
    """Map a sector string (Tiingo or GICS) to its SPDR ETF, or None if unknown."""
    return SECTOR_TO_ETF.get(_norm(sector))


def industry_etf(industry: str | None) -> str | None:
    """Map a Tiingo industry string to a pure-play ETF, or None if not curated."""
    return INDUSTRY_TO_ETF.get(_norm(industry))


def classify(sector: str | None, industry: str | None) -> dict:
    """
    Resolve the heuristic hedge ladder for one stock.

    Returns a dict with:
      sector_etf      — SPDR sector ETF (rank2) or None if sector unmappable
      industry_etf    — pure-play industry ETF (rank1) or None
      industry_source — 'pure_play' when industry_etf was found,
                        'sector_fallback' otherwise
      rank1_etf       — the intended industry-tier hedge: industry_etf when
                        present, else the sector ETF, else SPY
      rank2_etf       — the sector ETF when present, else SPY
      rank3_etf       — always SPY

    No beta/quality logic here — selection.py applies the quality gate and may
    demote rank1 → sector → SPY based on the computed beta/R².
    """
    s_etf = sector_etf(sector)
    i_etf = industry_etf(industry)

    if i_etf is not None:
        industry_source = "pure_play"
        rank1 = i_etf
    else:
        industry_source = "sector_fallback"
        rank1 = s_etf if s_etf is not None else SPY

    rank2 = s_etf if s_etf is not None else SPY
    rank3 = SPY

    return {
        "sector_etf": s_etf,
        "industry_etf": i_etf,
        "industry_source": industry_source,
        "rank1_etf": rank1,
        "rank2_etf": rank2,
        "rank3_etf": rank3,
    }


def all_referenced_etfs() -> set[str]:
    """Every ETF the crosswalk can emit — used to scope the bar fetch."""
    etfs = set(SECTOR_TO_ETF.values()) | set(INDUSTRY_TO_ETF.values())
    etfs.add(SPY)
    return etfs
