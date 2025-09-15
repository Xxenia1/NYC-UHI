#!/usr/bin/env python3
# %%
"""
Fetch ACS 5-year (2020 - 2024) tract-level data for New York City (state FIPS 36,
counties: 005 Bronx, 047 Kings (Brooklyn), 061 New York (Manhattan),
081 Queens, 085 Richmond (Staten Island)).

Tables pulled:
- Income:   B19013_001E (Median household income)
- Race:     B02001_001E (Total), _002E White, _003E Black, _005E Asian
- Ethnicity:B03002_001E (Total), _012E Hispanic or Latino
- Age:      B01001 (Total; <5; 65+ computed from components)
- Housing:  B25003_001E (Total), _002E Owner, _003E Renter

Outputs:
- data/acs_nyc_tract_YYYY.csv (tidy long-form per year with derived % columns)
- data/acs_nyc_tract_2020_2024_long.csv (all years stacked)
- data/acs_nyc_tract_2020_2024_wide.csv (one row per GEOID with year-suffixed cols)

Usage:
    # Optional: export your Census API key to increase rate limits
    export CENSUS_API_KEY=your_key_here
    python fetch_acs_nyc_2020_2024.py
"""

import csv
import os
import sys
import time
import json
from pathlib import Path
from urllib.parse import urlencode
import urllib.request
import pandas as pd
import numpy as np


YEARS = [2020, 2021, 2022, 2023]  # ACS 5-year vintages
STATE = "36"  # New York
COUNTIES = {
    "005": "Bronx",
    "047": "Kings",       # Brooklyn
    "061": "New York",    # Manhattan
    "081": "Queens",
    "085": "Richmond"     # Staten Island
}

# Variable lists
VARS = {
    # Income
    "B19013_001E": "median_income",
    # Race (B02001)
    "B02001_001E": "race_total",
    "B02001_002E": "white_alone",
    "B02001_003E": "black_alone",
    "B02001_005E": "asian_alone",
    # Ethnicity (Hispanic/Latino)
    "B03002_001E": "eth_total",
    "B03002_012E": "hispanic_any",
    # Age total + parts to compute <5 and 65+
    "B01001_001E": "pop_total",
    # under 5 = male<5 (003E) + female<5 (027E)
    "B01001_003E": "male_under5",
    "B01001_027E": "female_under5",
    # 65+ = male 65-66 (020E) + 67-69 (021E) + 70-74 (022E) + 75-79 (023E) + 80-84 (024E) + 85+ (025E)
    "B01001_020E": "male_65_66",
    "B01001_021E": "male_67_69",
    "B01001_022E": "male_70_74",
    "B01001_023E": "male_75_79",
    "B01001_024E": "male_80_84",
    "B01001_025E": "male_85_plus",
    # 65+ females
    "B01001_044E": "female_65_66",
    "B01001_045E": "female_67_69",
    "B01001_046E": "female_70_74",
    "B01001_047E": "female_75_79",
    "B01001_048E": "female_80_84",
    "B01001_049E": "female_85_plus",
    # Housing tenure
    "B25003_001E": "hh_total",
    "B25003_002E": "owner_occ",
    "B25003_003E": "renter_occ"
}

API_BASE = "https://api.census.gov/data/{year}/acs/acs5"

def build_url(year: int, vars_for_call: list, county: str, key: str | None) -> str:
    params = {
        "get": ",".join(vars_for_call),
        "for": "tract:*",
        "in": f"state:{STATE} county:{county}"
    }
    if key:
        params["key"] = key
    return f"{API_BASE.format(year=year)}?{urlencode(params)}"

# —— 替换原 fetch_json —— 
import urllib.error
from json import JSONDecodeError

def fetch_json(url: str, retries: int = 6, backoff: float = 1.6):
    """
    更稳健的抓取：处理 429/5xx、网络错误、返回非 JSON 的情况。
    指数回退: 1, 1.6, 2.56, ...
    """
    for i in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "nyc-uhi-acs-fetch/1.0"}
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                # 2xx 正常
                data = resp.read().decode("utf-8")
            out = json.loads(data)
            return out
        except urllib.error.HTTPError as e:
            # 429 限流/ 5xx 服务器错误：等待后重试
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(backoff ** i)
                continue
            # 其它 HTTP 错误：抛出
            raise
        except (urllib.error.URLError, TimeoutError):
            time.sleep(backoff ** i)
            continue
        except JSONDecodeError:
            # 有时返回 HTML 错页，等待再试
            time.sleep(backoff ** i)
            continue
    # 多次重试失败：
    raise RuntimeError(f"Failed to fetch after {retries} attempts: {url}")


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def to_int(x):
    try:
        return int(x)
    except:
        return None
# data cleaning and processing moved into main()
def clean_csv(file_path):
    df = pd.read_csv(file_path)

    # 替换 ACS 缺失值编码为 NaN
    df = df.replace({
        -666666666: np.nan,
        -222222222: np.nan
    })

    # 保存覆盖
    df.to_csv(file_path, index=False)
    print(f"✅ Cleaned file saved: {file_path}")

def main():
    out_dir = Path("data")
    ensure_dir(out_dir)
    key = os.getenv("CENSUS_API_KEY")

    # The API allows up to ~50 variables per call; we're safely under that.
    var_codes = list(VARS.keys())

    all_rows_long = []

    for year in YEARS:
        rows = []
    for county in COUNTIES.keys():
        url = build_url(year, var_codes, county, key)
        try:
            data = fetch_json(url)

            # 1) 校验返回
            if not data or len(data) < 2:
                print(f"[WARN] {year} {county} returned empty/short table.")
                continue

            # 2) 表头与索引
            headers = data[0]
            idx = {h: i for i, h in enumerate(headers)}

            # 3) 逐行解析
            for rec in data[1:]:
                row = {VARS[v]: to_int(rec[idx[v]]) if v in idx else None
                       for v in var_codes}
                state = rec[idx["state"]]
                county_fips = rec[idx["county"]]
                tract = rec[idx["tract"]]
                geoid = f"{state}{county_fips}{tract}"

                row.update({
                    "year": year,
                    "state": state,
                    "county": county_fips,
                    "tract": tract,
                    "GEOID": geoid,
                    "borough": COUNTIES[county_fips]
                })

                # —— 派生指标（防除零）——
                race_total = row.get("race_total") or 0
                eth_total  = row.get("eth_total") or 0
                pop_total  = row.get("pop_total") or 0
                hh_total   = row.get("hh_total") or 0

                row["pct_white"]    = round((row.get("white_alone") or 0)/race_total*100,3) if race_total else None
                row["pct_black"]    = round((row.get("black_alone") or 0)/race_total*100,3) if race_total else None
                row["pct_asian"]    = round((row.get("asian_alone") or 0)/race_total*100,3) if race_total else None
                row["pct_hispanic"] = round((row.get("hispanic_any") or 0)/eth_total*100,3) if eth_total else None

                under5 = (row.get("male_under5") or 0) + (row.get("female_under5") or 0)
                age65p = sum([(row.get(k) or 0) for k in [
                    "male_65_66","male_67_69","male_70_74","male_75_79","male_80_84","male_85_plus",
                    "female_65_66","female_67_69","female_70_74","female_75_79","female_80_84","female_85_plus"
                ]])
                row["under5"] = under5
                row["age65plus"] = age65p
                row["pct_under5"] = round(under5/pop_total*100,3) if pop_total else None
                row["pct_65plus"] = round(age65p/pop_total*100,3) if pop_total else None
                row["pct_renter"] = round((row.get("renter_occ") or 0)/hh_total*100,3) if hh_total else None
                row["pct_owner"]  = round((row.get("owner_occ") or 0)/hh_total*100,3) if hh_total else None

                rows.append(row)

        except Exception as e:
            print(f"[WARN] {year} county {county} fetch/process failed: {e}")
            # 失败只跳过该 county，不影响下一个 county
            continue

    # ← 这里写当年的 CSV；即使有少数 county 失败，也会写成功的部分
    # write_year_csv(rows, year)  # 保持你原来的写法


        # Write per-year CSV
        cols = ["year","GEOID","state","county","borough","tract",
                "median_income",
                "race_total","white_alone","black_alone","asian_alone","pct_white","pct_black","pct_asian",
                "eth_total","hispanic_any","pct_hispanic",
                "pop_total","under5","age65plus","pct_under5","pct_65plus",
                "hh_total","owner_occ","renter_occ","pct_owner","pct_renter"]
        year_path = out_dir / f"acs_nyc_tract_{year}.csv"
        
        with open(year_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cols)
            writer.writeheader()
            for r in rows:
                writer.writerow({k: r.get(k) for k in cols})

        print(f"Wrote {year_path} with {len(rows)} rows.")
        all_rows_long.extend(rows)

        # Be polite to the API
        time.sleep(0.6)

    # Write stacked long CSV
    long_path = out_dir / "acs_nyc_tract_2020_2024_long.csv"
    cols_long = list(all_rows_long[0].keys()) if all_rows_long else []
    with open(long_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols_long)
        writer.writeheader()
        for r in all_rows_long:
            writer.writerow(r)
    print(f"Wrote {long_path} with {len(all_rows_long)} rows.")

    # 🔹 清洗 long CSV
    clean_csv(long_path)


    # Build wide CSV (year-suffixed columns)
    # Keep GEOID + static geography; year-specific variables suffixed by _YYYY
    keep_static = ["GEOID","state","county","borough","tract"]
    year_vars = ["median_income","pct_white","pct_black","pct_asian","pct_hispanic",
                 "pct_under5","pct_65plus","pct_owner","pct_renter","pop_total","hh_total"]
    # Group rows by GEOID
    from collections import defaultdict
    by_geo = defaultdict(dict)
    geo_meta = {}
    for r in all_rows_long:
        geoid = r["GEOID"]
        y = r["year"]
        for v in year_vars:
            by_geo[geoid][f"{v}_{y}"] = r.get(v)
        if geoid not in geo_meta:
            geo_meta[geoid] = {k: r.get(k) for k in keep_static}

    wide_cols = keep_static + [f"{v}_{y}" for y in YEARS for v in year_vars]
    wide_path = out_dir / "acs_nyc_tract_2020_2024_wide.csv"
    with open(wide_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=wide_cols)
        writer.writeheader()
        for geoid, vals in by_geo.items():
            base = geo_meta.get(geoid, {"GEOID": geoid})
            row = {**base, **{col: vals.get(col) for col in wide_cols if col not in base}}
            writer.writerow(row)

    print(f"Wrote {wide_path} with {len(by_geo)} tracts.")
    clean_csv(wide_path)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Cancelled by user.", file=sys.stderr)
        sys.exit(1)
# %%