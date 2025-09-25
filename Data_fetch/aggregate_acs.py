# %% --------------- Imports & Setup ---------------
from pathlib import Path
import logging
import re
import pandas as pd
import geopandas as gpd

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(message)s"
)

pd.set_option("display.width", 180)
pd.set_option("display.max_columns", 40)

# %% --------------- Paths (改成你的) ---------------
BASE = Path("/Users/xeniax/Desktop/Map Inspiration/NYC UHI")

SHP_PATH = BASE / "data/2020censusTract/nyct2020.shp"
CSV_PATH = BASE / "NYC-UHI/Data_fetch/data/acs_nyc_tract_2020_2024_wide.csv"

OUT_GPKG_JOIN = BASE / "data/2020censusTract/nyct2020_with_acs.gpkg"
OUT_GPKG_NTA  = BASE / "data/2020censusTract/nyc_acs_nta_agg.gpkg"

GROUP_FIELD = "NTA2020"     # 想聚合到 NTA；如果改聚合到 Borough 则写 "BoroName"
TRACT_ID_CANDIDATES = ("GEOID", "GEOID10", "CT2020")  # 逐个探测

# %% --------------- Helper: weighted average ---------------
def weighted_avg(df: pd.DataFrame, value_col: str, weight_col: str) -> float | None:
    v, w = df[value_col], df[weight_col]
    w = pd.to_numeric(w, errors="coerce")
    v = pd.to_numeric(v, errors="coerce")
    if w.notna().sum() == 0 or w.fillna(0).sum() == 0:
        return None
    return (v * w).sum() / w.sum()

# %% --------------- Main ---------------
def main():
    # 1) 读图层
    gdf = gpd.read_file(SHP_PATH)
    logging.info(f"Shapefile loaded: {len(gdf):,} rows, CRS={gdf.crs}")

    # 2) 确认 tract 主键字段
    tract_key = next((k for k in TRACT_ID_CANDIDATES if k in gdf.columns), None)
    assert tract_key, f"在 {SHP_PATH.name} 里找不到 tract 主键字段 {TRACT_ID_CANDIDATES}"
    logging.info(f"Detected tract key in shp: {tract_key}")

    # 3) NTA/聚合字段必须存在
    assert GROUP_FIELD in gdf.columns, f"聚合字段 {GROUP_FIELD} 不在 shapefile 属性表里"
    # 4) 统一 tract 主键为 11 位字符串
    gdf[tract_key] = gdf[tract_key].astype(str).str.zfill(11)

    # 5) 读 ACS 宽表
    df = pd.read_csv(CSV_PATH, low_memory=False, dtype={ "GEOID": str, "GEOID10": str, "CT2020": str })
    logging.info(f"CSV loaded: {df.shape}")

    # 6) 找 CSV 的 tract 主键
    csv_key = next((k for k in TRACT_ID_CANDIDATES if k in df.columns), None)
    assert csv_key, f"在 CSV 里找不到 tract 主键字段 {TRACT_ID_CANDIDATES}"
    logging.info(f"Detected tract key in CSV: {csv_key}")

    df[csv_key] = df[csv_key].astype(str).str.zfill(11)

    # 7) 粗检字段类型：把明显的数值列转数值
    # 规则：数字开头/包含pop/total/pct/med 的列视为数值
    numeric_like = [c for c in df.columns
                    if c != csv_key and (
                        re.search(r"^(pop|total|pct|med|median|hh|owner|renter|age)", c) or
                        re.search(r"\d", c)
                    )]
    for c in numeric_like:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    logging.info("Sample dtypes in CSV:")
    logging.info(df[numeric_like[:12]].dtypes)

    # 8) 左连接：把 NTA 等分组字段带入 tract 表
    merged = df.merge(
        gdf[[tract_key, GROUP_FIELD]],
        left_on=csv_key, right_on=tract_key,
        how="left", validate="m:1"
    )
    miss = merged[GROUP_FIELD].isna().mean()
    logging.info(f"After join: {len(merged):,} rows; {miss:.1%} rows missing {GROUP_FIELD}")
    assert miss < 0.02, f"有 {miss:.1%} 行找不到 {GROUP_FIELD}，多半是主键没对上（前导零/列名/年份）"

    # 9) 构建聚合方案：sum 列 vs 加权平均列
    sum_cols  = [c for c in merged.columns if re.search(r"(pop|_total$|_count$)", c, re.I)]
    # 注意：选择一个合理的权重列（优先用 2023 总人口；没有就 fallback）
    weight_col_candidates = [c for c in merged.columns if re.fullmatch(r"pop_total_20\d{2}", c)]
    weight_col = max(weight_col_candidates, default=None)
    if not weight_col:
        # 再退一步：找一个最“像”总人口的列
        for guess in ["pop_total_2023", "pop_total_2022", "pop_total", "hh_total"]:
            if guess in merged.columns:
                weight_col = guess
                break
    assert weight_col, "没有找到作为权重的总人口列（例如 pop_total_2023）。请确认 CSV 字段。"
    logging.info(f"Weight column: {weight_col}")

    # 比例/中位数类列：含 pct_ 或 med 字样，且不是 sum_cols
    wavg_cols = [c for c in merged.columns
                 if (("pct_" in c) or ("med" in c))
                 and c not in sum_cols
                 and c not in (tract_key, csv_key, GROUP_FIELD)]

    # 10) 执行聚合（避免 groupby.apply 引发的 FutureWarning）
    group = merged.groupby(GROUP_FIELD, dropna=False)

    # 10.1 sum 聚合
    agg_sum = group[sum_cols].sum(min_count=1)

    # 10.2 加权平均聚合（逐列计算）
    agg_w = {}
    for c in wavg_cols:
        agg_w[c] = group.apply(lambda g: weighted_avg(g, c, weight_col))
    agg_w = pd.DataFrame(agg_w)

    nta_agg = pd.concat([agg_sum, agg_w], axis=1).reset_index()

    # 11) 结果自检
    logging.info(f"NTA aggregated: {nta_agg.shape}")
    logging.info(nta_agg.head(6))

    # 数值全 NA 的列列出来，方便你排查
    all_na = [c for c in nta_agg.columns if nta_agg[c].isna().all() and c != GROUP_FIELD]
    if all_na:
        logging.warning(f"{len(all_na)} 列在聚合后全是 NA（可能原始列是字符串/全空）：{all_na[:10]}...")

    # 12) 写回 GPKG（也把 tract+ACS 的 join 结果先写一份，便于 QGIS 检查）
    # 12.1 写 join 结果
    joined_gdf = gdf.merge(
        df, left_on=tract_key, right_on=csv_key, how="left", validate="1:m"
    )
    joined_gdf.to_file(OUT_GPKG_JOIN, layer="tract_with_acs", driver="GPKG")
    logging.info(f"Saved joined layer → {OUT_GPKG_JOIN}::tract_with_acs")

    # 12.2 写 NTA 聚合结果（仅属性表）
    nta_agg_gdf = gdf[[GROUP_FIELD, "geometry"]].dissolve(by=GROUP_FIELD, as_index=False)
    nta_agg_gdf = nta_agg_gdf.merge(nta_agg, on=GROUP_FIELD, how="left", validate="1:1")

    nta_agg_gdf.to_file(OUT_GPKG_NTA, layer="nta_acs", driver="GPKG")
    logging.info(f"Saved NTA aggregate → {OUT_GPKG_NTA}::nta_acs")

    # 13) 额外导出一个 CSV 便于快速 eyeballing
    (OUT_GPKG_NTA.parent / "nta_acs_preview.csv").write_text(
        nta_agg.head(50).to_csv(index=False)
    )
    logging.info("Preview CSV exported (前 50 行).")

if __name__ == "__main__":
    main()
# %%
