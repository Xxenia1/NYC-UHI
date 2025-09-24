# pip install geopandas pandas pyproj shapely fiona
from pathlib import Path
import re
import pandas as pd
import geopandas as gpd

# ========= 1) 路径配置 =========
BASE = Path("/Users/xeniax/Desktop/Map Inspiration/NYC UHI")

SHP_PATH  = BASE / "data/2020censusTract/nyct2020.shp"        # 你的 tract 面
CSV_PATH  = BASE / "NYC-UHI/Data_fetch/data/acs_nyc_tract_2020_2024_wide.csv"  # 你的 ACS 宽表
OUT_JOIN  = BASE / "data/2020censusTract/nyct2020_with_acs.gpkg"               # 连接后的输出
OUT_AGG   = BASE / "data/2020censusTract/nyc_acs_nta_agg.gpkg"                 # 聚合后的输出

# 设定聚合层级字段（在 nyct2020.shp 里存在的字段）："NTA2020" 或 "BoroName" 等
GROUP_FIELD = "NTA2020"   # 想聚合到 NTA 就用这个；要聚合到 Borough 可改成 "BoroName"

# ========= 2) 读数据 =========
gdf = gpd.read_file(SHP_PATH)

# 兼容 GDF 的 key 字段：GEOID / GEOID10 / CT2020 都有人用，这里做探测
if "GEOID" in gdf.columns:
    key_shp = "GEOID"
elif "GEOID10" in gdf.columns:
    key_shp = "GEOID10"
elif "CT2020" in gdf.columns:
    key_shp = "CT2020"
else:
    raise ValueError("在 shapefile 里找不到 GEOID/GEOID10/CT2020 之类的连接键。")

# 读 CSV（把 GEOID 强制成字符串，避免丢前导 0）
df = pd.read_csv(CSV_PATH, dtype={"GEOID": str, "GEOID10": str, "CT2020": str})

# 兼容 CSV 的 key 字段
for k in ["GEOID", "GEOID10", "CT2020"]:
    if k in df.columns:
        key_csv = k
        break
else:
    raise ValueError("在 CSV 里找不到 GEOID/GEOID10/CT2020 之类的连接键。")

# 统一成 11 位
gdf[key_shp] = gdf[key_shp].astype(str).str.zfill(11)
df[key_csv]  = df[key_csv].astype(str).str.zfill(11)

# ========= 3) 连接 =========
joined = gdf.merge(df, left_on=key_shp, right_on=key_csv, how="left")

# ========= 4) 字段类型清洗：把应为数值的列转为 numeric =========
# 识别规则（尽量泛化）：包含这些模式的就当数值列
numeric_like_patterns = [
    r"(^|_)pop(_|$)",           # …pop_total, pop 等
    r"(^|_)total(_|$)",         # …_total
    r"(^|_)race(_|$)",
    r"(^|_)owner(_|$)",
    r"(^|_)renter(_|$)",
    r"(^|_)male(_|$)",
    r"(^|_)female(_|$)",
    r"(^|_)age\d",              # age65plus 等
    r"(^|_)med(ian)?(_|)inc",   # median_income / med_inc
    r"(^|_)pct(_|$)",           # 百分比
    r"(^|_)len(gth)?(_|$)",
    r"(^|_)area(_|$)",
    r"^\d{2}med_inco$"          # 见你截图的 20med_inco
]

def looks_numeric(col: str) -> bool:
    c = col.lower()
    return any(re.search(p, c) for p in numeric_like_patterns)

# 可能的 ID/名称类字段（保持为字符串）
id_like = {key_shp.lower(), key_csv.lower(), "nta2020", "ntaname", "boroname",
           "ct2020", "borocode", "ctlabel", "cdtaname", "cdta2020", "boroct2020",
           "puma"}

# 找出候选数值列，做 to_numeric（空串/非数字会变 NaN）
num_cols = [c for c in joined.columns
            if c.lower() not in id_like and looks_numeric(c)]
for c in num_cols:
    joined[c] = pd.to_numeric(joined[c], errors="coerce")

# 建议：保存一个**干净版**（GeoPackage 避免 shapefile 限制）
joined.to_file(OUT_JOIN, layer="tract_with_acs", driver="GPKG")
print(f"✓ 连接+清洗完成：{OUT_JOIN}::tract_with_acs")

# ========= 5) 聚合（到 NTA 或 Borough）=========
# a) 定义权重字段（人口总数用来做加权平均）
#   自动找类似 “…pop_total”的列；找不到时回退到 “pop_total”
pop_weight_cols = [c for c in joined.columns if re.search(r"(^|_)pop_total(_|$)", c.lower())]
POP_W = pop_weight_cols[0] if pop_weight_cols else "pop_total"
if POP_W not in joined.columns:
    # 尝试一个更保守的候选
    raise ValueError("没找到人口总数字段（pop_total）。请手工把 POP_W 改成你表里的真实列名。")

# b) 识别“百分比/比例/率”列：以 pct_ 开头或包含 _pct_ 的列
pct_cols = [c for c in num_cols if re.search(r"(^pct_|_pct_)", c.lower())]

# c) 识别“收入”列：median_income / med_inco
inc_cols = [c for c in num_cols if re.search(r"median(_|)inc|^..med_inco$", c.lower())]

# d) “总量”列：数值列里，剔除百分比与收入，剩下的就按 sum
sum_cols = sorted(set(num_cols) - set(pct_cols) - set(inc_cols))

# 先准备一个 DataFrame 用于聚合（避免 geometry 参与 pandas 运算）
df_attr = pd.DataFrame(joined.drop(columns=joined.geometry.name))
df_attr["_w"] = df_attr[POP_W].fillna(0)

# ——— 聚合：总量 = sum ———
agg_dict = {c: "sum" for c in sum_cols}
grouped = df_attr.groupby(GROUP_FIELD, dropna=False).agg(agg_dict)

# ——— 聚合：百分比 = 加权平均（按人口）———
def wavg(group, col, wcol):
    v = group[col]
    w = group[wcol]
    num = (v * w).sum(skipna=True)
    den = w.sum(skipna=True)
    return (num / den) if den and den != 0 else pd.NA

for c in pct_cols:
    grouped[c] = df_attr.groupby(GROUP_FIELD, dropna=False).apply(lambda g: wavg(g, c, "_w"))

# ——— 聚合：收入 = 加权平均（同样按人口；若你更偏好用户数可把 POP_W 换成 hh_total）———
for c in inc_cols:
    grouped[c] = df_attr.groupby(GROUP_FIELD, dropna=False).apply(lambda g: wavg(g, c, "_w"))

grouped = grouped.reset_index()

# 把聚合结果和聚合层级的**几何**合并（ dissolve 只保留几何；属性用我们算的）
geoms = joined[[GROUP_FIELD, joined.geometry.name]].dissolve(by=GROUP_FIELD, as_index=False)
agg_gdf = geoms.merge(grouped, on=GROUP_FIELD, how="left")

# 和 CRS 保持一致
agg_gdf.set_crs(joined.crs, inplace=True)

# 输出聚合结果
agg_gdf.to_file(OUT_AGG, layer=f"agg_by_{GROUP_FIELD.lower()}", driver="GPKG")
print(f"✓ 聚合完成：{OUT_AGG}::agg_by_{GROUP_FIELD.lower()}")
