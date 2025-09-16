# joint ACS to shp (census data)
# joint.py
from pathlib import Path
import geopandas as gpd
import pandas as pd

# --- 1) 路径设置（就地读写） ---
base = Path("/Users/xeniax/Desktop/Map Inspiration/NYC UHI/data/2020censusTract")
shp_path = base / "nyct2020.shp"            # 如果文件名不同，改这里
csv_path = "/Users/xeniax/Desktop/Map Inspiration/NYC UHI/NYC-UHI/Data_fetch/data/acs_nyc_tract_2020_2024_wide.csv"  # CSV在data/下就改成 base.parent / "acs_..." 

# --- 2) 读数据 ---
gdf = gpd.read_file(shp_path)

# shapefile里的 tract 主键字段名；常见有 GEOID / GEOID10 / CT2020
key_shp = "GEOID" if "GEOID" in gdf.columns else ("GEOID10" if "GEOID10" in gdf.columns else "CT2020")
gdf[key_shp] = gdf[key_shp].astype(str).str.zfill(11)  # 统一成11位字符串

df = pd.read_csv(csv_path, dtype={"GEOID": str})
df["GEOID"] = df["GEOID"].str.zfill(11)

# --- 3) 合并 ---
merged = gdf.merge(df, left_on=key_shp, right_on="GEOID", how="left")

# 合并质量检查
matched = merged["GEOID"].notna().sum()
total = len(merged)
print(f"Joined {matched}/{total} tracts ({matched/total:.1%})")

# --- 4) 导出到同一目录（就地） ---
# 推荐：GeoPackage（字段名长度安全、支持中文、一个文件即可）
gpkg_out = base / "nyct2020_with_acs.gpkg"
merged.to_file(gpkg_out, driver="GPKG", layer="nyct2020_with_acs")
print(f"Wrote {gpkg_out}")

# 如需Shapefile（注意字段名<=10字符会被截断）
shp_out = base / "nyct2020_with_acs.shp"
merged.to_file(shp_out, driver="ESRI Shapefile")
print(f"Wrote {shp_out}")

