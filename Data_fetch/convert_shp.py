import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# 读取 CSV
df = pd.read_csv("/Users/xeniax/Desktop/Map Inspiration/NYC UHI/data/2015_Street_Tree_Census_-_Tree_Data_20250915.csv")

# 转换成 GeoDataFrame
gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df.longitude, df.latitude),
    crs="EPSG:4326"  # WGS84
)

# 保存为 shapefile
gdf.to_file("nyc_tree_census_2015.shp")
