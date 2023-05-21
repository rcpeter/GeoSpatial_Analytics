import geopandas as gpd

data = gpd.datasets.get_path('naturalearth_cities')
print(data)