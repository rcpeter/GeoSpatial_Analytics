import geopandas as gpd

shp_file = gpd.read_file('/home/y4xxh/anaconda3/envs/SSTD_GeoSpatial/lib/python3.7/site-packages/geopandas/datasets/naturalearth_lowres/naturalearth_lowres.shp')

shp_file['geometry'] = shp_file['geometry'].apply(lambda x: x.wkt)

shp_file.to_csv('/home/y4xxh/Documents/SSTD_GeoSpatial/data/naturalearth_lowres.csv', index=False)