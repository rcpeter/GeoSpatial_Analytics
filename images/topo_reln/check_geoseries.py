from shapely.geometry import Polygon
import geopandas as gpd
import matplotlib.pyplot as plt
import dask_geopandas as dask_gpd

polygon_coord1 = [(0, 0), (1, 0), (1, 1), (0, 1)]
polygon_coord2 = [(1, 1), (1, 2), (2, 2), (2, 1)]

poly_1 = Polygon(polygon_coord1)
poly_2 = Polygon(polygon_coord2)

polygon = gpd.GeoSeries([poly_1, poly_2, name='polygon', meta='object', division])

fig, ax = plt.subplots()
polygon.plot(ax=ax)
plt.savefig('/home/y4xxh/Documents/SSTD_GeoSpatial/images/topo_reln/check_Geoseries.jpeg')
