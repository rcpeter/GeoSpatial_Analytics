import geopandas as gpd
import dask.dataframe as dd
import matplotlib.pyplot as plt
from pysal.viz import mapclassify

path = gpd.datasets.get_path('naturalearth_lowres')
gdf = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

# Perform area and population density calculations in GeoPandas - Else cannot interpret data type
gdf['area'] = gdf.geometry.area
gdf['pop_density'] = gdf['pop_est'] / gdf['area']

# Convert the modified Geopandas GeoDataFrame to a Dask DataFrame
ddf = dd.from_pandas(gdf, npartitions=4)

# Compute the Natural Breaks classification for the population density
scheme = mapclassify.NaturalBreaks(ddf['pop_density'].compute(), k=5)

fig, ax = plt.subplots(figsize=(10, 10))
gdf.assign(cl=scheme.yb).plot(column='cl', categorical=True, legend=True, ax=ax)
ax.set_title('Population Density by Country (Natural Breaks)')
plt.savefig('Population_Density_by_Country-Natural_Breaks.jpeg', format='jpeg', dpi=300)

# '''
# GeoSpatial operations - polygon on polygon
# compare every record on table a vs every record on table b
# you're checking if they satisfy the micro benchmarks - topological
# if data set consists of point - lines, polygon, if B intersects A - Crosses
# fetch topological relations
# two step query evaluation:
# 1. geometrtic objects >> Filter >> Candidate result set - filter - min bounding box
# 2. refinemnet >> final result set to fetch overlapping
# '''