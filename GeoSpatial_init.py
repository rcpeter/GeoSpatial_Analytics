# Note: Only using matplotlib here for visualization, refer to GeoSpatial_pysal.py for using pysal for visualization.

import warnings
from shapely.errors import ShapelyDeprecationWarning
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import dask.dataframe as dd
from dask_geopandas import from_dask_dataframe
import pysal.viz.mapclassify as mc
import matplotlib.pyplot as plt

# Load the naturalearth_lowres dataset using GeoPandas
gdf = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
print("\nnaturalearth_lowres dataset looks like: \n", gdf)

# Convert the GeoDataFrame to a Dask DataFrame
ddf = dd.from_pandas(gdf, npartitions=4)
print("\nGeoDataFrame to Dask DataFrame: \n", ddf)

# Convert the Dask DataFrame to a Dask GeoDataFrame
dask_gdf = from_dask_dataframe(ddf)
print("\nDask DataFrame to Dask GeoDataFrame looks like: \n", dask_gdf)

# Define a geospatial query
result = dask_gdf[dask_gdf['continent'] == 'Asia'].compute()
print("\nGeoSpatial query after defining: \n", result)

# Visualize the results
fig, ax = plt.subplots(figsize=(10, 10))
result.plot(column='gdp_md_est', legend=True, ax=ax, classification_kwds={'scheme': 'quantiles', 'k': 5})
ax.set_title('GDP in Asia')
plt.savefig('GDP_in_Asia.jpeg', format='jpeg', dpi=300)

