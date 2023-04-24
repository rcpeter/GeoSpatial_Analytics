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
print('\nNaturalearth_lowres dataset looks like: \n', gdf)

# Convert the GeoDataFrame to a Dask DataFrame
ddf = dd.from_pandas(gdf, npartitions=4)
print("\nGeoDataFrame to Dask DataFrame: \n", ddf)

# Convert the Dask DataFrame to a Dask GeoDataFrame
dask_gdf = from_dask_dataframe(ddf)
print("\nDask DataFrame to Dask GeoDataFrame looks like: \n", dask_gdf)

# Define a geospatial query to filter data for the Asia continent
result = dask_gdf[dask_gdf['continent'] == 'Asia'].compute()
print("\nGeoSpatial query after defining: \n", result)

# Use PySAL's mapclassify to classify the GDP data using the Quantiles classification scheme
gdp_classified = mc.Quantiles(result['gdp_md_est'], k=5)

# Visualize the results
fig, ax = plt.subplots(figsize=(10, 10))
gdf[gdf['continent'] != 'Asia'].plot(facecolor='lightgray', ax=ax)
result.assign(gdp_classified=gdp_classified.yb).plot(column='gdp_classified', cmap='coolwarm', legend=True, ax=ax)
ax.set_title('GDP in Asia (Quantiles Classification)')
plt.savefig('GDP_in_Asia_quantiles.jpeg', format='jpeg', dpi=300)
