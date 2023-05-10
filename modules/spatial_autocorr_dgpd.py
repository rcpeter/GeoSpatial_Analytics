# Partition based on re-ingested data, for visualisation: https://tomaugspurger.github.io/posts/dask-geopandas-partitions/

'''
The issue with performing spatial autocorrelation calculations (like Moran's I) using Dask-GeoPandas is that the calculations would require knowledge about entire dataset, not just individual chunks. Dask-GeoPandas is designed for operations that can be performed on individual chunks of data in-parallel thus no support for these direct calculations.

There is a re-route based approach which could be taken:
Gather data into a single GeoPandas DF when you need to perform spatial autocorrelation calculations. This would allow to take advantage of Dask's parallel processing for parts of the analysis, but can't be parallelized, while still using GeoPandas and PySAL for rest.

Another way which I could proceed with is 
'''

'''
pysal function such as ps.lib.weights.Queen.from_dataframe, ps.explore.esda.Moran and ps.explore.esda.Moran_Local require in memory geopandas geodataframes, therefore used compute() to convert dask geodatafram to geopandas gdf when needed
'''

import dask_geopandas as dgpd
import geopandas as gpd
import esda
import libpysal as lps
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import dask

# Load built-in dataset
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

# Convert GeoDataFrame to Dask-GeoDataFrame
world_dask = dgpd.from_geopandas(world, npartitions=4)

# Calculate the spatial weights matrix using Queen contiguity
# Note: The spatial weights matrix calculation is done using the original GeoDataFrame because I could not find use case for using spatial weights matric calculation using dask (the closest found was spatial partitioning based, but building a matrix is computationally (would need to load the entire dataset into the memory) tedious.)
weights = lps.weights.Queen.from_dataframe(world)

# Standardize the population data using Dask
# Doesn't really make sense to use this, but still, for the sake of implementation
world_dask["pop_est_std"] = (world_dask["pop_est"] - world_dask["pop_est"].mean()) / world_dask["pop_est"].std()

# Compute values and convert back to GeoDataFrame for operations not supported in Dask-GeoPandas
world = world_dask.compute()

# Compute Global Moran's I
global_moran = esda.moran.Moran(world["pop_est_std"], weights)
print(f"Global Moran's I: {global_moran.I:.4f}, p-value: {global_moran.p_sim:.4f}")

# Compute Local Moran's I
local_moran = esda.moran.Moran_Local(np.nan_to_num(world["pop_est_std"]), weights)

# Add Local Moran's I values and p-values to the GeoDataFrame
world["local_moran"] = local_moran.Is
world["local_moran_p"] = local_moran.p_sim

# Define a function to categorize Local Moran's I values
def moran_category(row):
    if row["local_moran_p"] < 0.05:
        if row["local_moran"] > 0:
            return "High-High"
        else:
            return "Low-High"
    else:
        if row["local_moran"] > 0:
            return "High-Low"
        else:
            return "Low-Low"

# Apply the function to the GeoDataFrame
world["moran_category"] = world.apply(moran_category, axis=1)

# Plot - Local Moran's I categories
fig, ax = plt.subplots(figsize=(15, 10))
world.plot(column="moran_category", categorical=True, legend=True, cmap="coolwarm_r", ax=ax)
plt.title("Local Moran's I Categories")
plt.savefig('/home/y4xxh/Documents/SSTD_GeoSpatial/images/analytics/spatial_autocorr_dgpd.jpeg')

