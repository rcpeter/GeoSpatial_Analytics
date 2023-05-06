import geopandas as gpd
'''
Using libpysal and esda, because pysal is not a traditional package but a meta package  - pysal does not provide functionality, but installs all the separate pysal packages to make sure they can work together.
'''

import esda # Using for Moran's I computation
import libpysal as lps # Using for spatial weights
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

# Calculate the spatial weights matrix using Queen contiguity
weights = lps.weights.Queen.from_dataframe(world)

# Standardize the population data
world["pop_est_std"] = (world["pop_est"] - world["pop_est"].mean()) / world["pop_est"].std()

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
plt.savefig('/home/y4xxh/Documents/SSTD_GeoSpatial/images/analytics/spatial_autocorr_gpd.jpeg')

'''
Sergio Ray
'''