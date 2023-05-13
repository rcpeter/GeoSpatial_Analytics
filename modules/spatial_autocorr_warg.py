import geopandas as gpd
import esda
import libpysal as lps
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

def compute_spatial_autocorr(data_file_path, cols, output_file_path):
    assert len(cols) > 0, "At least one column name must be provided."
    
    df = gpd.read_file(data_file_path)
    print(df)

    # Ensure the columns exist in the dataframe
    for col in cols:
        assert col in df.columns, f"Column {col} does not exist in the dataframe."
    
    # Calculate the spatial weights matrix using Queen contiguity
    weights = lps.weights.Queen.from_dataframe(df)
    
    # Standardize the provided columns
    for col in cols:
        df[f"{col}_std"] = (df[col] - df[col].mean()) / df[col].std()
    
    for col in cols:
        # Compute Global Moran's I
        global_moran = esda.moran.Moran(df[f"{col}_std"], weights)
        print(f"Global Moran's I for {col}: {global_moran.I:.4f}, p-value: {global_moran.p_sim:.4f}")
        
        # Compute Local Moran's I
        local_moran = esda.moran.Moran_Local(np.nan_to_num(df[f"{col}_std"]), weights)
        
        # Add Local Moran's I values and p-values to the GeoDataFrame
        df["local_moran"] = local_moran.Is
        df["local_moran_p"] = local_moran.p_sim
        
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
        df["moran_category"] = df.apply(moran_category, axis=1)
        
        # Plot - Local Moran's I categories
        fig, ax = plt.subplots(figsize=(15, 10))
        df.plot(column="moran_category", categorical=True, legend=True, cmap="coolwarm_r", ax=ax)
        plt.title(f"Local Moran's I Categories for {col}")
        plt.savefig(f"{output_file_path}_{col}.jpeg")
        print(f"Plot saved at: {output_file_path}_{col}.jpeg")

if __name__ == '__main__':
    # Import tiger dataset path
    # data_file_path = gpd.datasets.get_path('naturalearth_lowres')
    data_file_path = '/home/y4xxh/Documents/SpatialDaskDB_TIGER/learned_DaskDB/data/arealm/arealm.shp'
    cols = ["ALAND", "AWATER"]
    output_file_path = "/home/y4xxh/Documents/SSTD_GeoSpatial/images/analytics/spatial_autocorr_warg"
    compute_spatial_autocorr(data_file_path, cols, output_file_path)

'''
PySAL (Python Spatial Analysis Library): PySAL offers several methods for spatial autocorrelation analysis, including:
Gamma Statistic: esda.Gamma(y, w[, operation, standardize, ...]) computes the Gamma index for spatial autocorrelation​2​.
Moran Statistics: esda.Moran(y, w[, transformation, ...]) computes Moran's I Global Autocorrelation Statistic​3​.
Bivariate Moran's I: esda.Moran_BV(x, y, w[, transformation, ...]) computes Bivariate Moran's I​4​, and esda.Moran_Local_BV(x, y, w[, ...]) computes Bivariate Local Moran Statistics​5​.
Spatial Pearson: esda.Spatial_Pearson([connectivity, ...]) calculates the Global Spatial Pearson Statistic​6​.
'''