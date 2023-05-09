import geopandas as gpd
import esda
import libpysal as lps
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

def compute_bivariate_spatial_autocorr(data_file_path, cols, output_file_path):
    assert len(cols) > 0, "At least one column name must be provided."
    
    df = gpd.read_file(data_file_path)
    
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
    
    # If two columns are provided, compute Bivariate Moran's I
    if len(cols) == 2:
        bivariate_moran = esda.moran.Moran_BV(df[f"{cols[0]}_std"], df[f"{cols[1]}_std"], weights)
        print(f"Bivariate Moran's I between {cols[0]} and {cols[1]}: {bivariate_moran.I:.4f}, p-value: {bivariate_moran.p_sim:.4f}")

if __name__ == '__main__':
    data_file_path = gpd.datasets.get_path('naturalearth_lowres')
    cols = ["pop_est", "gdp_md_est"]
    output_file_path = "/home/y4xxh/Documents/SSTD_GeoSpatial/images/analytics/bivariate_spatial_autocorr"
    compute_bivariate_spatial_autocorr(data_file_path, cols, output_file_path)
