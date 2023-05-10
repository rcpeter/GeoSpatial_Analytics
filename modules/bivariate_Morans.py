'''
FAQ:
Bivariate Moran's I is a measure of spatial autocorerlation for two variables - pop_est and gdp_md_est. The measure ranges from -ve (indicating perfect dispersion), 0 (no correlation) to +ve (high correlation).

Result from code:
Bivariate Moran's I between pop_est and gdp_md_est: 0.0868, p-value: 0.0440

The Bivariate Moran's I value of 0.0868 suggests a weak but positive spatial correlation between the two variables. This means areas with high (or low) values for population estimate tend to be near areas with high (or low) values for GDP estimate. However, as the value is closer to 0, the correlation isn't particularly strong.

p-value less than 0.05 is considered statistically significant. A p-value of 0.0440 suggests that there is a statistically significant (albeit weak) spatial correlation between the population estimate and the GDP estimate. In other words, the observed spatial pattern is unlikely to be due to random chance.

Resulting images:
1. bvsa_local_moran_gdp_md_est.jpeg
2. bvsa_local_moran_pop_est.jpeg
3. bvsa_global_moran_gdp_md_est.jpeg
4. bvsa_global_moran_pop_est.jpeg

1. bvsa_local_moran_gdp_md_est.jpeg: Plot of Local Moran's I for the variable gdp_md_est - Local Moran's I measure of spatial autocorrelation that provides insight into local spatial patterns. In other terms, whether areas with similar GDP estimates are clustered together (high-high) or (low-low), or whether there are areas that are different from their neighbours (high-low) or (low-high)

2. bvsa_local_moran_pop_est.jpeg: Plot of Local Moran's I for the variable pop_est - Local Moran's I measure of spatial autocorrelation that provides whether areas with similar population estimates are clustered together, or wheter there are areas different from the neighbours.

3. bvsa_global_moran_gdp_md_est.jpeg: Plot of Local Moran's I for the variable gdp_md_est. Global Moran's I is a measure of spatial autocorrelation that provides a single summary statistic for the whole dataset, indicating whether the overall pattern shows clustering (Postive Moran's I), randomness (Around 0 Moran's I) or dispersion (Negative Moran's I).

4. bvsa_global_moran_pop_est.jpeg: Plot of Global Moran's I, but for pop_est. Provides a summary of overall spatial autocorrelation in population estimates.
'''

import geopandas as gpd
import esda
import libpysal as lps
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from splot.esda import moran_scatterplot, plot_local_autocorrelation
from matplotlib import colors

def compute_bivariate_spatial_autocorr(data_file_path, cols, output_file_path):
    assert len(cols) > 0, "At least one column name must be provided."
    
    df = gpd.read_file(data_file_path)
    
    # Ensure the columns exist in the dataframe
    for col in cols:
        assert col in df.columns, f"Column {col} does not exist in the dataframe."
    
    # Calculate the spatial weights matrix using Queen contiguity
    weights = lps.weights.Queen.from_dataframe(df)
    weights.transform = 'r'
    
    # Standardize the provided columns
    for col in cols:
        df[f"{col}_std"] = (df[col] - df[col].mean()) / df[col].std()
    
    for col in cols:
        # Compute Global Moran's I
        global_moran = esda.moran.Moran(df[f"{col}_std"], weights)
        print(f"Global Moran's I for {col}: {global_moran.I:.4f}, p-value: {global_moran.p_sim:.4f}")
        
        # Compute Local Moran's I
        local_moran = esda.moran.Moran_Local(np.nan_to_num(df[f"{col}_std"]), weights)
        
        # Plot - Global Moran Scatterplot
        fig, ax = moran_scatterplot(global_moran, aspect_equal=True)
        plt.title(f"Global Moran's I Scatterplot for {col}")
        plt.savefig(f"{output_file_path}_global_moran_{col}.jpeg")
        print(f"Plot saved at: {output_file_path}_global_moran_{col}.jpeg")
        
        # Plot - Local Moran's I Cluster Map
        fig, ax = plt.subplots(figsize=(10,10))
        plot_local_autocorrelation(local_moran, df, f"{col}_std")
        plt.title(f"Local Moran's I Cluster Map for {col}")
        plt.savefig(f"{output_file_path}_local_moran_{col}.jpeg")
        print(f"Plot saved at: {output_file_path}_local_moran_{col}.jpeg")
    
    # If two columns are provided, compute Bivariate Moran's I
    if len(cols) == 2:
        bivariate_moran = esda.moran.Moran_BV(df[f"{cols[0]}_std"], df[f"{cols[1]}_std"], weights)
        print(f"Bivariate Moran's I between {cols[0]} and {cols[1]}: {bivariate_moran.I:.4f}, p-value: {bivariate_moran.p_sim:.4f}")

if __name__ == '__main__':
    data_file_path = gpd.datasets.get_path('naturalearth_lowres')
    cols = ["pop_est", "gdp_md_est"]
    output_file_path = "/home/y4xxh/Documents/SSTD_GeoSpatial/images/analytics/spatial_autocorrelation/bivariate_spatial_autocorrelation/bvsa"
    compute_bivariate_spatial_autocorr(data_file_path, cols, output_file_path)

