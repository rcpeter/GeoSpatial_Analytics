import geopandas as gpd
import esda
import libpysal as lps
import matplotlib.pyplot as plt
import numpy as np
from splot.esda import moran_scatterplot, lisa_cluster
import matplotlib.patches as mpatches

def compute_bivariate_spatial_autocorr(data_file_path, output_file_path):
    df = gpd.read_file(data_file_path)
    
    # Filter the dataframe based on the required conditions
    df_filtered = df[(df['pop_est'] > 1000000) & (df['gdp_md_est'] > 3000)]
    
    weights = lps.weights.Queen.from_dataframe(df_filtered)
    weights.transform = 'r'
    
    df_filtered['pop_est_std'] = (df_filtered['pop_est'] - df_filtered['pop_est'].mean()) / df_filtered['pop_est'].std()
    df_filtered['gdp_md_est_std'] = (df_filtered['gdp_md_est'] - df_filtered['gdp_md_est'].mean()) / df_filtered['gdp_md_est'].std()

    # Calculate Moran's I for the standardized columns
    bivariate_moran = esda.moran.Moran_BV(df_filtered['pop_est_std'], df_filtered['gdp_md_est_std'], weights)
    print(f"Bivariate Moran's I: {bivariate_moran.I:.4f}, p-value: {bivariate_moran.p_sim:.4f}")

    # Perform LISA cluster analysis
    local_moran = esda.moran.Moran_Local(np.nan_to_num(df_filtered['pop_est_std']), weights)
    
    # Extract significant locations
    sig = local_moran.p_sim < 0.05
    quadrant = local_moran.q
    
    # Create separate dataframes for each cluster type
    df_high_high = df_filtered.loc[sig & (quadrant == 1)]
    df_low_low = df_filtered.loc[sig & (quadrant == 3)]
    df_low_high = df_filtered.loc[sig & (quadrant == 2)]
    df_high_low = df_filtered.loc[sig & (quadrant == 4)]
    
    # Create a combined plot for all clusters with custom legend
    fig, ax = plt.subplots(figsize=(10, 10))
    df_high_high.plot(ax=ax, color='red', label='High-High Cluster')
    df_low_low.plot(ax=ax, color='blue', label='Low-Low Cluster')
    df_low_high.plot(ax=ax, color='green', label='Low-High Cluster')
    df_high_low.plot(ax=ax, color='orange', label='High-Low Cluster')
    
    # Create custom legend
    legend_handles = [
        mpatches.Patch(color='red', label='HH'),
        mpatches.Patch(color='blue', label='LL'),
        mpatches.Patch(color='green', label='LH'),
        mpatches.Patch(color='orange', label='HL')
    ]
    ax.legend(handles=legend_handles, loc='upper right')
    
    ax.set_title("Bivariate Spatial Autocorrelation Clusters")
    plt.savefig(f"{output_file_path}_combined_clusters.jpeg")
    print(f"Plot saved at: {output_file_path}_combined_clusters.jpeg")

if __name__ == '__main__':
    data_file_path = gpd.datasets.get_path('naturalearth_lowres')
    output_file_path = "/home/y4xxh/Documents/SSTD_GeoSpatial/images/analytics/spatial_autocorrelation/bivariate_spatial_autocorrelation/bvsa"
    compute_bivariate_spatial_autocorr(data_file_path, output_file_path)
