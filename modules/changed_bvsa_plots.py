import geopandas as gpd
import esda
import libpysal as lps
import matplotlib.pyplot as plt
import numpy as np
from splot.esda import moran_scatterplot, lisa_cluster

def compute_bivariate_spatial_autocorr(data_file_path, cols, output_file_path):
    assert len(cols) > 0, "At least one column name must be provided."
    
    df = gpd.read_file(data_file_path)
    
    # Filter the dataframe based on pop_est and gdp_md_est
    # df = df[(df['continent'] == 'Europe') | (df['continent'] == 'Asia')]
    df = df[(df['pop_est'] > 100) | (df['gdp_md_est'] < 16)]

    # Add check for number of rows
    if len(df) < 2:
        raise ValueError("Not enough rows in the dataframe after filtering. Please adjust the filtering conditions.")
    
    for col in cols:
        assert col in df.columns, f"Column {col} does not exist in the dataframe."
    
    weights = lps.weights.Queen.from_dataframe(df)
    weights.transform = 'r'
    
    for col in cols:
        df[f"{col}_std"] = (df[col] - df[col].mean()) / df[col].std()
    
    for col in cols:
        global_moran = esda.moran.Moran(df[f"{col}_std"], weights)
        print(f"Global Moran's I for {col}: {global_moran.I:.4f}, p-value: {global_moran.p_sim:.4f}")
        
        local_moran = esda.moran.Moran_Local(np.nan_to_num(df[f"{col}_std"]), weights)

        fig, ax = moran_scatterplot(global_moran, aspect_equal=True)
        plt.title(f"Global Moran's I Scatterplot for {col}")
        plt.savefig(f"{output_file_path}_global_moran_{col}.jpeg")
        print(f"Plot saved at: {output_file_path}_global_moran_{col}.jpeg")
        
        # Extract significant locations
        sig = local_moran.p_sim < 0.05
        # Create a new column for quadrant values
        df["quadrant"] = local_moran.q
        # Filter the data to include only significant locations and valid quadrant values (1 to 4)
        df_sig = df.loc[sig & df["quadrant"].isin([1, 2, 3, 4])]

        # Create a new weights matrix for the filtered data
        weights_sig = lps.weights.Queen.from_dataframe(df_sig)
        weights_sig.transform = 'r'
                
        # Recalculate local_moran for the filtered data
        local_moran_sig = esda.moran.Moran_Local(np.nan_to_num(df_sig[f"{col}_std"]), weights_sig)
                
        fig, ax = lisa_cluster(local_moran_sig, df_sig, p=0.05) # Plotting LISA cluster map with significant locations only

        plt.title(f"Local Moran's I LISA Cluster Map for {col}")
        plt.savefig(f"{output_file_path}_local_moran_{col}.jpeg")
        print(f"Plot saved at: {output_file_path}_local_moran_{col}.jpeg")
    
    if len(cols) == 2:
        bivariate_moran = esda.moran.Moran_BV(df[f"{cols[0]}_std"], df[f"{cols[1]}_std"], weights)
        print(f"Bivariate Moran's I between {cols[0]} and {cols[1]}: {bivariate_moran.I:.4f}, p-value: {bivariate_moran.p_sim:.4f}")

if __name__ == '__main__':
    data_file_path = gpd.datasets.get_path('naturalearth_lowres')
    cols = ["pop_est", "gdp_md_est"]
    output_file_path = "/home/y4xxh/Documents/SSTD_GeoSpatial/images/analytics/spatial_autocorrelation/bivariate_spatial_autocorrelation/changed_bvsa"
    compute_bivariate_spatial_autocorr(data_file_path, cols, output_file_path)
