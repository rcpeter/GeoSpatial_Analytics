'''
Note the below are the subcategories to cover for spatial autoregression:
Medium:
1. Bayesian Estimation
2. Jacobian Matrix based - Maximum Likelihood based (MLE)
If Spatially Autocorrelated
1. Spatial Lag Model - Dependent variable, which is the weighted average of values of dependent variable for neighbouring observation.
2. Spatial Error Model - Dependednt variable, which is the weighted average of values of the error terms for neighbouring observation.

'''

import geopandas as gpd
import libpysal as lps
import pysal as ps
import dask_geopandas as dgp
import matplotlib.pyplot as plt

def compute_spatial_autoregression(data_file_path, output_file_path, column_1, column_2):
    gdf = gpd.read_file(data_file_path)
    # Convert GeoDataFrame to a Dask-GeoDataFrame
    dgdf = dgp.from_geopandas(gdf, npartitions=4)

    # dgdf = dgp.from_dataframe(gdf)

    # Calculate the spatial weights matrix using Queen's Contiguity and standardize rows
    weights = lps.weights.Queen.from_dataframe(dgdf)
    weight.transform = 'r'

    # Create dependent variable (y) and independent variable (x)
    y = dgdf[column_1].to_dask_array(lengths=True)
    x = dgdf[[column_2]].to_dask_array(lengths=True)

    # Fit the spatial lag model or any spreg model of your choice, but proceed further only with this code if there is the columns are spatially autocorrelated
    model = ps.model.spreg.ML_Lag(y, x, w=weights, name_y='pop_est', name_x=['gdp_md_est'])

    # Print the summary of the model
    print(model.summary)

    # Plot the observed vs predicted values
    plt.figure(figsize=(9, 9))
    plt.scatter(y, model.predy, color='blue')
    plt.plot([y.min(), y.max()], [y.min(), y.max()], color='red')
    plt.title('Observed vs Predicted Values: Spatial Lag Model')
    plt.xlabel('Observed Values')
    plt.ylabel('Predicted Values')
    plt.savefig(f"{output_file_path}_observed_vs_predicted.jpeg")
    print(f"Plot saved at: {output_file_path}_observed_vs_predicted.jpeg")

if __name__ == '__main__':
    data_file_path = gpd.datasets.get_path('naturalearth_lowres')
    output_file_path = "/home/y4xxh/Documents/SSTD_GeoSpatial/images/spatial_autoregression/spatial_lag_model"
    column_1 = 'pop_est'
    column_2 = 'gdp_md_est'
    compute_spatial_autoregression(data_file_path, output_file_path, column_1, column_2)