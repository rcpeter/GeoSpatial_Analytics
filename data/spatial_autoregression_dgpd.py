import geopandas as gpd
import esda
import libpysal as lps
import numpy as np
from dask.distributed import Client
from pysal.model.spreg import ML_Error

# Connect to a Dask cluster
client = Client()

def spatial_autoregression(data, dependent_var, independent_vars):
    # Load data from geopandas dataset
    df = gpd.read_file(data)

    # Ensure the variables exist in the dataframe
    assert dependent_var in df.columns, f"Column {dependent_var} does not exist in the dataframe."
    for var in independent_vars:
        assert var in df.columns, f"Column {var} does not exist in the dataframe."

    # Calculate the spatial weights matrix using Queen contiguity
    weights = lps.weights.Queen.from_dataframe(df)

    # Standardize the variables
    df[f"{dependent_var}_std"] = (df[dependent_var] - df[dependent_var].mean()) / df[dependent_var].std()
    for var in independent_vars:
        df[f"{var}_std"] = (df[var] - df[var].mean()) / df[var].std()

    # Create the regression model
    y = df[f"{dependent_var}_std"].values
    X = df[[f"{var}_std" for var in independent_vars]].values
    model = ML_Error(y, X, w=weights, name_y=f"{dependent_var}", name_x=independent_vars)
    model.fit()
    print(model.summary)

    # Disconnect from the Dask cluster
    client.close()

if __name__ == '__main__':
    dependent_var = "pop_est"
    independent_vars = ["gdp_md_est"]
    data = gpd.datasets.get_path('naturalearth_lowres')
    spatial_autoregression(data, dependent_var, independent_vars)
