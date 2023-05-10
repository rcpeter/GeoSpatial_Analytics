from spatial_autocorr_warg import compute_spatial_autocorr
import geopandas as gpd

data_file_path = gpd.datasets.get_path('naturalearth_lowres')
cols = ["pop_est", "gdp_md_est"]
output_file_path = "/home/y4xxh/Documents/SSTD_GeoSpatial/images/analytics/spatial_autocorr"

compute_spatial_autocorr(data_file_path, cols, output_file_path)