import sys
import csv
import dask.dataframe as dd
import dask.delayed as delayed
import shapely.geometry as sg
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon

csv.field_size_limit(sys.maxsize)

def determine_geometry(points):
    line = sg.LineString(points)
    if line.is_ring:
        return "Polygon"
    else:
        return "Line"

def pysal_polygons(gdf):
    # Use PySAL to create a map
    fig, ax = plt.subplots(1, figsize=(10, 6))
    gdf.plot(cmap='viridis', linewidth=0.8, ax=ax, edgecolor='0.8')  # Removed column='geometry' argument

    plt.savefig('/home/y4xxh/Documents/SSTD_GeoSpatial/images/check_polygon_existence.jpeg', format='jpeg', dpi=300)

def visualize_polygons(polygons):
    gdf = gpd.GeoDataFrame(geometry=polygons)
    pysal_polygons(gdf)
    return 

def check_polygons_equality(polygons, given_polygon):
    # Check if the given_polygon is equal to any of the polygons in the list
    for polygon in polygons:
        if polygon.equals(given_polygon):
            visualize_polygons(polygons)
            print('Saving polygon existence as it does exist')
            return 
            
    print("The given polygon does not exist in the csv file.")

def refine_polygon_csv(csv_file):
    df = dd.read_csv(csv_file, delimiter='|', names=['id', 'polygon'], header=None)
    df['polygon'] = df['polygon'].apply(lambda x: x.replace('POLYGON ((', '').replace('))', '').replace(')', '').replace('(', ''), meta=('polygon', 'object'))
    df['points'] = df['polygon'].apply(lambda x: [tuple(map(float, point.split())) for point in x.split(',')], meta=('points', 'object'))
    df['geometry_type'] = df['points'].apply(determine_geometry, meta=('geometry_type', 'object'))
    df['polygon'] = df.apply(lambda row: sg.Polygon(row['points']) if row['geometry_type'] == "Polygon" else None, axis=1, meta=('polygon', 'object'))
    
    return df.compute()

if __name__ == '__main__':
    csv_file = '/home/y4xxh/Documents/SSTD_GeoSpatial/data/test_csv_2.csv'
    given_polygon = sg.Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])

    df = refine_polygon_csv(csv_file)
    polygons = df[df['geometry_type'] == "Polygon"]['polygon'].tolist()

    check_polygons_equality(polygons, given_polygon)
