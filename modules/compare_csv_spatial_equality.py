'''
compare two csv to check if there are spatially equal polygons, and plot them
'''

import random
import rtree
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

def visualize_polygons(matching_polygons_1, matching_polygons_2):
    r = random.random()
    g = random.random()
    b = random.random()

    gdf1 = gpd.GeoDataFrame(geometry=matching_polygons_1)
    gdf1.plot(color=(r,g,b), edgecolor='black')

    gdf2 = gpd.GeoDataFrame(geometry=matching_polygons_2)
    gdf1.plot(color=(r,g,b), edgecolor='black')

    plt.gca().set_aspect('equal')
    plt.savefig('/home/y4xxh/Documents/SSTD_GeoSpatial/images/topo_reln/compare_csv_spatial_equality.jpeg', format='jpeg', dpi=300)
    return 

def check_polygons_equality(polygons1, polygons2):
    # Create an R-tree index for polygons2
    idx = rtree.index.Index()
    for i, polygon in enumerate(polygons2):
        idx.insert(i, polygon.bounds)

    matching_polygons_1 = []
    matching_polygons_2 = []
    for i, polygon in enumerate(polygons1):
        # Query the R-tree: it returns an iterator over the indices of the polygons that overlap with the bounding box of the current polygon
        potential_matches = [j for j in idx.intersection(polygon.bounds)]
        for j in potential_matches:
            if polygon.equals(polygons2[j]):
                matching_polygons_1.append(polygon)
                matching_polygons_2.append(polygons2[j])
                break  # no need to check other polygons if we found a match

    if matching_polygons_1:
        visualize_polygons(matching_polygons_1, matching_polygons_2)
        print('Saving polygons as they do exist')
    else:
        print("There are no matching polygons in the csv files.")
    return 

def refine_polygon_csv(csv_file):
    df = dd.read_csv(csv_file, delimiter='|', names=['id', 'polygon'], header=None)
    df['polygon'] = df['polygon'].apply(lambda x: x.replace('POLYGON ((', '').replace('))', '').replace(')', '').replace('(', ''), meta=('polygon', 'object'))
    df['points'] = df['polygon'].apply(lambda x: [tuple(map(float, point.split())) for point in x.split(',')], meta=('points', 'object'))
    df['geometry_type'] = df['points'].apply(determine_geometry, meta=('geometry_type', 'object'))
    df['polygon'] = df.apply(lambda row: sg.Polygon(row['points']) if row['geometry_type'] == "Polygon" else None, axis=1, meta=('polygon', 'object'))
    
    return df.compute()

if __name__ == '__main__':
    csv_file1 = '/home/y4xxh/Documents/SSTD_GeoSpatial/data/test_csv_1.csv'
    csv_file2 = '/home/y4xxh/Documents/SSTD_GeoSpatial/data/test_csv_2.csv'
    df1 = refine_polygon_csv(csv_file1)
    df2 = refine_polygon_csv(csv_file2)
    polygons1 = df1[df1['geometry_type'] == "Polygon"]['polygon'].tolist()
    polygons2 = df2[df2['geometry_type'] == "Polygon"]['polygon'].tolist()
    check_polygons_equality(polygons1, polygons2)
   
