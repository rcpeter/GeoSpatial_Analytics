'''
If polygon within the csv file passed is spatially equal to any other polygon then it plots the polygon
'''

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

def visualize_polygons(matching_polygons):
    gdf = gpd.GeoDataFrame(geometry=matching_polygons)
    gdf.plot(color='blue', edgecolor='black')

    plt.gca().set_aspect('equal')
    plt.savefig('/home/y4xxh/Documents/SSTD_GeoSpatial/images/topo_reln/poly_spatial_equal_poly.jpeg', format='jpeg', dpi=300)
    return 

import rtree

def check_polygons_equality(polygons):
    # Create an R-tree index
    idx = rtree.index.Index()
    for i, polygon in enumerate(polygons):
        idx.insert(i, polygon.bounds)

    matching_polygons = []
    for i, polygon in enumerate(polygons):
        # Query the R-tree: it returns an iterator over the indices of the polygons that overlap with the bounding box of the current polygon
        potential_matches = [j for j in idx.intersection(polygon.bounds) if j != i]
        for j in potential_matches:
            if polygon.equals(polygons[j]):
                matching_polygons.append(polygon)
                break  # no need to check other polygons if we found a match

    if matching_polygons:
        visualize_polygons(matching_polygons)
        print('Saving polygons as they do exist')
    else:
        print("There are no matching polygons in the csv file.")
    return 

def refine_polygon_csv(csv_file):
    df = dd.read_csv(csv_file, delimiter='|', names=['id', 'polygon'], header=None)
    df['polygon'] = df['polygon'].apply(lambda x: x.replace('POLYGON ((', '').replace('))', '').replace(')', '').replace('(', ''), meta=('polygon', 'object'))
    df['points'] = df['polygon'].apply(lambda x: [tuple(map(float, point.split())) for point in x.split(',')], meta=('points', 'object'))
    df['geometry_type'] = df['points'].apply(determine_geometry, meta=('geometry_type', 'object'))
    df['polygon'] = df.apply(lambda row: sg.Polygon(row['points']) if row['geometry_type'] == "Polygon" else None, axis=1, meta=('polygon', 'object'))
    
    return df.compute()

if __name__ == '__main__':
    csv_file = '/home/y4xxh/Documents/SSTD_GeoSpatial/data/test_csv_2.csv'
    df = refine_polygon_csv(csv_file)
    polygons = df[df['geometry_type'] == "Polygon"]['polygon'].tolist()
    check_polygons_equality(polygons)
