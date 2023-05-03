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

def visualize_polygons(containing_polygon, given_polygon):
    fig, ax = plt.subplots()

    gdf_containing = gpd.GeoDataFrame(geometry=[containing_polygon])
    gdf_containing.plot(ax=ax, color='blue', edgecolor='black')

    # Add the given polygon
    gdf_given = gpd.GeoDataFrame(geometry=[given_polygon])
    gdf_given.plot(ax=ax, color='red', edgecolor='black')

    plt.gca().set_aspect('equal')
    plt.savefig('/home/y4xxh/Documents/SSTD_GeoSpatial/images/topo_reln/poly_inside_another_poly.jpeg', format='jpeg', dpi=300)
    return 

def check_polygons_equality(polygons, given_polygon):
    # Check if the given_polygon is equal to or contained in any of the polygons in the list
    for polygon in polygons:
        if polygon.equals(given_polygon):
            print('The given polygon exists as passed')
            visualize_polygons(polygon, given_polygon)
            return 
        elif polygon.contains(given_polygon):
            print('The given polygon exists within another polygon')
            visualize_polygons(polygon, given_polygon)
            return 

    print("The given polygon does not exist in the csv file.")
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
    given_polygon = sg.Polygon([(0.2, 0.2), (0.8, 0.2), (0.8, 0.8), (0.2, 0.8), (0.2, 0.2)])
    df = refine_polygon_csv(csv_file)
    polygons = df[df['geometry_type'] == "Polygon"]['polygon'].tolist()
    check_polygons_equality(polygons, given_polygon)
