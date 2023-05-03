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

def visualize_polygons(polygons):
    gdf = gpd.GeoDataFrame(geometry=polygons)
    gdf.plot(color='blue', edgecolor='black')

    # Add the given polygon for comparison
    gdf_given = gpd.GeoDataFrame(geometry=[given_polygon])
    gdf_given.plot(color='red', edgecolor='black')

    plt.gca().set_aspect('equal')
    plt.savefig('/home/y4xxh/Documents/SSTD_GeoSpatial/images/check_polygon_existence.jpeg', format='jpeg', dpi=300)
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
    # csv_file = '/home/y4xxh/Documents/SSTD_GeoSpatial/data/test_csv_2.csv'
    # given_polygon = sg.Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
    
    csv_file = '/home/y4xxh/Documents/SSTD_GeoSpatial/data/test_csv_1.csv'
    given_polygon = sg.Polygon([(-122.123024, 37.665946), (-122.126254, 37.664274), (-122.128843, 37.662935), (-122.130619, 37.663369), (-122.13186, 37.661307), (-122.132003, 37.661342), (-122.13212, 37.661315), (-122.132252, 37.661271), (-122.132411, 37.661189), (-122.132902, 37.660947), (-122.133517, 37.660673), (-122.133994, 37.660453), (-122.134492, 37.660201), (-122.135079, 37.659943), (-122.135473, 37.65974), (-122.135902, 37.659553), (-122.136815, 37.659125), (-122.13707, 37.658999), (-122.137236, 37.658922), (-122.137699, 37.658768), (-122.138149, 37.658631), (-122.138335, 37.658543), (-122.138405, 37.658444), (-122.138446, 37.658329), (-122.13846, 37.658225), (-122.138405, 37.658087), (-122.138301, 37.657961), (-122.138163, 37.657818), (-122.138101, 37.657675), (-122.138129, 37.657598), (-122.138232, 37.65751), (-122.138286, 37.657482), (-122.138628, 37.657847), (-122.139005, 37.658258), (-122.139202, 37.658473), (-122.139633, 37.658918), (-122.1406, 37.659969), (-122.141094, 37.660499), (-122.14117, 37.66058), (-122.141352, 37.660776), (-122.141891, 37.661356), (-122.142264, 37.661755), (-122.142649, 37.662168), (-122.143244, 37.662811), (-122.143382, 37.662955), (-122.143759, 37.663352), (-122.14357, 37.663338), (-122.143309, 37.663319), (-122.141961, 37.663223), (-122.141512, 37.663191), (-122.14137, 37.663179), (-122.141306, 37.663174), (-122.140944, 37.663145), (-122.140802, 37.663134), (-122.140784, 37.66312), (-122.14076, 37.663108), (-122.140706, 37.663092), (-122.140671, 37.663086), (-122.140255, 37.663014), (-122.140117, 37.662991), (-122.139961, 37.662961), (-122.139495, 37.662872), (-122.139479, 37.662869), (-122.139338, 37.662864), (-122.13918, 37.662871), (-122.138706, 37.662896), (-122.138549, 37.662905), (-122.138374, 37.662915), (-122.137852, 37.662947), (-122.137678, 37.662958), (-122.137505, 37.662966), (-122.136999, 37.662991), (-122.136987, 37.662992), (-122.136816, 37.66301), (-122.136421, 37.663031), (-122.135236, 37.663096), (-122.134973, 37.663111), (-122.134907, 37.663122), (-122.13485, 37.663153), (-122.134822, 37.663207), (-122.134816, 37.663268), (-122.134816, 37.663278), (-122.134818, 37.663333), (-122.134861, 37.663675), (-122.134878, 37.663807), (-122.134419, 37.664563), (-122.134374, 37.664638), (-122.131835, 37.665003), (-122.13096, 37.66513), (-122.130722, 37.66513), (-122.130012, 37.66513), (-122.129775, 37.665131), (-122.129455, 37.665131), (-122.128495, 37.665131), (-122.128175, 37.665131), (-122.127995, 37.665084), (-122.127749, 37.66502), (-122.127906, 37.665275), (-122.127926, 37.665306), (-122.127963, 37.66545), (-122.128022, 37.665463), (-122.128202, 37.665504), (-122.128262, 37.665518), (-122.128154, 37.665636), (-122.128143, 37.665832), (-122.128102, 37.666561), (-122.128084, 37.6669), (-122.128065, 37.667256), (-122.128019, 37.66727), (-122.127884, 37.667314), (-122.127839, 37.667329), (-122.127839, 37.667358), (-122.127839, 37.667447), (-122.12784, 37.667477), (-122.127742, 37.667635), (-122.127668, 37.667758), (-122.127636, 37.667789), (-122.127615, 37.667796), (-122.127269, 37.667834), (-122.127085, 37.667855), (-122.126972, 37.667861), (-122.126634, 37.66788), (-122.126522, 37.667887), (-122.126399, 37.667893), (-122.126033, 37.667914), (-122.125911, 37.667921), (-122.125815, 37.667925), (-122.125529, 37.667939), (-122.125434, 37.667944), (-122.125284, 37.667954), (-122.124838, 37.667984), (-122.124689, 37.667995), (-122.12454, 37.668004), (-122.124093, 37.668033), (-122.123945, 37.668043), (-122.12341, 37.668071), (-122.12318, 37.668074), (-122.123157, 37.668071), (-122.123137, 37.668067), (-122.12311, 37.668051), (-122.122862, 37.667765), (-122.121947, 37.666533), (-122.122128, 37.66641), (-122.123024, 37.665946)])
    
    df = refine_polygon_csv(csv_file)

    polygons = df[df['geometry_type'] == "Polygon"]['polygon'].tolist()
    check_polygons_equality(polygons, given_polygon)

