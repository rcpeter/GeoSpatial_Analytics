import csv
import sys

csv.field_size_limit(sys.maxsize)

def modify_csv(csv_file, output_file):
    with open(csv_file, 'r') as file:
        reader = csv.reader(file, delimiter='|')
        with open(output_file, 'w', newline='') as output:
            writer = csv.writer(output)
            for row in reader:
                if len(row) >= 2:
                    polygon_str = row[1]

                    # Extract the polygon coordinates
                    coordinates_str = polygon_str.replace('POLYGON ((', '').replace('))', '')
                    coordinates = coordinates_str.split(', ')
                    modified_row = [','.join(coordinate.split()) for coordinate in coordinates]

                    # Write the modified row to the output file
                    writer.writerow(modified_row)

# Example usage
csv_file = '/home/y4xxh/Documents/SSTD_GeoSpatial/data/areawater.csv'
output_file = '/home/y4xxh/Documents/SSTD_GeoSpatial/data/validation_areawater.csv'
modify_csv(csv_file, output_file)
