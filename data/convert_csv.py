import pandas as pd

with open('/home/y4xxh/Documents/SSTD_GeoSpatial/data/areawater.csv', 'r') as f:
    file_content = f.read()

lines = file_content.strip().split('\n')
polygons = []

for line in lines:
    id_, geometry = line.split('|')
    polygons.append({'id': int(id_), 'geometry': geometry})

df = pd.DataFrame(polygons)
df.to_csv('/home/y4xxh/Documents/SSTD_GeoSpatial/data/convert_areawater.csv', index=False)
