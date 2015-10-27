import geojson
import os
import sys
import csv

fw = open('state_district_taluk.csv', 'w')
f = open('india_taluk.geojson', 'r')
json = geojson.loads(f.read())

csvf = csv.writer(fw, doublequote=True, quoting=csv.QUOTE_NONNUMERIC)
csvf.writerow(('State', 'District', 'Taluka'))
features = json['features']
for feature in features:
    properties = feature['properties']
    row = tuple([properties['NAME_'+str(i+1)] for i in range(3)])
    print row
    csvf.writerow(row)

fw.close()
f.close()
