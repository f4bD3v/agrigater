""" 
  script derived from:
  http://geospatialpython.com/2013/07/shapefile-to-geojson.html
  geojson specs:
  http://geojson.org
"""

import shapefile
from json import dumps
from os import path

def convert_shp(data_dir, admin_level, infile):
  reader = shapefile.Reader(path.join(data_dir, infile+'.shp'))
  fields = reader.fields[1:]
  field_names = [field[0] for field in fields]
  buffer = []
  for sr in reader.shapeRecords():
    # remove empty byte fields
    record = [field.decode('utf-8').strip() if not (isinstance(field, int) or isinstance(field, str)) else field for field in sr.record]
    atr = dict(zip(field_names, record))
    geom = sr.shape.__geo_interface__
    buffer.append(dict(type="Feature", geometry=geom, properties=atr)) 

  # write the GeoJSON file
  geojson = open(path.join(data_dir, 'Indian_'+admin_level+'.geojson'), "w")
  geojson.write(dumps({"type": "FeatureCollection", "features": buffer}, indent=2) + "\n")
  geojson.close()

# read 3rd level -> tehsil/taluk boundaries

def main():
  data_dir = '../../../data/admin/geo/'

  country_infile = 'IND_adm0'
  convert_shp(data_dir, 'country', country_infile)
  state_infile = 'IND_adm1'
  convert_shp(data_dir, 'states', state_infile)
  district_infile = 'IND_adm2'
  convert_shp(data_dir, 'districts', district_infile)
  taluk_infile = 'IND_adm3'
  convert_shp(data_dir, 'taluks', taluk_infile)
  return
  
if __name__ == "__main__":
  main()
