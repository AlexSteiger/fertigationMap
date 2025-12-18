# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Alexander Steiger

#!/usr/bin/python3

from datetime import datetime
import os
import shutil
#import json
import requests
import pandas as pd
import geopandas
from sqlalchemy import create_engine, text
import os

## Export the current soil moisture data as a Shapefile
## Upload to Geonode works through copying the new shapefiles in the Geoserver Docker Container
## sudo docker cp /data/current_ru_soil_moisture geoserver4my_geonode:/geoserver_data/data/data/geonode
## Also the Layer and the Store need to be created once (execute "createSMCLayerandStore.py")
postgreSQLTable = ['ru_soil_moisture','bursa_soil_moisture','ugent_soil_moisture']
uni = ["ru","bursa","ugent"]
alchemyEngine   = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres', pool_recycle=3600);

conn = alchemyEngine.connect()

for i in [2]:
  SQL = ("SELECT DISTINCT ON (device_id) "
       " device_id, time, soil_temp, soil_mc, soil_ec, lat, long "
       " FROM %s ORDER BY device_id, time desc;")%(postgreSQLTable[i])
  print(SQL)

  df = pd.read_sql(text(SQL),con=conn)

  #print(df.dtypes)
  #print(df)

  # Transform DataFrame into a GeoDataFrame
  gdf = geopandas.GeoDataFrame(
  df, geometry=geopandas.points_from_xy(df.long, df.lat))

  # Add projection
  gdf.crs = 'epsg:4326'

  # Transform python datetime object to an string (shapefile cant read datetime format)
  gdf['time'] = gdf['time'].dt.strftime("%Y-%m-%dT%H:%M:%S")

  folder = 'outputFiles/current_' + uni[i] + '_soil_moisture'
  file = 'current_' + uni[i] + '_soil_moisture'

  # Create a new directory if it does not exist
  isExist = os.path.exists(folder)
  if not isExist:
    os.makedirs(folder)

  # Export data as shapefile
  print('exporting: '+ file )
  gdf.to_file(folder, driver='ESRI Shapefile')

  ## Upload the data
  # create zip file)
  shutil.make_archive(folder, 'zip', folder)

  #print(gdf.dtypes)

  # Upload to geonode
  try:
    with open(folder +'.zip', 'rb') as f:
      data = f.read()
    url = 'https://geoportal.addferti.eu/geoserver/rest/workspaces/'
    response = requests.put(
      url + 'geonode/datastores/' + file + '/file.shp',
      headers={'Content-type': 'application/zip'},
      data=data,
      verify=False,
      auth=('admin', 'geoserver')
      )
    print(folder +'.zip uploaded' )
  except FileNotFoundError:
    print(folder + " file not found")
conn.close()
