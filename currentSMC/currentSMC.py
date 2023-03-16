#!/usr/bin/python3

import time
import json
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
alchemyEngine   = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres', pool_recycle=3600);
conn = alchemyEngine.connect()

for i in range(0,3):

  SQL = ("SELECT DISTINCT ON (device_id) " 
       " device_id, time, soil_temp, soil_mc, soil_ec, lat, long "
       " FROM %s ORDER BY device_id, time desc;")%(postgreSQLTable[i])
  print(SQL)

  try:
      df_csmc = pd.read_sql(text(SQL),con=conn)
  except:
      print(postgreSQLTable[i] + ' table not available.')
  else:
      folder = 'current_'+ postgreSQLTable[i]
      isExist = os.path.exists(folder)
      if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(folder)
      # Transform DataFrame into a GeoDataFrame
      gdf = geopandas.GeoDataFrame(
      df_csmc, geometry=geopandas.points_from_xy(df_csmc.long, df_csmc.lat))

      # Add projection
      gdf.crs = 'epsg:4326'

      # Transform python datetime object to an string (shapefile cant read datetime format)
      gdf['time'] = gdf['time'].dt.strftime("%Y-%m-%dT%H:%M:%S")

      #print(gdf)
      #gdf.plot()

      # Export data to file
      print('exporting: current_' + postgreSQLTable[i] + '.shp')
      gdf.to_file(folder + '/current_' + postgreSQLTable[i]+'.shp')

conn.close()

# In[ ]:




