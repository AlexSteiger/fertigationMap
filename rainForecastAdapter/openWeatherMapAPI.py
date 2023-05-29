#!/usr/bin/python3

# Python program to find current weather details 
# of an location using openweathermap api


import os
#import time
from datetime import datetime
from sqlalchemy import create_engine, text
import requests, json
import pandas as pd
import geopandas as gpd
 
# curl query:
# curl "https://api.openweathermap.org/data/2.5/onecall?lat=51.509865&lon=-0.118092&exclude=minutely,hourly,alerts&appid=65d4508050d5008b768b660a688651ad" | python -mjson.tool
# Turkey Fields: 40.137442, 28.383499
    
# Enter your API key here
api_key = "65d4508050d5008b768b660a688651ad"
 
# base_url variable to store url
base_url = "https://api.openweathermap.org/data/2.5/onecall?"

# Variables
location = ["Kassow","Karacabey","Les Moeres"]
postgreSQLTable = ["ru_weather","bursa_weather","ugent_weather"]

lon = ["12.079214","28.383499","2.55874"]
lat = ["53.869024","40.137442","51.02979"]

df_out = None

for i in range(0, 3):
  print(postgreSQLTable[i])

  # complete_url variable to store complete url address
  complete_url = base_url + "appid=" + api_key + "&lat=" + lat[i] + "&lon=" + lon[i] + "&exclude=minutely,hourly,alerts"

  # get method of requests module return response object
  response = requests.get(complete_url)

  # convert json format data into python format data:
  x = response.json()

  #print(type(x))
  #print(json.dumps(x, sort_keys=True, indent=4))

  # read the response (x) into a dataframe:
  df = pd.DataFrame(x['daily'])

  # normalize nested 'temp' data:
  df_temp = pd.json_normalize(df['temp'])

  # add rain column, if not exists
  if 'rain' not in df.columns:
    df["rain"] = 0

  # subset of the dataframe:
  df      = df[["dt","rain","humidity","dew_point","wind_speed","clouds","uvi"]]
  df_temp = df_temp['day']
  
  # concat the two dataframes horizontally:
  df = pd.concat([df, df_temp], axis=1)

  # add lat & lon & location
  df['lon'] = lon[i]
  df['lat'] = lat[i]
  df['location'] = location[i]

  # rename time column:
  df = df.rename(columns={"dt":"date","day":"temp"})
 
  # kelvin to celsius
  df['temp'] = df['temp'] - 273.15
  df['dew_point'] = df['dew_point'] - 273.15

  # fill NaN with Null
  df = df.fillna(0)

  # convert from unix time to python datetime:
  df['date'] = pd.to_datetime(df['date'],unit='s')
  df['date'] = df['date'].dt.date

  #print(df.dtypes)
  #print(df)

  ## Upload to local database
  alchemyEngine = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres');
  postgreSQLConnection = alchemyEngine.connect();
  try:
    frame = df.to_sql(postgreSQLTable[i], alchemyEngine, index=False, if_exists='append')
    print("append sucessfull") 
    # Delete duplicates: (ctid > t.ctid -> delete original row ; ctid < t.ctid -> delete new row)
    SQL = ("DELETE FROM {} t WHERE EXISTS (SELECT FROM {} WHERE date = t.date AND ctid > t.ctid);"
           .format(postgreSQLTable[i],postgreSQLTable[i]))
    #print(SQL)
    with alchemyEngine.connect() as con:
      con.execute(text(SQL))
      con.commit()
  except TypeError:
    print("trying to create table", postgreSQLTable[i])
    frame = df.to_sql(postgreSQLTable[i], alchemyEngine, index=False, if_exists='fail');
  finally:
    postgreSQLConnection.close();
  
  # Append the data to df_out
  if df_out is None:
    df_out = pd.DataFrame(columns=list(df.columns))
  df_out = df_out.append(df)

## Save as a shapefile
# Transform python datetime object to an string (shapefile can only read str and numbers)
df_out['date'] = df_out['date'].astype(str)

# Create a new directory if it does not exist
folder = 'current_weather_forecast'
isExist = os.path.exists(folder)
if not isExist:
  os.makedirs(folder)
 
# Transform DataFrame into a GeoDataFrame
gdf = gpd.GeoDataFrame(df_out, geometry=gpd.points_from_xy(df_out.lon, df_out.lat))

# Add projection
gdf.crs = 'epsg:4326'

# Export data to file
print('exporting: current_weather_forecast.shp')
gdf.to_file(folder + '/current_weather_forecast.shp')

# Upload to geonode
url = 'https://geoportal.addferti.eu/geoserver/rest/workspaces/'

try:
  with open(folder + '/' + folder +'.zip', 'rb') as f:
    data = f.read()

  response = requests.put(
    url + 'geonode/datastores/' + folder + '/file.shp',
    headers={'Content-type': 'application/zip'},
    data=data,
    verify=False,
    auth=('admin', 'addferti')
    )
  print(folder +'.zip uploaded' )
except FileNotFoundError:
  print(folder + " file not found")
  

