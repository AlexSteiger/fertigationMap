#!/usr/bin/python3

import os
import shutil
import io
import requests
import pandas as pd
import numpy as np
import ssl
import urllib.request
import geopandas as gpd 
from datetime import date
from sqlalchemy import create_engine, text

## read the MZs as Grids (with SensorID and FC) from the GeoPortal
def get_MZ(*args):
    #print("******************* MZ_gdf *******************")
    # Create an SSL context that doesn't verify the certificates.
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    if len(args) == 3:
        university, MZtype,field = args
        filename = f"{MZtype}_mz_as_grid_{university}_{field}"
        print(filename)

    if len(args) == 1:
        university = args[0]
        filename = f"{university}_mz_as_grid"
        print(filename)
        
    URL1 = "http://geoportal.addferti.eu/geoserver/ows"
    URL2 = "?service=WFS&version=1.0.0&request=GetFeature&typename=geonode%3A"
    URL4 = filename
    URL5 = "&outputFormat=json"
    URL = URL1 + URL2 + URL4 + URL5
    #print(filename)
    print(URL)
    # Download the file.
    with urllib.request.urlopen(URL, context=ssl_context) as response, open(f"outputFiles/{filename}.geojson", "wb") as out_file:
        data = response.read()
        out_file.write(data)
   
    # Read the data into a geopandas dataframe 
    MZ_gdf = gpd.read_file(f"outputFiles/{filename}.geojson")
    
    old_crs = MZ_gdf.crs.to_epsg()
    
    MZ_gdf = MZ_gdf.to_crs(4326)
    
    new_crs = MZ_gdf.crs.to_epsg()
    #print("Transform crs of",filename,"from",old_crs,"to",new_crs)
    #print(MZ_gdf.columns.values)
    MZ_gdf.plot()
    return MZ_gdf
    
## read the rain forecast
def get_rain(university):
    #print("******************* rain_df *******************")
    # Create an SSL context that doesn't verify the certificates.
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    filename = f"current_{university}_weather_forecast"
    URL1 = "http://geoportal.addferti.eu/geoserver/ows"
    URL2 = "?service=WFS&version=1.0.0&request=GetFeature&typename=geonode%3A"
    URL4 = filename
    URL5 = "&outputFormat=json"
    URL = URL1 + URL2 + URL4 + URL5
    
    # Select the most current soil moisture data
    #SQL1 = "SELECT SUM(rain) FROM (SELECT rain FROM"
    #SQL2 = postgreSQLTable
    #SQL3 = "ORDER BY date DESC LIMIT 7) subquery;"
    #SQL = SQL1 + " " + SQL2 + " " + SQL3

    with urllib.request.urlopen(URL, context=ssl_context) as response, open(f"outputFiles/{filename}.geojson", "wb") as out_file:
        data = response.read()
        out_file.write(data)
        
    rain_gdf = gpd.read_file(f"outputFiles/{filename}.geojson")
    rain_gdf = rain_gdf.drop('id', axis=1)
    rain_gdf['date'] = pd.to_datetime(rain_gdf['date'])

    
    #print("CRS of",filename,": ",rain_gdf.crs.to_epsg())
    rain_df = rain_gdf.drop('geometry',axis=1) #tranform gdf to a df
    #print(rain_df.columns.values)
    return rain_df

## read the current Soil Moisture Content from the Database
def get_current_SMC(university):
    #print("******************* Soil Moisture Sensors *******************")
    # Create an SSL context that doesn't verify the certificates.
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    filename = f"current_{university}_soil_moisture"
    URL1 = "http://geoportal.addferti.eu/geoserver/ows"
    URL2 = "?service=WFS&version=1.0.0&request=GetFeature&typename=geonode%3A"
    URL4 = filename
    URL5 = "&outputFormat=json"
    URL = URL1 + URL2 + URL4 + URL5

    with urllib.request.urlopen(URL, context=ssl_context) as response, open(f"outputFiles/{filename}.geojson", "wb") as out_file:
        data = response.read()
        out_file.write(data)

    CMC_gdf = gpd.read_file(f"outputFiles/{filename}.geojson")
    CMC_gdf = CMC_gdf.drop('id', axis=1)
    
    if 'SensorID' in CMC_gdf.columns:
        CMC_gdf = CMC_gdf.rename(columns={"SensorID":"device_id"})
    
    #print("CRS of",filename,": ",CMC_gdf.crs.to_epsg())
    #print(CMC_gdf.columns.values)
    return CMC_gdf

def get_settings(uni):
    #print("******************* settings *******************")
    # Create an SSL context that doesn't verify the certificates.
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    filename = f"{uni}_settings"
    URL1 = "http://geoportal.addferti.eu/geoserver/ows"
    URL2 = "?service=WFS&version=1.0.0&request=GetFeature&typename=geonode%3A"
    URL4 = filename
    URL5 = "&outputFormat=json"
    URL = URL1 + URL2 + URL4 + URL5

    with urllib.request.urlopen(URL, context=ssl_context) as response, open(f"outputFiles/{filename}.geojson", "wb") as out_file:
        data = response.read()
        out_file.write(data)

    settings = gpd.read_file(f"outputFiles/{filename}.geojson")   
    
    return settings