#!/usr/bin/python3

import json
import os
import shutil
import io
import zipfile
import requests
import pandas as pd
import ssl
import urllib.request
import geopandas as gpd 
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.colors import LinearSegmentedColormap

uni           = ["ru","ugent","bursa"]
field         = ["Kassow","Blondel-3","Karacabey"]

## Settings
#FK = 30%
#PWP = 8%
depth         = [300, 300, 300] # Irrigation depth/root depth in mm
fill_up_rate  = [0.9, 0.9, 0.9] # Fill up rate in percent of field capacity (e.g. 0.9 = 90%)
MAD_set       = [0.5,0.5,0.5]   # Management allowance depletion in percent (e.g. 0.5 = 50%)
fer_user_rate = [500,500,500]   # Fertilization user rate in l/ha

## read the MZs as Grids (with SensorID and FC) from the GeoPortal
def get_MZ(university,MZtype):
    # Create an SSL context that doesn't verify the certificates.
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    filename = f"{MZtype}_MZ_as_grid_{university}"
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
    print("Transform crs of",filename,"from",old_crs,"to",new_crs)

    return MZ_gdf
    
## read the current Soil Moisture Content from the Database
def get_current_SMC(university):
    postgreSQLTable = university + "_soil_moisture"
    
    alchemyEngine = create_engine(
        'postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres')

    try:
        conn = alchemyEngine.connect()
        print("Reading from Database", postgreSQLTable)
    except:
        print("Unable to connect to Database", university)

    # Select the most current soil moisture data
    SQL1 = "SELECT DISTINCT ON (device_id) device_id, soil_temp, soil_mc, soil_ec, long, lat"
    SQL2 = "FROM"
    SQL3 = postgreSQLTable
    SQL4 = "ORDER BY device_id, time desc;"
    SQL = SQL1 + " " + SQL2 + " " + SQL3 + " " + SQL4

    CMC_df = pd.read_sql_query(text(SQL),conn)
    #print(CMC_df)
    return CMC_df
    
## read the rain forecast
def get_rain(university):
    postgreSQLTable = university + "_weather"
    alchemyEngine = create_engine(
        'postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres')

    try:
        conn = alchemyEngine.connect()
        print("Reading from Database", postgreSQLTable)
    except:
        print("Unable to connect to Database", university[i])

    # Select the most current soil moisture data
    SQL1 = "SELECT SUM(rain) FROM (SELECT rain FROM"
    SQL2 = postgreSQLTable
    SQL3 = "ORDER BY date DESC LIMIT 5) subquery;"
    SQL = SQL1 + " " + SQL2 + " " + SQL3

    rain_df = pd.read_sql_query(text(SQL),conn)
    #print(rain_df)
    return rain_df
    
for i in [1]:
	irr_MZ_gdf  = get_MZ(uni[i],"irr")
	fer_MZ_gdf  = get_MZ(uni[i],"fer")
	rain_df = get_rain(uni[i])
	CMC_df  = get_current_SMC(uni[i])   
    
	#print(rain_df)
	#print(CMC_df)

	## Merge all the data
	MZ_gdf = irr_MZ_gdf.sjoin(fer_MZ_gdf, how='left', op='contains')

	gdf = MZ_gdf.merge(CMC_df, on='device_id')

	# drop some columns (if they exist)
	if 'id_left' in gdf.columns:
		gdf = gdf.drop('id_left', axis=1)
	if 'fid_left' in gdf.columns:
		gdf = gdf.drop('fid_left', axis=1)
	if 'id_right' in gdf.columns:
		gdf = gdf.drop('id_right', axis=1) 
	if 'fid_right' in gdf.columns:
		gdf = gdf.drop('fid_right', axis=1)
	if 'index_right' in gdf.columns:
		gdf = gdf.drop('index_right', axis=1)
	if 'soil_temp' in gdf.columns:
		gdf = gdf.drop('soil_temp', axis=1)
	if 'soil_ec' in gdf.columns:
		gdf = gdf.drop('soil_ec', axis=1)

	if 'long' in gdf.columns:
		gdf = gdf.rename(columns={"long":"sen_long","lat":"sen_lat","soil_mc":"sen_mc"})
		
	gdf = gdf.drop_duplicates()

	## Calculate FC_mm, PWP_mm and CMC_mm
	# FC  [mm] : Field Capacity
	# PWP [mm] : Permanent Wilting Point
	# CMC [mm] : Current soil Moisture Content

	# FC  [mm]  =  FC [%] * VW [g/cm3] * depth [mm] / 100
	# PWP [mm] = PWP [%] * VW [g/cm3] * depth [mm] / 100
	# CMC [mm] = CMC [%] * VW [g/cm3] * depth [mm] / 100

	gdf['FC_mm'] =  gdf['FC']      * gdf['VW_g_cm3'] / 100 * depth[0]
	gdf['PWP_mm'] = gdf['PWP']     * gdf['VW_g_cm3'] / 100 * depth[0]
	gdf['CMC_mm'] = gdf['sen_mc']  * gdf['VW_g_cm3'] / 100 * depth[0]

	## Calculate IN, AW, MAD and WL
	# IN  [mm] : Irrigation Need
	# AW  [mm] : Available Water
	# MAD [mm] : Maximum Allowable Depetion
	# WL  [mm] : Water Left until MAD

	# IN  [mm] = FC [mm] * fill_up_rate [%] * CMC [mm]
	# AW  [mm] = FC [mm] - PWP [mm]
	# MAD [mm] = AW [mm] * MAD [%] + PWP [mm]
	# WL  [mm] = MC [mm] - MAD [mm]

	gdf['IN_mm']  = gdf['FC_mm']  * fill_up_rate[0]  - gdf['CMC_mm']
	gdf['AW_mm']  = gdf['FC_mm']  - gdf['PWP_mm']
	gdf['MAD_mm'] = gdf['PWP_mm'] + gdf['AW_mm']  * MAD_set[0] 
	gdf['WL_mm']  = gdf['CMC_mm'] - gdf['MAD_mm']

	gdf['irr_l_ha'] = gdf['IN_mm'] * 10000
	gdf['fer_l_ha'] = gdf['fer_rate'] * fer_user_rate[0] / 100


	gdf = gdf.round({
	                 "FC_mm":1,"PWP_mm":1,
	                 "CMC_mm":1,"IN_mm":1,
	                 "AW_mm":1,"MAD_mm":1,
	                 "WL_mm":1,"irr_l_ha":0,
	                 "fer_l_ha":0
	                })
	
	## Plots
	fig, axs = plt.subplots(nrows=5, ncols=2, figsize=(12,25))

	colorFC =  LinearSegmentedColormap.from_list("", ['beige','maroon'])
	colorIN =  LinearSegmentedColormap.from_list("", ['lightblue','darkblue'])
	colorSMC = LinearSegmentedColormap.from_list("", ['gray','lightblue','darkblue'])
	colorWL =  LinearSegmentedColormap.from_list("", ['hotpink','lightgreen'])


	gdf.plot(ax=axs[0,0], column="irr_MZ",  cmap='Set3',legend="TRUE", categorical="true")
	gdf.plot(ax=axs[0,1], column="FC_mm",   cmap=colorFC,legend="TRUE", categorical="true")
	gdf.plot(ax=axs[1,0], column="PWP_mm",  cmap=colorFC,legend="TRUE", categorical="true")
	gdf.plot(ax=axs[1,1], column="AW_mm",   cmap=colorFC,legend="TRUE", categorical="true")
	gdf.plot(ax=axs[2,0], column="CMC_mm",  cmap=colorSMC,legend="TRUE", categorical="true")
	gdf.plot(ax=axs[2,1], column="WL_mm",   cmap=colorWL,legend="TRUE", categorical="true")
	gdf.plot(ax=axs[3,0], column="IN_mm",   cmap='Blues',legend="TRUE", categorical="true")
	gdf.plot(ax=axs[3,1], column="irr_l_ha",cmap='Blues',legend="TRUE", categorical="true")
	gdf.plot(ax=axs[4,0], column="fer_rate",cmap='cool',legend="TRUE", categorical="true")
	gdf.plot(ax=axs[4,1], column="fer_l_ha",cmap='cool',legend="TRUE", categorical="true")
	
	axs[0,0].set_title(field[i] + ": Irrigation MZs")
	axs[0,1].set_title(field[i] + ": Field Capacity [mm]")
	axs[1,0].set_title(field[i] + ": Permanent Wilting Point [mm]")
	axs[1,1].set_title(field[i] + ": Available Water (FC - PWP) [mm]")
	axs[2,0].set_title(field[i] + ": Current Soil Moisture Content [mm]")
	axs[2,1].set_title(field[i] + ": Water left until MAD [mm]")
	axs[3,0].set_title(field[i] + ": Irrigation Need for next application [mm]")
	axs[3,1].set_title(field[i] + ": Irrigation Need for next application [l/ha]")
	axs[4,0].set_title(field[i] + ": Fertilization rate in relation to user rate [%]")
	axs[4,1].set_title(field[i] + ": Fertilization amount for next application [l/ha]")	
	
	axs[0,0].axis("off")
	axs[0,1].axis("off")
	axs[1,0].axis("off")
	axs[1,1].axis("off")
	axs[2,0].axis("off")
	axs[2,1].axis("off")
	axs[3,0].axis("off")
	axs[3,1].axis("off")
	axs[4,0].axis("off")
	axs[4,1].axis("off")

	plt.savefig("outputFiles/"+field[i]+".png")
	#plt.show()

	## Export the results
	folder      = "outputFiles/" + uni[i] + "_application_map"
	filename    = "application_map_" + uni[i]
	pathAndName = folder + "/" + filename
	pathAndName

	print(gdf.crs)

	# Create a new directory if it does not exist
	if not os.path.exists(folder):
		os.makedirs(folder)

	# reduce the dataframe
	gdf_out = gdf[['fer_l_ha','irr_l_ha','geometry']] 

	# Export as Shapefile
	gdf_out.to_file(pathAndName+ ".shp", driver='ESRI Shapefile')

	# Export as GeoJson
	#gdf.to_file(pathAndName+".json", driver="GeoJSON")

	## Upload the Geoserver
	# create and open (temporary) zip file)
	shutil.make_archive(filename, 'zip', folder)

	with open(filename + ".zip", 'rb') as file:
		zip_contents = file.read()
		
	#url = 'https://example.com/upload'

	## Export "Water left until MAD" as a matrix
	# Extract centroids of polygon and transform to form: WL_mm, x, y
	gdf_wl = gdf
	gdf_wl['centroid'] = gdf_wl.centroid
	gdf_wl['x'] = gdf_wl['centroid'].x 
	gdf_wl['y'] = gdf_wl['centroid'].y 
	gdf_wl = gdf_wl[['WL_mm','x','y']]
	gdf_wl

	filename = "outputFiles/water_left_" + uni[i] + ".txt"
	gdf_wl.to_csv(filename)
	# Reproject to WGS84 + longlat
	#wl_raster_idw = projectRaster(wl_raster_idw, crs="+proj=longlat +datum=WGS84")

	# Transform Raster to Point matrix with format: "x, y, value"
	#wl_matrix_idw = rasterToPoints(wl_raster_idw, spatial=False)
	#wl_matrix_idw.columns = ["x", "y", "wl"]

	#filename = "water_left_" + university[i] + ".txt"
	#wl_matrix_idw.to_csv(filename, sep=",", index=False)

