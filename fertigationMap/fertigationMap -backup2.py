#!/usr/bin/python3
import fertigationMapfunctions as fMf

import json
import os
import shutil
import io
import zipfile
import requests
import pandas as pd
import numpy as np
from datetime import date
import ssl
import urllib.request
import geopandas as gpd 
from sqlalchemy import create_engine, text
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.colors import LinearSegmentedColormap

uni           = ["ugent","ugent","ugent"]
field         = ["grandplane","mortier","galloutibout"]
fieldname     = ["Grandplane","Mortier","Gallou Tibout"]

## Settings (order: ru, ugent, bursa)
#FK = 30%
#PWP = 8%
depth         = [200, 200, 200] # Irrigation depth/root depth in mm
fill_up_rate  = [0.9, 0.9, 0.9] # Fill up rate in percent of field capacity (e.g. 0.9 = 90%)
MAD_set       = [0.5,0.5,0.5]   # Management allowance depletion in percent (e.g. 0.5 = 50%)
irr_user_rate = [30,30,30]
fer_user_rate = [500,500,500]   # Fertilization user rate in l/ha

# In the MZs is either the column: 
# "fertility" (L, ML, M, MH, H)             --> fertility of each MZ
# "fer_l_ha"  (eg. 209, 384, 732, 907, 558) --> fixed rate for each MZ

# Fertility related to fertilization rates (eg. L (Low fertility) = 1.5*fer_rate)
fer_rates_rel = {"ru":   {"L" : 1.5,"ML": 1.25,"M" : 1.0,"MH": 0.75,"H" : 0.5},
                 "ugent":{"L" : 1.5,"ML": 1.25,"M" : 1.0,"MH": 0.75,"H" : 0.5},
                 "bursa":{"L" : 1.5,"ML": 1.25,"M" : 1.0,"MH": 0.75,"H" : 0.5}} #L = Low fertility

# Alternatively the final fertilization rate can also be set here:
use_absolute_rates = [False,False,True]
fer_rates_abs = {"ru":   {1 : 200, 2: 300, 3 : 550, 4: 732, 5 : 906, 99: 555},
                 "ugent":{1 : 200, 2: 300, 3 : 550, 4: 732, 5 : 906, 99: 555},
                 "bursa":{1 : 209, 2: 384, 3 : 732, 4: 907, 5 : 558, 99: 558}}

# Priorities: "use_absolute_rates" > "fer_l_ha" from MZ > "fertility" from MZ

Irr_min        = [10,10,10]
Irr_max        = [50,50,50]
bins           = "no"

settings_df     = fMf.get_settings('ugent')
print("settings:")
print(settings_df)
    
MZ_gdf = None
fer_MZ_gdf = None

# Loop through specific indices in the array [0,1,2] = [ru,ugent,bursa]
for  i  in  [0,1,2]:
    #print(field[i])    
    irr_MZ_gdf = None
    MZ_gdf = None
    fer_MZ_gdf = None
    irr_MZ_gdf    =  fMf.get_MZ(uni[i],"irr",field[i])
    #fer_MZ_gdf    =  fMf.get_MZ(uni[i],"fer")
    #MZ_gdf        =  fMf.get_MZ(uni[i])
    rain_df       =  fMf.get_rain(uni[i])
    CMC_gdf       =  fMf.get_current_SMC(uni[i])  

    #irr_MZ_gdf    =  fMf.get_MZ("pisa","irr")        
    #rain_df       =  fMf.get_rain("ru")
    #CMC_gdf       =  fMf.get_current_SMC("pisa")  

    depth = settings_df['root_depth'].tolist()
    fill_up_rate = settings_df['fill_up_ra'].tolist()
    Irr_min = settings_df['irr_min'].tolist()
    Irr_max = settings_df['irr_max'].tolist()
    
    ## Paths for output files
    folder      = "outputFiles/" + field[i] +  "_application_map"
    filename    = "application_map_" + field[i]
    pathAndName = folder + "/" + filename

#INSERT INTO ugent_soil_moisture (device_id, time, soil_ec, soil_temp, soil_mc, lat, long)
#VALUES ('uni-gent-22', '2024-06-25 15:43:03+00', 763, 21.11, 41.59, 50.98970247, 2.544487848);

    ## Merge all the data
    if fer_MZ_gdf is not None:
        print("join irr_MZ and fer_MZ")
        MZ_gdf = irr_MZ_gdf.sjoin(fer_MZ_gdf, how='left', op='contains')
        
    if MZ_gdf is None:
        MZ_gdf = irr_MZ_gdf
        
    CMC_df = CMC_gdf.drop('geometry',axis=1) #tranform gdf to a df
    
    # Left attribute join on 'device_id'
    gdf = MZ_gdf.merge(CMC_df, on='device_id', how='left')

    # drop some columns (if they exist)
    if 'id' in gdf.columns:
        gdf = gdf.drop('id', axis=1)        
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
    if 'soil_gdfemp' in gdf.columns:
        gdf = gdf.drop('soil_temp', axis=1)
    if 'soil_ec' in gdf.columns:
        gdf = gdf.drop('soil_ec', axis=1)
    if 'ogc_fid' in gdf.columns:
        gdf = gdf.drop('ogc_fid', axis=1)
    if 'fid' in gdf.columns:
        print("yes")
        gdf = gdf.drop('fid', axis=1)

    gdf['fc'] = pd.to_numeric(gdf['fc'])
    gdf['pwp'] = pd.to_numeric(gdf['pwp'])
    gdf['vw_g_cm3'] = pd.to_numeric(gdf['vw_g_cm3'])
    
    # rename the columns
    gdf = gdf.rename(columns={"soil_mc":"smc"})
    gdf = gdf.drop_duplicates()
    print(gdf.columns.values)
    #print(CMC_df.columns.values)
    #print(gdf.head())

    ## Calculate FC_mm, PWP_mm and CMC_mm
    # FC  [mm] : Field Capacity
    # PWP [mm] : Permanent Wilting Point
    # CMC [mm] : Current soil Moisture Content

    # FC  [mm] =  FC [%] * VW [g/cm3] * depth [mm] / 100
    # PWP [mm] = PWP [%] * VW [g/cm3] * depth [mm] / 100
    # CMC [mm] = CMC [%] *            * depth [mm] / 100
    print("Settings:  Depth: " + str(depth[i]) + ", Fill up rate: " + str(fill_up_rate[i]))

    gdf['FC_mm'] =  gdf['fc']      * gdf['vw_g_cm3'] / 100 * depth[i]
    gdf['PWP_mm'] = gdf['pwp']     * gdf['vw_g_cm3'] / 100 * depth[i]
    gdf['CMC_mm'] = gdf['smc']                       / 100 * depth[i]
    # alternatively for using the predicted SMC:
    #gdf['CMC_mm'] = gdf['smc_pred']                       / 100 * depth[i]

    ## Calculate IN, AW, MAD and WL
    # IN  [mm] : Irrigation Need
    # AW  [mm] : Available Water
    # MAD [mm] : Maximum Allowable Depetion
    # WL  [mm] : Water Left until MAD

    # IN  [mm] = FC [mm] * fill_up_rate [%] - CMC [mm] - rain [mm]
    # AW  [mm] = FC [mm] - PWP [mm]
    # MAD [mm] = AW [mm] * MAD [%] + PWP [mm]
    # WL  [mm] = MC [mm] - MAD [mm]

    gdf['IN_mm']  = gdf['FC_mm']  * fill_up_rate[i]  - gdf['CMC_mm']
    gdf['AW_mm']  = gdf['FC_mm']  - gdf['PWP_mm']
    gdf['MAD_mm'] = gdf['PWP_mm'] + gdf['AW_mm']  * MAD_set[0] 
    gdf['WL_mm']  = gdf['CMC_mm'] - gdf['MAD_mm']

    # set min and max for the IN
    gdf['Irr_mm'] = gdf['IN_mm']
    for index, value in gdf["Irr_mm"].items():
        if value > Irr_max[i]:
            gdf.at[index, "Irr_mm"] = Irr_max[i]
        if value < Irr_min[i]:
            gdf.at[index, "Irr_mm"] = Irr_min[i] 

    # categorize the IN_mm data into 5 categories (from IN_min to IN_max)
    IN_range = gdf["Irr_mm"].max() - gdf["Irr_mm"].min()
    if bins == "yes": 
        for index, value in gdf["Irr_mm"].items():
            if value < gdf["Irr_mm"].min() + IN_range * 1/8:
                gdf.at[index, "Irr_mm"] =  gdf["Irr_mm"].min()
            if value > gdf["Irr_mm"].min() + IN_range * 1/8 and value < gdf["Irr_mm"].min() + IN_range * 3/8:
                gdf.at[index, "Irr_mm"] =  gdf["Irr_mm"].min() + IN_range * 2/8
            if value > gdf["Irr_mm"].min() + IN_range * 3/8 and value < gdf["Irr_mm"].min() + IN_range * 5/8:
                gdf.at[index, "Irr_mm"] =  gdf["Irr_mm"].min() + IN_range * 4/8
            if value > gdf["Irr_mm"].min() + IN_range * 5/8 and value < gdf["Irr_mm"].min() + IN_range * 7/8:
                gdf.at[index, "Irr_mm"] =  gdf["Irr_mm"].min() + IN_range * 6/8
            if value > gdf["Irr_mm"].min() + IN_range * 7/8:
                gdf.at[index, "Irr_mm"] =  gdf["Irr_mm"].max()
    
    for index, value in gdf["irr_mz"].items():
        if value == 99:
            gdf.at[index, "Irr_mm"] =  irr_user_rate[i]
    
    gdf['irr_l_ha'] = gdf['Irr_mm'] * 10000
    print(gdf["Irr_mm"].min(), gdf["Irr_mm"].min() 
          + IN_range * 2/8, gdf["Irr_mm"].min()
          + IN_range * 4/8, gdf["Irr_mm"].min() 
          + IN_range * 6/8, gdf["Irr_mm"].max())

    gdf["irr_l_ha"] = round(gdf["irr_l_ha"])
    gdf["Irr_mm"] = round(gdf["Irr_mm"],1)
    print(gdf)
    print(gdf["Irr_mm"].sort_values().unique())
 

	## Irrigation Plots
    if field[i] == "mortier":
        fig, axs = plt.subplots(nrows=3, ncols=3, figsize=(16,15))
        print("plotting " + field[i])
    elif field[i] == "grandplane":
        fig, axs = plt.subplots(nrows=3, ncols=3, figsize=(16,15))
        print("plotting " + field[i])
    elif field[i] == "galloutibout":
        fig, axs = plt.subplots(nrows=3, ncols=3, figsize=(16,24))
        print("plotting " + field[i])
    else:
        fig, axs = plt.subplots(nrows=3, ncols=3, figsize=(16,24))
        print("plotting...")

    # Colormaps
    colorFC  = LinearSegmentedColormap.from_list("", ['beige','maroon'])
    colorPWP = LinearSegmentedColormap.from_list("", ['maroon','beige'])
    colorIN  = LinearSegmentedColormap.from_list("", ['lightblue','darkblue'])
    colorSMC = LinearSegmentedColormap.from_list("", ['whitesmoke','lightblue','darkblue'])   
    colorWL  = LinearSegmentedColormap.from_list("", ['red','lightgreen'])


    # define bounds
    boundsFC  = np.linspace(100, 200, 9)    
    boundsPWP = np.linspace(20, 80, 9)
    boundsIN  = np.linspace(0, 90, 9)
    boundsSMC = np.linspace(0, 50, 9)
    boundsWL  = np.linspace(-10, 30, 9)
    
    # define bins and normalize    
    normFC  = mpl.colors.BoundaryNorm(boundsFC,  colorFC.N)
    normPWP = mpl.colors.BoundaryNorm(boundsPWP, colorPWP.N)
    normIN  = mpl.colors.BoundaryNorm(boundsIN,  colorIN.N)
    normSMC = mpl.colors.BoundaryNorm(boundsSMC, colorSMC.N)
    normWL  = mpl.colors.BoundaryNorm(boundsWL,  colorWL.N)

    
    legend_set =  {'loc': 'upper right', 'prop': {'size': 12}}
    gdf.plot(ax=axs[0,0], column="irr_mz",  cmap="viridis",legend="TRUE", categorical="true",legend_kwds=legend_set)
    gdf.plot(ax=axs[0,1], column="smc",     cmap=colorSMC,legend="TRUE",norm=normSMC)
    gdf.plot(ax=axs[0,2], column="CMC_mm",  cmap=colorSMC,legend="TRUE")
    gdf.plot(ax=axs[1,0], column="FC_mm",   cmap=colorFC,legend="TRUE")
    gdf.plot(ax=axs[1,1], column="PWP_mm",  cmap=colorPWP,legend="TRUE")
    gdf.plot(ax=axs[1,2], column="AW_mm",   cmap=colorFC,legend="TRUE")
    gdf.plot(ax=axs[2,0], column="WL_mm",   cmap=colorWL,legend="TRUE",norm=normWL)
    gdf.plot(ax=axs[2,1], column="IN_mm",   cmap='cool',legend="TRUE",norm=normIN)
    gdf.plot(ax=axs[2,2], column="Irr_mm",cmap='cool',legend="TRUE", categorical="true",legend_kwds=legend_set)

    axs[0,0].set_title("Irrigation MZs")
    axs[0,1].set_title("Soil Moisture [%]")
    axs[0,2].set_title(f"Soil Moisture content of {str(depth[i])} mm soil depth [mm]")
    axs[1,0].set_title("FC [mm]")
    axs[1,1].set_title("PWP [mm]")
    axs[1,2].set_title("Available water (FC-PWP) [mm]")
    axs[2,0].set_title(f"Water left until MAD [mm],\n with MAD at {str(MAD_set[i]*100)} % of AW")
    axs[2,1].set_title("Theoretical Irrigation application rates [mm]")
    axs[2,2].set_title(f"Irrigation application rates [mm]\n with min: {str(Irr_min[i])} mm and max: {str(Irr_max[i])} mm ")
    
    axs[0,0].axis("off")
    axs[0,1].axis("off")
    axs[0,2].axis("off")
    axs[1,0].axis("off")
    axs[1,1].axis("off")
    axs[1,2].axis("off")
    axs[2,0].axis("off")
    axs[2,1].axis("off")
    axs[2,2].axis("off")
    
    fig.suptitle(str(fieldname[i]) + " " + str(date.today()) + ": With Root depth "+ str(depth[i]) +"mm and Fill up Rate "+str(fill_up_rate[i]*100)) 
    
    plt.tight_layout(pad=2.5)
    
    plt.savefig("outputFiles/" + field[i] + "_irr.png")
    plt.savefig("outputFiles/irrigation_charts/" + field[i] + "_irr_" + str(date.today()) + ".png")

    #plt.show()

    ## Fertilization Plots
    if fer_MZ_gdf is not None:
        plotsize = [18,8,8]
        fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(6,8))
        legend_set =  {'loc': 'upper right', 'prop': {'size': 12}}
        gdf.plot(ax=axs[0], column="fer_mz",cmap='viridis', legend="TRUE",categorical="true",legend_kwds=legend_set)
        gdf.plot(ax=axs[1], column="fer_l_ha",cmap='cool',legend="TRUE",categorical="true",legend_kwds=legend_set)
        axs[0].set_title(field[i] + ": Fertilization MZ ")
        axs[1].set_title(field[i] + ": Fertilization amount [l/ha]")     
        axs[0].axis("off")
        axs[1].axis("off")
        #axs[0].legend(loc='upper center',legend="TRUE")
        plt.tight_layout()
        plt.savefig("outputFiles/" + field[i] + "_fer.png")

  

	## Export the results
    # folder      = "outputFiles/" + field[i] +  "_application_map"
    
    # Create a new directory if it does not exist
    if not os.path.exists(folder):
        os.makedirs(folder)

    # reduce the dataframe
    if fer_MZ_gdf is not None:
        gdf_out = gdf[['fer_l_ha','irr_l_ha','IN_mm','row', 'column', 'field_id','geometry']]
    else:
        gdf_out = gdf[['irr_l_ha','IN_mm','row', 'column', 'field_id','geometry']]
        
    gdf_out = gdf_out.rename(columns={"IN_mm":"irr_mm"})

    # Export as Shapefile
    gdf_out.to_file(folder, driver='ESRI Shapefile')

    # Export as GeoJson
    gdf_out.to_file(folder+"/"+field[i]+"_"+str(date.today())+".geojson", driver="GeoJSON")
    print(folder+"/"+field[i]+"_"+str(date.today())+".geojson")

    #"outputFiles/irrigation_charts/" + field[i] + "_irr_" + str(date.today()) + ".png"

    # create zip file)
    shutil.make_archive(folder, 'zip', folder)
    
    
"""
	## Upload the Geoserver
    try:
      with open(folder +'.zip', 'rb') as f:
        data = f.read()
      url = 'https://geoportal.addferti.eu/geoserver/rest/workspaces/'    
      response = requests.put(
        url + 'geonode/datastores/' + filename + '/file.shp',
        headers={'Content-type': 'application/zip'},
        data=data,
        verify=False,
        auth=('admin', 'geoserver')
        )
      print(folder +'.zip uploaded' )
    except FileNotFoundError:
      print(folder + " file not found")
"""

    ## Export "Water left until MAD" as a matrix
    # Extract centroids of polygon and transform to form: WL_mm, x, y
    #gdf_wl = gdf
    #gdf_wl['centroid'] = gdf_wl.centroid
    #gdf_wl['x'] = gdf_wl['centroid'].x 
    #gdf_wl['y'] = gdf_wl['centroid'].y 
    #gdf_wl = gdf_wl[['WL_mm','x','y']]
    #gdf_wl

    #filename = "outputFiles/water_left_" + uni[i] + ".txt"
    #gdf_wl.to_csv(filename)
    # Reproject to WGS84 + longlat
    #wl_raster_idw = projectRaster(wl_raster_idw, crs="+proj=longlat +datum=WGS84")

    # Transform Raster to Point matrix with format: "x, y, value"
    #wl_matrix_idw = rasterToPoints(wl_raster_idw, spatial=False)
    #wl_matrix_idw.columns = ["x", "y", "wl"]

    #filename = "water_left_" + university[i] + ".txt"
    #wl_matrix_idw.to_csv(filename, sep=",", index=False)


	#filename = "water_left_" + university[i] + ".txt"
	#wl_matrix_idw.to_csv(filename, sep=",", index=False)

