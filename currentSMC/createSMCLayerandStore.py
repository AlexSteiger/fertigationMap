#!/usr/bin/python3

#Create the layer and the store on GeoServer (Updating the layer is not possible):
import requests
name = ['ru_soil_moisture','bursa_soil_moisture','ugent_soil_moisture']
url = 'https://geoportal.addferti.eu/geoserver/rest/workspaces/'

for i in range(0,3):
    datastore = 'current_' + name[i]
    folder = 'current_'+ name[i]
    try:
        with open(folder + '/current_' + name[i]+'.zip', 'rb') as f:
            data = f.read()

        response = requests.put(
            url + 'geonode/datastores/' + datastore + '/file.shp',
            headers={'Content-type': 'application/zip'},
            data=data,
            verify=False,
            auth=('admin', 'addferti')
        )
        print(folder + '/current_' + name[i] + ".zip uploaded" )
    except FileNotFoundError: 
        print(name[i] + " file not found")
