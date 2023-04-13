#!/usr/bin/python3

#Create the layer and the store on GeoServer (Updating the layer is not possible)
# Update Geonode with:
# docker exec -it django4my_geonode python manage.py updatelayers
import requests

name = ['ru','bursa','ugent']
url = 'https://geoportal.addferti.eu/geoserver/rest/workspaces/'

for i in range(0,3):
    datastore = 'VRI_' + name[i] + '_application_map'
    folder = 'outputFiles/VRI_' + name[i] + '_application_map' 
    try:
        with open(folder + '/' + datastore +'.zip', 'rb') as f:
            data = f.read()

        response = requests.put(
            url + 'geonode/datastores/' + datastore + '/file.shp',
            headers={'Content-type': 'application/zip'},
            data=data,
            verify=False,
            auth=('admin', 'addferti')
        )
        print(folder + '/' + datastore + '.zip uploaded' )
    except FileNotFoundError: 
        print(folder + '/' + datastore +'.zip not found!!!')