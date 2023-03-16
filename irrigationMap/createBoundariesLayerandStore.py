#!/usr/bin/python3

#Create the layer and the store on GeoServer (Updating the layer is not possible):
# docker exec -it django4my_geonode python manage.py updatelayers

import requests
name = ['field_boundaries_ru','field_boundaries_bursa','field_boundaries_ugent']
url = 'https://geoportal.addferti.eu/geoserver/rest/workspaces/'
folder = 'outputFiles/Field_Boundaries'

for i in range(0,3):
    file = name[i] + '.zip'
    datastore = name[i]
    print('open: ' + folder + '/' + file,)
    try:
        with open(folder + '/' + file, 'rb') as f:
            data = f.read()

        response = requests.put(
            url + 'geonode/datastores/' + datastore + '/file.shp',
            headers={'Content-type': 'application/zip'},
            data=data,
            verify=False,
            auth=('admin', 'addferti')
        )
        
        print(folder + '/current_' + file + ' uploaded' )
    except FileNotFoundError: 
        print(file + " file not found")
