import requests
import json
from requests.auth import HTTPBasicAuth
from array import *

##########
#class for uploading
def postData(coordinates, ride_id, collection):
    try:
        body = {}
        body['ride_id'] = ride_id
        body['coordinates'] = coordinates
        body['collection'] = collection + "_copy"
        body = json.dumps(body)

        urlValue ="https://myopendash.de/nr/UpdateGPSData"
        #print (urlValue)
        headers={"Content-type": "application/json"}
        rValue= requests.post(urlValue, auth=HTTPBasicAuth("test", "test"), data=body, headers=headers)

    except Exception as e:
        print(e)
    return rValue

##############

user_id = "2023339e-ddcb-4aeb-87c2-f0ab9ef2df47"
collection= user_id + "-1-rides"
#collection="rides_copy2"


# WebService der fur einen Nutzer alle Fahrten ausgibt
url = "https://myopendash.de/nr/getRides"
data = {'user_id': user_id, 'car_id': 1, 'identifier': user_id + "-1", "collection": collection}
headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
r = requests.post(url, auth=HTTPBasicAuth('test', 'test'), data=json.dumps(data), headers=headers)
json_data = json.loads(r.text)

# Bei jedem Nutzer gibt es verschiedene Fahrten
# In dieser Schleife werden die einzelnen Fahrten durchgegangen
# Jede Fahrt hat ein eigenes GeoJSON
# Hier konnte man jetzt die ganzen GeoJSON Datei in ein Array packen
# oder alle Koordinaten aus allen Fahrten in einer Liste speichern
for data in json_data:
    #print(data)
    #print(data['geojson'])
    #check alt/lat if has to be updated bc its too low, switch  and send back to server
    newCoordArray= []
    for item in data['geojson']['coordinates']:
        # if Altitude < 16, its the latitude, switch positions 2 with 1
        if ((float(item[2]) < 16) & (float(item[2])>float(-1000))):
            tempArr=[item[0],item[2],item[1], item[3]]
            print("switched")
        else:
            tempArr=item
        print(tempArr)
        newCoordArray.append(tempArr)

        try:
            #print(jsonFile)
            postData(newCoordArray, data['ride_id'], collection)
        except Exception as inst:
            print(type(inst))
            print(inst.args)
            print ("Error - Upload Result")
            break