import requests
import json
from requests.auth import HTTPBasicAuth

##########
#class for uploading
def postData(date, ride_id, collection):
    try:
        body = {}
        body['ride_id'] = ride_id
        body['dayOfRide'] = date
        body['collection'] = collection
        body = json.dumps(body)

        urlValue ="https://myopendash.de/nr/UpdateGPSData"
        #print (urlValue)
        headers={"Content-type": "application/json"}
        rValue= requests.post(urlValue, auth=HTTPBasicAuth("test", "test"), data=body, headers=headers)

    except Exception as e:
        print(e)
    return rValue

##############
userids=[
    "2023339e-ddcb-4aeb-87c2-f0ab9ef2df47",
    "2b1d40f9-4ee5-4b2c-b9be-0f2923f2608f",
    "57544707-5a9f-4ffa-b8d2-aad619a5a587",
    "91d6a883-f0c4-46b5-af24-961476e3b2ec",
    "a4db3490-32f8-4739-b082-8abf6dff5be8",
    "b7c1de31-0b43-420e-a7e1-4fa8af318854",
    "c5d57dc0-4758-45e5-ac8f-2ffb91bb2d14",
    "d90666a5-6c01-488e-9eaf-baf1e5b56822"
]


for user_id in userids:
    identifier= user_id + "-1"
    collection= identifier +"-rides_minimized"

    # WebService der fur einen Nutzer alle Fahrten ausgibt
    url = "https://myopendash.de/nr/getRides"
    data = {'user_id': user_id, 'car_id': 1, 'identifier': identifier, "collection": collection}
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, auth=HTTPBasicAuth('test', 'test'), data=json.dumps(data), headers=headers)
    json_data = json.loads(r.text)

        # Bei jedem Nutzer gibt es verschiedene Fahrten
    # In dieser Schleife werden die einzelnen Fahrten durchgegangen
    # Jede Fahrt hat ein eigenes GeoJSON
    # Hier konnte man jetzt die ganzen GeoJSON Datei in ein Array packen
    # oder alle Koordinaten aus allen Fahrten in einer Liste speichern
    for data in json_data:
        newDate=0;
        #print(data)
        #print(data['geojson'])
        #check date if has to be updated (best, check dayOfRide), then use first geojson.coordinates[0][3] to update date and send back to server
        if 'geojson' in data:
            #print(data['geojson']['coordinates'][0][3])
            #timestamp should have length of 13
            if len(str(data['geojson']['coordinates'][0][2])) < 16:
                power= 13-len(str(data['geojson']['coordinates'][0][3]))
                newDate=data['geojson']['coordinates'][0][3]*(10**power)
                print(newDate)
            else:
                print("date ok")
                newDate=data['geojson']['coordinates'][0][3]

            try:
                #print(jsonFile)
                postData(newDate, data['ride_id'], collection)
            except Exception as inst:
                print(type(inst))
                print(inst.args)
                print ("Error - Upload Result")
                break