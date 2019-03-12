import requests
import json
from requests.auth import HTTPBasicAuth
import math

##########
#class for uploading
#postData(identifier, collection, timestamp, data['ride_id'], newCoordinatesList)
def postData(identifier, timestamp, ride_id, coordinateList):
    try:
        body = {}
        body['identifier'] = identifier
        body['ride_id'] = ride_id
        body['timestamp'] = timestamp
        #webservice nodeJS adds "_minimized" itself
        body['collection'] = "rides"
        body['sensors'] = coordinateList
        body = json.dumps(body)
        print(body)
        urlValue ="https://myopendash.de/nr/minimizedRides"
        #print (urlValue)
        headers={"Content-type": "application/json"}
        rValue= requests.post(urlValue, auth=HTTPBasicAuth("test", "test"), data=body, headers=headers)

    except Exception as e:
        print(e)
    return rValue

##############
userids=[
    "2b1d40f9-4ee5-4b2c-b9be-0f2923f2608f"
   #"2023339e-ddcb-4aeb-87c2-f0ab9ef2df47",
   # "2b1d40f9-4ee5-4b2c-b9be-0f2923f2608f",
  #  "57544707-5a9f-4ffa-b8d2-aad619a5a587",
 #   "91d6a883-f0c4-46b5-af24-961476e3b2ec",
 #   "a4db3490-32f8-4739-b082-8abf6dff5be8",
   # "b7c1de31-0b43-420e-a7e1-4fa8af318854",
 #  "c5d57dc0-4758-45e5-ac8f-2ffb91bb2d14",
  #  "d90666a5-6c01-488e-9eaf-baf1e5b56822"
  ]


for user_id in userids:
    identifier= user_id + "-1"
    collection= identifier +"-rides"
    # WebService der fur einen Nutzer alle Fahrten ausgibt
    url = "https://myopendash.de/nr/getRides"
    data = {'user_id': user_id, 'car_id': 1, 'identifier': identifier, "collection": collection}
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, auth=HTTPBasicAuth('test', 'test'), data=json.dumps(data), headers=headers)
    json_data = json.loads(r.text)

    # In dieser Schleife werden die einzelnen Fahrten durchgegangen
    # Jede Fahrt hat ein eigenes GeoJSON
    #Fuer jeden Ride :
    # wenn Laenge der Lat/long >35(?) dann reduce und immer noch startpunkt und letzten Punkt nehmen
    # dann zurueck laden an rides_minimized

    for data in json_data:
        timestamp= data['geojson']['coordinates'][0][3]
        print(timestamp)
        #print(data)
        #print(data['geojson'])
        #wenn element besteht (sollte immer)
        if 'geojson' in data:
            #rides max is 35, also wenn zu Lang und gekuerzt werden muss
            if (len(data['geojson']['coordinates']) > 35):
                #minimize here
                print("Shortening Ride")
                #max 35 value Pairs for URI length of routing, makes max 70 value points in coordinates array + one for the destination
                multiplier= math.floor(len(data['geojson']['coordinates'])/35)+1;
                multiplier= math.floor(len(data['geojson']['coordinates'])/35)+1;
                #print("multiplier:" +str(int(multiplier)))
                newCoordinatesList= data['geojson']['coordinates'][0::int(multiplier)]
                #re check Laenge der neuen
                if(len(newCoordinatesList)>35):
                    print("ALARM! LENGTH STILL TOO LONG:" +str(len(newCoordinatesList)))
            else:
                print("Taking Ride as is")
                newCoordinatesList= data['geojson']['coordinates']

        #upload is needed in both cases, either with new shortened coordinates list, or with existing list!
        #print( newCoordinatesList)
        try:
            postData(identifier, timestamp, data['ride_id'], newCoordinatesList)
        except Exception as inst:
            print(type(inst))
            print(inst.args)
            print ("Error - Upload Result")
            break