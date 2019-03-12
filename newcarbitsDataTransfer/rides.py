import requests
import json
from requests.auth import HTTPBasicAuth
import math

##########
#class for uploading
#postData(identifier, collection, timestamp, data['ride_id'], newCoordinatesList)
def postData(identifier, coordinateList):
    try:
        body = {}
        body['identifier'] = identifier
        body['sensors'] = coordinateList
        body = json.dumps(body)
        #print(body)
        urlValue ="https://myopendash.de/nr/putGpsData"
        #print (urlValue)
        headers={"Content-type": "application/json"}
        rValue= requests.post(urlValue, auth=HTTPBasicAuth("test", "test"), data=body, headers=headers)

    except Exception as e:
        print(e)
    return rValue

##############
userids=[
    #uploaded 2.3.18
  #  "2b1d40f9-4ee5-4b2c-b9be-0f2923f2608f" #, jan
   # "2023339e-ddcb-4aeb-87c2-f0ab9ef2df47" #martin h: hat andere struktur!!
    "57544707-5a9f-4ffa-b8d2-aad619a5a587", #tanja drin
    "91d6a883-f0c4-46b5-af24-961476e3b2ec"#, #vogel drin
    #"a4db3490-32f8-4739-b082-8abf6dff5be8", #farni
   # "b7c1de31-0b43-420e-a7e1-4fa8af318854", #sauer
   # "c5d57dc0-4758-45e5-ac8f-2ffb91bb2d14", #mocz
   # "d90666a5-6c01-488e-9eaf-baf1e5b56822" #frank w
]


for user_id in userids:
    print(user_id)
    identifier = user_id + "-1"
    collection= identifier +"-rides"
    # WebService der fur einen Nutzer alle Fahrten ausgibt
    url = "https://myopendash.de/nr/getRides"
    data = {'user_id': user_id, 'car_id': 1, 'identifier': identifier, 'collection':collection}
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, auth=HTTPBasicAuth('test', 'test'), data=json.dumps(data), headers=headers)
    json_data = json.loads(r.text)

    # In dieser Schleife werden die einzelnen Fahrten durchgegangen
    # Jede Fahrt hat ein eigenes GeoJSON
    #In der neuen DB brauchen wir nur Point-Objekte, also keine Multipoints.
    # Alle points werden einfach direkt reingeschrieben und ein analytics service uebernimmt die identifikation von einzelnen Rides.
    #Fuer jeden Ride :
    i=0
    for data in json_data:
        i=i+1
        #print("ident: "+ identifier)
        print(data)
        directory = ""
        ##check if geojson format exists! if not, change directory to use
        if 'geojson' not in data:
            directory = data['coordinates']
            #continue
        else:
            directory = data['geojson']['coordinates']
        jsonObject = {}

        for coordinateArray in directory:
        #print(data['geojson']['coordinates'])
        #upload is needed in both cases, either with new shortened coordinates list, or with existing list!
            #print(coordinateArray)
            try:
                postData(identifier, coordinateArray)
            except Exception as inst:
                print(type(inst))
                print(inst.args)
                print ("Error - Upload Result")
                break
