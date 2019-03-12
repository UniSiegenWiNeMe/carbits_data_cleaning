import requests
import json
from requests.auth import HTTPBasicAuth
import datetime
from dateutil import parser
#!/usr/bin/env python
import pika

userMapping = {
    '2b1d40f9-4ee5-4b2c-b9be-0f2923f2608f': 'jan-hbrs@urfei.com', #drin
    '2023339e-ddcb-4aeb-87c2-f0ab9ef2df47': 'martin@heuckmann.de',  #drin
    'd90666a5-6c01-488e-9eaf-baf1e5b56822': 'waefrae@freenet.de',#drin
    '91d6a883-f0c4-46b5-af24-961476e3b2ec': 'karin.vogel-klein@web.de', #drin
    '57544707-5a9f-4ffa-b8d2-aad619a5a587': 'tanja.puetzfeld@gmx.de' #drin
    'c5d57dc0-4758-45e5-ac8f-2ffb91bb2d14': 'praxlabs@moczalla.de', #drin
   'b7c1de31-0b43-420e-a7e1-4fa8af318854': 'd.staeudtner-sauer@gmx.de', #drin
   'a4db3490-32f8-4739-b082-8abf6dff5be8': 'farni@farni.de' #drin
}
#  helper function for getting miliseconds
epoch = datetime.datetime.utcfromtimestamp(0)
def unix_time_millis(dt):
    #strippedTimezone = datetime.datetime.strptime(dt[:19], '%Y-%m-%dT%H:%M:%S')
    return (dt - epoch).total_seconds() * 1000.0

#  ########
#  MAIN SCRIPT
#  ########
for user_id in userMapping:
    print(user_id)
    identifier = user_id + "-1"
    #  establish queue connection via rabbitMQ (amqp)
    credentials = pika.PlainCredentials('admin', 'ms40adminpw')
    parameters = pika.ConnectionParameters('kompetenzzentrum-siegen.digital', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    #  queue name ist egal
    channel.queue_declare(queue='carbits',  durable=True)

    identifier = user_id + "-1"
    collection= identifier +"-rides"
    # WebService der fur einen Nutzer alle Fahrten ausgibt
    url = "https://myopendash.de/nr/getRides"
    data = {'user_id': user_id, 'car_id': 1, 'identifier': identifier, 'collection':collection}
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, auth=HTTPBasicAuth('test', 'test'), data=json.dumps(data), headers=headers)
    json_data = json.loads(r.text)

    #  In dieser Schleife werden die einzelnen rides durchgegangen
    for ride in json_data:
        #  print(data)
        #  print(data['values'])
        jsonObject = {}

        #print(ride)
        if 'geojson' not in ride:
            directory = ride['coordinates']
            #continue
        else:
            directory = ride['geojson']['coordinates']
        jsonObject = {}

        for singleCoordinateArray in directory:

            millis = singleCoordinateArray[3]
            #print("before: " + str(millis))
            #print(millis)
            if len(str(millis)) < 13:
               power = 13-len(str(millis))
               #print(power)
               millis = millis*(10**power)
            #print(millis)
            # check if altitude is set
            if not isinstance(singleCoordinateArray[2], float):
                singleCoordinateArray[2] = -1000

            if float(singleCoordinateArray[0]) > float(singleCoordinateArray[1]):
                print("switching")
                helper = singleCoordinateArray[0]
                singleCoordinateArray[0] = singleCoordinateArray[1]
                singleCoordinateArray[1] = helper
            #  build json format
            jsonBrokerFormat = {}
            jsonBrokerFormat['id'] = "carbits.gps.raw"
            jsonBrokerFormat['parent'] = []
            jsonBrokerFormat['meta'] = {}
            jsonBrokerFormat['name'] = "carbits"
            jsonBrokerFormat['icon'] = "Icons/smarthome/default_18.png"
            jsonBrokerFormat['valueTypes'] = []
            #  Sensordimensionen fuer opendash  Anlegen
            gpsValue = {}
            #  this name may be longer
            gpsValue['name'] = "carbitsGPS"
            gpsValue['type'] = "Geo"
            gpsValue['unit'] = "long/lat/alt"
            jsonBrokerFormat['valueTypes'].append(gpsValue)
            jsonBrokerFormat['user'] = userMapping[user_id]
            jsonBrokerFormat['values'] = []
            #  alle Values fuer die Dimensionen
            valueObject = dict()
            valueObject['date'] = millis
            valueObject['type'] = "Feature"
            valueObject['value'] = []
            geometry = dict()
            geometry['coordinates'] = []
            float0= float(singleCoordinateArray[0])
            float1= float(singleCoordinateArray[1])
            float2= float(singleCoordinateArray[2])
            geometry['coordinates'].append(float0)
            geometry['coordinates'].append(float1)
            geometry['coordinates'].append(float2)
            geometry['type'] = "Point"
            valueObject['value'].append(geometry)

            jsonBrokerFormat['values'].append(valueObject)
            #print(jsonBrokerFormat)
            #  publish to queue
            channel.basic_publish(exchange='amq.topic', routing_key='carbits.gps', body=json.dumps(jsonBrokerFormat))
    #  after a user has been processed, close
    connection.close()
