#use to transfer GPS documents to working opendash Geojson format
#options:
# switch lat/long
# convert from degree DDMM.MMM to degree decimal
# remove values and value types other than GPS

# import requests
import json

import requests
from requests.auth import HTTPBasicAuth
import datetime
from dateutil import parser
#!/usr/bin/env python
import pika
#  helper function for getting miliseconds
epoch = datetime.datetime.utcfromtimestamp(0)
def unix_time_millis(dt):
    #strippedTimezone = datetime.datetime.strptime(dt[:19], '%Y-%m-%dT%H:%M:%S')
    return (dt - epoch).total_seconds() * 1000.0

#########
###BEFORE USING: MAKE BACKUP IN MONGO, then pick username
#############
users = [
    'timo.jakobi@h-brs.de',
    'luca.gallo@smail.wis.h-brs.de',
    'marcel.lacroze@student.uni-siegen.de',
    'paulstaskie@gmail.com',
    'ferhattelci@web.de'
]


#  ########
#  MAIN SCRIPT
#  ########
#  establish queue connection via rabbitMQ (amqp)
credentials = pika.PlainCredentials('admin', 'ms40adminpw')
parameters = pika.ConnectionParameters('kompetenzzentrum-siegen.digital', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
#  queue name ist egal
channel.queue_declare(queue='einfachteilen',  durable=True)

for username in users:

    # WebService der fur einen Nutzer alle Fahrten ausgibt
    url = "http://kompetenzzentrum-siegen.digital:4567/getHistoricalData/" + username + "_backup/owcore.einfachteilen.paj/0/1993353039698"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.get(url, headers=headers)
    json_data = json.loads(r.text)

    #  In dieser Schleife werden die einzelnen rides durchgegangen
    #we already have them
    existingValueTypes = []
    existingValueTypes = json_data['valueTypes']

    for singleGeoObject in json_data['values']:

        # use existing date
        millis = singleGeoObject['date']

        # switch coordinates if needed
        coordinates = []
        try:
            print(singleGeoObject['value'][0]['coordinates'])
            coordinates = singleGeoObject['value'][0]['coordinates']
        except KeyError:
            try:
                coordinates = singleGeoObject['value'][0]['geometry']['coordinates']
            except KeyError:
                continue
        #are there numbers?
        if (not isinstance(coordinates[0], float)) | (not isinstance(coordinates[1], float)):
            continue
        # are they not negative? (kind of : valid)
        if ((coordinates[0]<0) | (coordinates[1]<0)):
            continue
            #uncomment following block to switch wrong coordinates
       # if float(coordinates[0]) > float(coordinates[1]):
           # print("switching")
            #helper = coordinates[0]
            #coordinates[0] = coordinates[1]
            #coordinates[1] = helper
            ##########
            #uncomment to transfer coordinates from degree to decimal!
            #########
        #correct Minute degrees to decimal : format: DDMM.MMM . keep DD and divide MM.MMM by 60
        #coordinateNumbers0 = str(coordinates[0]).split(".")
        #coordinateNumbers1 = str(coordinates[1]).split(".")

        #dd0 = int(coordinateNumbers0[0])
        #mm0 = int(coordinateNumbers0[1])
        #mm0 = int(float(mm0)*1000)/60

       # dd1 = int(coordinateNumbers1[0])
       # mm1 = int(coordinateNumbers1[1])
       # mm1 = int(float(mm1)*1000)/60

       # coordinates[0]= float(str(dd0) + "." + str(mm0))
        #coordinates[1]= float(str(dd1) + "." + str(mm1))
        ##########
        #########
        #  build json format
        jsonBrokerFormat = {}
        jsonBrokerFormat['id'] = "owcore.einfachteilen.paj"
        jsonBrokerFormat['parent'] = []
        jsonBrokerFormat['meta'] = {}
        jsonBrokerFormat['name'] = "einfachTeilen"
        jsonBrokerFormat['icon'] = "Icons/smarthome/default_18.png"
        jsonBrokerFormat['valueTypes'] = []

        #use to throw away other value types than gps
        jsonBrokerFormat['valueTypes'].append(existingValueTypes[0])
        #jsonBrokerFormat['valueTypes'] = existingValueTypes
        jsonBrokerFormat['user'] = username
        jsonBrokerFormat['values'] = []
        #  alle Values fuer die Dimensionen
        valueObject = dict()
        valueObject['date'] = millis
        valueObject['type'] = "Feature"
        valueObject['value'] = []
        geometry = dict()
        geometry['coordinates'] = []
        float0= float(coordinates[0])
        float1= float(coordinates[1])
        geometry['coordinates'].append(float0)
        geometry['coordinates'].append(float1)

        geometry['type'] = "Point"
        valueObject['value'].append(geometry)
        #use following block to keep additional values
        #try:
            #valueObject['value'].append(singleGeoObject['value'][1])
            #valueObject['value'].append(singleGeoObject['value'][2])
        #except IndexError:
            #print("no additional values found")
        jsonBrokerFormat['values'].append(valueObject)
        print(jsonBrokerFormat)
        #  publish to queue
        channel.basic_publish(exchange='amq.topic', routing_key='einfachteilen.paj', body=json.dumps(jsonBrokerFormat))
    #  after a user has been processed, close
connection.close()
