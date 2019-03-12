import requests
import json
from requests.auth import HTTPBasicAuth
import datetime
from dateutil import parser
#!/usr/bin/env python
import pika



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
#########
###BEFORE USING: MAKE BACKUP IN MONGO, then pick username
users = [
    'timo.jakobi@h-brs.de',
    'luca.gallo@smail.wis.h-brs.de',
    'marcel.lacroze@student.uni-siegen.de',
    'paulstaskie@gmail.com'
]
for username in users:
    ##########
    # WebService der fur einen Nutzer alle Fahrten ausgibt
    url = "http://kompetenzzentrum-siegen.digital:4567/getHistoricalData/" + username + "_backup/owcore.einfachteilen.paj/0/1999968482268"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.get(url, headers=headers)
    json_data = json.loads(r.text)

    #  In dieser Schleife werden die einzelnen rides durchgegangen
    #we already have them
    valueTypes = []

    for singleGeoObject in json_data['values']:

        # use existing date
        millis = singleGeoObject['date']
        try:
            speed = singleGeoObject['value'][1]
            print(speed)
        except IndexError:
                continue
        try:
            speed = float(speed)*1.6093
        except ValueError:
            print("ValueError")
            continue
        #  build json format
        jsonBrokerFormat = dict()
        jsonBrokerFormat['id'] = "owcore.einfachteilen.speed"
        jsonBrokerFormat['parent'] = []
        jsonBrokerFormat['meta'] = {}
        jsonBrokerFormat['name'] = "einfachTeilen"
        jsonBrokerFormat['icon'] = "Icons/smarthome/default_18.png"
        jsonBrokerFormat['valueTypes'] = []

        jsonBrokerFormat['user'] = username
        jsonBrokerFormat['values'] = []
        #  alle Values fuer die Dimensionen
        valueObject = dict()
        valueObject['date'] = millis
        valueObject['value'] = []

        #set up Value object for speed and ignition
        speedValueObject = valueObject
        speedValueObject['value'].append(speed)


        # set up valueType object for speed and ignition
        speedTypeValue = dict()
        speedTypeValue['name'] = 'Geschwindigkeit_1'
        speedTypeValue['type'] = 'Number'

        # set up whole Object
        jsonBrokerFormat['values'].append(speedValueObject)
        jsonBrokerFormat['valueTypes'].append(speedTypeValue)


        #  publish to queue
        channel.basic_publish(exchange='amq.topic', routing_key='einfachteilen.speed', body=json.dumps(jsonBrokerFormat))
    #  after a user has been processed, close
connection.close()
