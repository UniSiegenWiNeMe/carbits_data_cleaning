#!/usr/bin/env python
#  coding=UTF-8
import requests
import json
import pika

users = [
    'timo.jakobi@h-brs.de',
   'luca.gallo@smail.wis.h-brs.de',
   'marcel.lacroze@student.uni-siegen.de',
    'paulstaskie@gmail.com'
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
#########
###BEFORE USING: MAKE BACKUP IN MONGO, then pick username
#########
for username in users:
    #username = "marcel.lacroze@student.uni-siegen.de"
    #############

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
            ignition = bool(singleGeoObject['value'][2])
            print(ignition)
        except IndexError:
                continue
        #  build json format
        jsonBrokerFormat = dict()
        jsonBrokerFormat['id'] = "owcore.einfachteilen.ignition"
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
        ignitionValueObject = valueObject
        ignitionValueObject['value'].append(ignition)


        # set up valueType object for speed and ignition
        ignitionTypeValue = dict()
        ignitionTypeValue['name'] = 'Zündung'
        ignitionTypeValue['type'] = 'Boolean'

        # set up whole Object
        jsonBrokerFormat['values'].append(ignitionValueObject)
        jsonBrokerFormat['valueTypes'].append(ignitionTypeValue)


        #  publish to queue
        channel.basic_publish(exchange='amq.topic', routing_key='einfachteilen.ignition', body=json.dumps(jsonBrokerFormat))
    #  after a user has been processed, close
connection.close()
