import requests
import json
from requests.auth import HTTPBasicAuth
import datetime
#!/usr/bin/env python
import pika
##############
obdCodes = {
    'cel': 'Berechnete Motorlast',
    'ect': 'Motorkuehlmitteltemperatur',
    'rpm': 'Umdrehungen pro Minute',
    'speed': 'Geschwindigkeit',
    'ta':'Zeitvorschub vor TDC',
    'iat': 'Ansauglufttemperatur',
    'mafr': 'Luftdurchsatz',
    'tp': 'Drosselklappenstellung',
    'rses': 'Zeit seit Motorstart',
    'dtwm': 'Distanz mit Fehlern',
    'frp': 'Kraftstoff Einspritz Druck',
    'frgp': 'Kraftstoffzuleitung Druck',
    'cegr': 'Befohlene Abgasrueckfuehrung',
    'egre':'Abgasrueckfuehrungsfehler',
    'cep': 'Befohlene Verdunstungsspuelung',
    'ftli': 'Tankfuellstand',
    'wscc': 'Motorerwaermungen seit Fehlerloeschung',
    'dtscc': 'Distanz seit Fehlerloeschung',
    'esvp': 'Verdampfungssystem Dampfdruck',
    'abp': 'Barometrischer Druck'
}
obdUnits = {
    'ect': 'C',
    'rpm': 'U/min',
    'speed': 'km/h',
    'ta': 's',
    'iat': ' C',
    'mafr': 'Gramm/s',
    'tp': '%',
    'rses': 's',
    'dtwm': 'Km',
    'frp': 'kPa',
    'frgp': 'kPa',
    'cegr': '%',
    'egre': '%',
    'abp': 'kPa',
    'dtscc': 'Km',
    'esvp': 'Pa',
    'wscc': 'Motorstarts',
    'ftli': '%',
    'cep': '%',
    'cel': ' %'
}
userids = [
    #  uploaded 6.3.18
    #"2b1d40f9-4ee5-4b2c-b9be-0f2923f2608f",  # , jan drin
    "2023339e-ddcb-4aeb-87c2-f0ab9ef2df47",  # martin h: hat andere struktur!! drin
    "57544707-5a9f-4ffa-b8d2-aad619a5a587"  # tanja drin
    #"91d6a883-f0c4-46b5-af24-961476e3b2ec",  # vogel drin
   # "a4db3490-32f8-4739-b082-8abf6dff5be8",  # farni drin
    #"b7c1de31-0b43-420e-a7e1-4fa8af318854",  # sauer drin
    #"c5d57dc0-4758-45e5-ac8f-2ffb91bb2d14",  # mocz drin
    #"d90666a5-6c01-488e-9eaf-baf1e5b56822"   # frank w drin
]
obdSensors = [
    "abp",
    "cegr",
    "cel",
    "cep",
    "dtscc",
    "dtwm",
    "ec",
    "ect",
    "egre",
    "esvp",
    "frgp",
    "frp",
    "ftli",
    "iat",
    "mafr",
    "rpm",
    "rses",
    "speed",
    "ta",
    "tp",
    "wscc"
]
userMapping = {
    '2b1d40f9-4ee5-4b2c-b9be-0f2923f2608f': 'jan-hbrs@urfei.com',
    '2023339e-ddcb-4aeb-87c2-f0ab9ef2df47': 'martin@heuckmann.de',
    'd90666a5-6c01-488e-9eaf-baf1e5b56822': 'waefrae@freenet.de',
    '91d6a883-f0c4-46b5-af24-961476e3b2ec': 'karin.vogel-klein@web.de',
    '57544707-5a9f-4ffa-b8d2-aad619a5a587': 'tanja.puetzfeld@gmx.de',
    'c5d57dc0-4758-45e5-ac8f-2ffb91bb2d14': 'praxlabs@moczalla.de',
    'b7c1de31-0b43-420e-a7e1-4fa8af318854': 'd.staeudtner-sauer@gmx.de',
    'a4db3490-32f8-4739-b082-8abf6dff5be8': 'farni@farni.de'
}
#  helper function for getting miliseconds
epoch = datetime.datetime.utcfromtimestamp(0)
def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0

#  ########
#  MAIN SCRIPT
#  ########
for user_id in userids:
    print(user_id)
    identifier = user_id + "-1"
    for sensor in obdSensors:
        print(sensor)
        #  establish queue connection via rabbitMQ (amqp)
        credentials = pika.PlainCredentials('admin', 'ms40adminpw')
        parameters = pika.ConnectionParameters('kompetenzzentrum-siegen.digital', 5672, '/', credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        #  queue name ist egal
        channel.queue_declare(queue='carbits',  durable=True)

        collection = identifier+ "-" + sensor
        #  WebService der fur einen Nutzer alle Fahrten ausgibt
        url = "https://myopendash.de/nr/getObdData"
        data = {'user_id': user_id, 'identifier': identifier, 'collection':collection}
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        r = requests.post(url, auth=HTTPBasicAuth('test', 'test'), data=json.dumps(data), headers=headers)
        json_data = json.loads(r.text)

        if sensor=='ec':
            sensor='ect'
        #  In dieser Schleife werden die einzelnen Documents der collections durchgegangen
        for data in json_data:
            #  print(data)
            #  print(data['values'])
            jsonObject = {}
            for singleObject in data["values"]:
                if singleObject['value'] == -1000:
                    continue
                #  print(singleObject)
                #  check and correct date
                newDate = datetime.datetime.today()
                #  print(singleObject['timestamp'])
                if len(str(int(singleObject['timestamp']))) > 10:
                    power = 10-len(str(int(singleObject['timestamp'])))
                    #  print(power)
                    #  print(int(singleObject['timestamp'])*(10**power))
                    newDate = datetime.datetime.fromtimestamp(int(singleObject['timestamp'])*(10**power))
                else:
                    newDate = datetime.datetime.fromtimestamp(int(singleObject['timestamp']))

                millis = unix_time_millis(newDate)
                #  build json format
                jsonBrokerFormat = {}
                jsonBrokerFormat['id'] = "carbits."+sensor+".raw"
                jsonBrokerFormat['parent'] = []
                jsonBrokerFormat['meta'] = {}
                jsonBrokerFormat['name'] = sensor
                jsonBrokerFormat['icon'] = "Icons/smarthome/default_18.png"
                jsonBrokerFormat['valueTypes'] = []
                #  Sensordimensionen fuer opendash  Anlegen
                obdValue = {}
                #  this name may be longer
                obdValue['name'] = obdCodes[sensor]
                obdValue['type'] = "Number"
                obdValue['unit'] = obdUnits[sensor]
                jsonBrokerFormat['valueTypes'].append(obdValue)
                jsonBrokerFormat['user'] = userMapping[user_id]
                jsonBrokerFormat['values'] = []
                #  alle Values fuer die Dimensionen
                values = {}
                values['date'] = millis
                values['value'] = []
                values['value'].append(singleObject['value'])
                jsonBrokerFormat['values'].append(values)
                #  print(jsonBrokerFormat)
                #  publish to queue
                channel.basic_publish(exchange='amq.topic',routing_key='carbits.obd',body=json.dumps(jsonBrokerFormat))
        #  after a collection has been processed, close
        connection.close()
