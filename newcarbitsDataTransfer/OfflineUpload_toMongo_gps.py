from __future__ import print_function
import os
import errno
import time
import os
import sys
import subprocess
import json
import math
import datetime
import pymongo
import httplib2
import json
import socket
import requests
import json
from requests.auth import HTTPBasicAuth
import datetime
#!/usr/bin/env python
import pika
import re
##############
obdCodes = {
    'gps' : 'Position'
}
obdUnits = {
    'gps': ''
}

obdSensors = [
    "gps"
]

#######USERINFORMATION#########
USERFOLDERNAME = "frankw"
userid = 'd90666a5-6c01-488e-9eaf-baf1e5b56822'
identifier = userid + "-1"

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
##########
#  helper function for getting miliseconds
epoch = datetime.datetime.utcfromtimestamp(0)
def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0

def extractGPS(dateiname):
    print("starting with " + dateiname)
    fobj = open(dateiname, 'rU')
    n=0
    for line in fobj:
        n=n+1
        data = {}
        line = line.replace(' ', '')
        line = line.replace('\x00', '')
        line = line.split('\t')
        aktuellerDatensatz = {}
        if len(line) > 2:
            aktuellerDatensatz = dict(s.split(':') for s in line)
        #print(aktuellerDatensatz)
        if 'Zeit' in aktuellerDatensatz:
            # print("Zeit found")
            aktuellerDatensatz['Zeit'] = aktuellerDatensatz['Zeit'].replace('\n', '')
            data['timestamp'] = convertTime(aktuellerDatensatz['Zeit'])
            # if time exists, at least we have an ignition date
            formatAndUploadIgnition(data)
            # check if also GPS data exists
            if aktuellerDatensatz['Longitude'] == aktuellerDatensatz['Latitude'] == "nan":
                print("no GPS data")
            else:
                print("GPS found")
                try:
                    data['gps'] = {}
                    data['gps']['value'] = {}
                    data['gps']['value']['longitude'] = float(aktuellerDatensatz['Longitude'])
                    data['gps']['value']['latitude'] = float(aktuellerDatensatz['Latitude'])
                    if not aktuellerDatensatz['Altitude'] is "nan":
                        data['gps']['value']['altitude'] = float(aktuellerDatensatz['Altitude'])
                    else:
                        data['gps']['value']['altitude'] = None
                    formatAndUploadGPS(data)
                except Exception as e:
                    print(e)
        else:
            print("Keine ZEIT in Daten")
    print("Closing File: " + dateiname + " processed lines: " + str(n))
    fobj.close()

def formatAndUploadGPS(data):
        millis = data['timestamp']
        #  build json format
        jsonBrokerFormat = {}
        jsonBrokerFormat['id'] = "carbits.gps.raw"
        jsonBrokerFormat['parent'] = []
        jsonBrokerFormat['meta'] = {}
        jsonBrokerFormat['name'] = 'GPS'
        jsonBrokerFormat['icon'] = "Icons/smarthome/default_18.png"
        jsonBrokerFormat['valueTypes'] = []
        #  Sensordimensionen fuer opendash  Anlegen
        valueTyp = {}
        #  this name may be longer
        valueTyp['name'] = obdCodes['gps']
        valueTyp['type'] = "Number"
        valueTyp['unit'] = obdUnits['gps']
        jsonBrokerFormat['valueTypes'].append(valueTyp)
        jsonBrokerFormat['user'] = userid
        jsonBrokerFormat['values'] = []
        #  alle Values fuer die Dimensionen
        values = {}
        values['date'] = millis
        values['value'] = []
        values['value'].append(data['gps']['value'])
        jsonBrokerFormat['values'].append(values)
        print(jsonBrokerFormat)
        #  publish to queue
        # channel.basic_publish(exchange='amq.topic', routing_key='carbits.obd', body=json.dumps(jsonBrokerFormat))

def formatAndUploadIgnition(timedata):
    #print(timedata)
    millis = timedata['timestamp']
    #  build json format
    jsonBrokerFormat = {}
    jsonBrokerFormat['id'] = "carbits.ignition.raw"
    jsonBrokerFormat['parent'] = []
    jsonBrokerFormat['meta'] = {}
    jsonBrokerFormat['name'] = 'Zuendung'
    jsonBrokerFormat['icon'] = "Icons/smarthome/default_18.png"
    jsonBrokerFormat['valueTypes'] = []
    #  Sensordimensionen fuer opendash  Anlegen
    valueTyp = {}
    #  this name may be longer
    valueTyp['name'] = "ignition"
    valueTyp['type'] = "Boolean"
    valueTyp['unit'] = ''
    jsonBrokerFormat['valueTypes'].append(valueTyp)
    jsonBrokerFormat['user'] = userid
    jsonBrokerFormat['values'] = []
    #  alle Values fuer die Dimensionen
    values = {}
    values['date'] = millis
    values['value'] = [1]
    jsonBrokerFormat['values'].append(values)
    print(jsonBrokerFormat)
    #  publish to queue
    # channel.basic_publish(exchange='amq.topic', routing_key='carbits.obd', body=json.dumps(jsonBrokerFormat))

def convertTime(timeElement):
    try:
        if len(timeElement) >= 16:
            print("long date found as %d.%m.%Y %H:%M:%S")
            timeStruct= datetime.datetime.strptime(timeElement, "%d.%m.%Y %H:%M:%S")
            timestamp = time.mktime(timeStruct.timetuple())
            # print(data)
        else:
            #print("short date found as float")
            timestamp = float(timeElement)/1000
    except Exception as e:
        print(e)
    timestamp = int(timestamp)
    # print(timestamp)
    return timestamp


def sorted_ls(path):
    mtime = lambda f: os.stat(os.path.join(path, f)).st_ctime
    # print(mtime)
    return list(sorted(os.listdir(path), key=mtime))

##########START OF MAIN FUNCTION W/O METHODS############
os.chdir("C:/Users/timoj/Nextcloud/carbits/Empirie/Daten/bis Ende/" + USERFOLDERNAME)
dirList = os.listdir('.')
dirList = sorted_ls('.')
# print(dirList)

#  establish queue connection via rabbitMQ (amqp)
credentials = pika.PlainCredentials('einfachTeilen', 'teilenMachtSpass')
parameters = pika.ConnectionParameters('kompetenzzentrum-siegen.digital', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
#  queue name ist egal
channel.queue_declare(queue='carbits',  durable=True)


# for all files starting with GPS identifier
for sFile in dirList:
    if sFile[:10] == "Start_GPS5" and sFile[-15:] != "hochgeladen.txt" :
        try:
            extractGPS(sFile)
            # os.rename(sFile, sFile[:-4]+"hochgeladen.txt")
        except EnvironmentError as e:
            print("FEHLER" + os.strerror(e.errno))

dirList = os.listdir('.')
dirList = sorted_ls('.')


# close connection
connection.close()
