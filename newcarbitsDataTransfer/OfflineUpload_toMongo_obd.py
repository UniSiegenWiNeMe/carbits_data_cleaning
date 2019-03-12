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
# !/usr/bin/env python
import pika
import re

##############
obdCodes = {
    'cel': 'Berechnete Motorlast',
    'ect': 'Motorkuehlmitteltemperatur',
    'rpm': 'Umdrehungen pro Minute',
    'speed': 'Geschwindigkeit',
    'ta': 'Zeitvorschub vor TDC',
    'iat': 'Ansauglufttemperatur',
    'mafr': 'Luftdurchsatz',
    'tp': 'Drosselklappenstellung',
    'rses': 'Zeit seit Motorstart',
    'dtwm': 'Distanz mit Fehlern',
    'frp': 'Kraftstoff Einspritz Druck',
    'frgp': 'Kraftstoffzuleitung Druck',
    'cegr': 'Befohlene Abgasrueckfuehrung',
    'egre': 'Abgasrueckfuehrungsfehler',
    'cep': 'Befohlene Verdunstungsspuelung',
    'ftli': 'Tankfuellstand',
    'wscc': 'Motorerwaermungen seit Fehlerloeschung',
    'dtscc': 'Distanz seit Fehlerloeschung',
    'esvp': 'Verdampfungssystem Dampfdruck',
    'abp': 'Barometrischer Druck',
    'gps': 'Position'
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
    'cel': ' %',
    'gps': ''
}
obdSensors = {
    'Absolute Barometric Pressure': 'abp',
    'Commanded EGR': "cegr",
    'Calculated engine load': "cel",
    'commanded evaporative purge': "cep",
    'Distance traveled since codes cleared': 'dtscc',
    'Distance traveled with malfunction': "dtwm",
    'Distancetraveled with malfunction': "dtwm",
    'Engine coolant temperature': "ect",
    'EGR Error': "egre",
    'Evap. System Vapor Pressure': 'esvp',
    'Fuel Rail Gauge Pressure': "frgp",
    'Fuel Rail Pressure': "frp",
    'Fuel Tank Level Input': 'ftli',
    'Intake air temperature': "iat",
    'MAF air flow rate': "mafr",
    'RPM': "rpm",
    'Run time since enginge start': "rses",
    "speed": 'speed',
    'Timing advance': "ta",
    'Throttle position': "tp",
    'Warm-ups since codes cleared': 'wscc'
}

#######USERINFORMATION#########
USERFOLDERNAME = "frankw"
userid = 'd90666a5-6c01-488e-9eaf-baf1e5b56822'
identifier = userid + "-1"

userMapping = {
    '2b1d40f9-4ee5-4b2c-b9be-0f2923f2608f': 'jan-hbrs@urfei.com',
    '2023339e-ddcb-4aeb-87c2-f0ab9ef2df47': 'martin@heuckmann.de',
    'd90666a5-6c01-488e-9eaf-baf1e5b56822': 'waefrae@freenet.de__test',
    '91d6a883-f0c4-46b5-af24-961476e3b2ec': 'karin.vogel-klein@web.de',
    '57544707-5a9f-4ffa-b8d2-aad619a5a587': 'tanja.puetzfeld@gmx.de',
    'c5d57dc0-4758-45e5-ac8f-2ffb91bb2d14': 'praxlabs@moczalla.de',
    'b7c1de31-0b43-420e-a7e1-4fa8af318854': 'd.staeudtner-sauer@gmx.de',
    'a4db3490-32f8-4739-b082-8abf6dff5be8': 'farni@farni.de'
}
##########
def extractOBD(dateiname):
    print("starting with " + dateiname)
    fobj = open(dateiname, 'rU')
    n = 0
    for line in fobj:
        n = n + 1
        line = line.replace('\n', '')
        line = line.replace('\x00', '')
        line = line.split('\t')
        aktuellerDatensatz = {}
        if len(line) > 2:
            #if time Item exists
            if any('Zeit' in item for item in line):
                #extract Zeit-Item because it might have ':' in its format, to proceed to create a dict.
                listFilteredForZeit = [k for k in line if 'Zeit' not in k]
                time = [k for k in line if 'Zeit' in k]
                time = str(time[0].replace(' Zeit: ', ''))
                aktuellerDatensatz = dict(s.split(':') for s in listFilteredForZeit)
                for item in aktuellerDatensatz:
                    if "NO DATA" not in aktuellerDatensatz[item]:
                        timestamp = convertTime(time)
                        # remove unit here
                        valueOnly = aktuellerDatensatz[item].split(' ')
                        #print(valueOnly[1])
                        formatAndUploadOBD(item, valueOnly[1], timestamp)
                        # check if also GPS data exists
                    else:
                        print(aktuellerDatensatz[item])
            else:
                print("Keine ZEIT in Daten")
    print("Closing File: " + dateiname + " processed lines: " + str(n))
    fobj.close()


def formatAndUploadOBD(itemName, itemValue, timestamp):
    try:
        itemName = itemName.replace(' ', '', 1)
        itemShortName = obdSensors[itemName]
        #  build json format
        jsonBrokerFormat = {}
        jsonBrokerFormat['id'] = 'carbits.' + itemShortName + '.raw'
        jsonBrokerFormat['parent'] = []
        jsonBrokerFormat['meta'] = {'description': itemName}
        jsonBrokerFormat['name'] = itemShortName
        jsonBrokerFormat['icon'] = 'Icons/smarthome/default_18.png'
        jsonBrokerFormat['valueTypes'] = []
        #  Sensordimensionen fuer opendash  Anlegen
        valueTyp = {}
        #  this name may be longer
        valueTyp['name'] = itemName
        valueTyp['type'] = itemShortName
        valueTyp['unit'] = obdUnits[itemShortName]
        jsonBrokerFormat['valueTypes'].append(valueTyp)
        jsonBrokerFormat['user'] = userid
        jsonBrokerFormat['values'] = []
        #  alle Values fuer die Dimensionen
        values = {}
        values['date'] = timestamp
        values['value'] = itemValue
        jsonBrokerFormat['values'].append(values)
        #  publish to queue
        channel.basic_publish(exchange='amq.topic', routing_key='einfachTeilen', body=json.dumps(jsonBrokerFormat))
        print("sent:" , jsonBrokerFormat)
    except KeyError as e:
        print(e)

def convertTime(timeElement):
    try:
        timeElement = timeElement.replace(' ', '', 1)
       # print(len(timeElement))
        if ':' in timeElement:
            #print('long date found as %d.%m.%Y %H:%M:%S')
            timeStruct = datetime.datetime.strptime(timeElement, '%d.%m.%Y%H:%M:%S')
            timestamp = time.mktime(timeStruct.timetuple())
        else:
            # print("short date found as float")
            timestamp = float(timeElement)
    except Exception as e:
        print(e)
    timestamp = int(timestamp)
    #print(timestamp)
    return timestamp


def sorted_ls(path):
    mtime = lambda f: os.stat(os.path.join(path, f)).st_ctime
    # print(mtime)
    return list(sorted(os.listdir(path), key=mtime))


##########START OF MAIN FUNCTION W/O METHODS############
os.chdir('C:/Users/timoj/Nextcloud/carbits/Empirie/Daten/bis Ende/' + USERFOLDERNAME)
dirList = os.listdir('.')
dirList = sorted_ls('.')
# print(dirList)

#  establish queue connection via rabbitMQ (amqp)
credentials = pika.PlainCredentials('einfachTeilen', 'teilenistdoof')
parameters = pika.ConnectionParameters('kompetenzzentrum-siegen.digital', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
#  queue name ist egal
channel.queue_declare(queue='carbits', durable=True)

dirList = os.listdir('.')
dirList = sorted_ls('.')

# for all files starting with OBD identifier
for sFile in dirList:
    #length with "Start" and date,time and txt
    if len(sFile) == 26 and sFile[-15:] != "hochgeladen.txt":
        try:
            extractOBD(sFile)
            # os.rename(sFile, sFile[:-4]+"hochgeladen.txt")
        except EnvironmentError as e:
            print(os.strerror(e.errno))
# close connection
connection.close()
