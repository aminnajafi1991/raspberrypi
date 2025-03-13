# Developed by: Sebastian Maurice, PhD
# Date: 2021-01-18 
# Toronto, Ontario Canada

# TML python library
import maadstml

# Uncomment IF using Jupyter notebook 
import nest_asyncio

import json
import random
from joblib import Parallel, delayed
import sys
import multiprocessing
import pandas as pd
import asyncio
import datetime
import time
import os

# Apply asyncio fix for Jupyter notebooks
nest_asyncio.apply()

basedir = os.environ['userbasedir'] 

# Set Global Host/Port for VIPER
VIPERHOST=''
VIPERPORT=''
HTTPADDR='https://'

#############################################################################################################
#                                      STORE VIPER TOKEN

def getparams():
     global VIPERHOST, VIPERPORT, HTTPADDR
     with open(basedir + "/Viper-preprocess/admin.tok", "r") as f:
        VIPERTOKEN=f.read()

     if VIPERHOST=="":
        with open(basedir + '/Viper-preprocess/viper.txt', 'r') as f:
          output = f.read()
          VIPERHOST = HTTPADDR + output.split(",")[0]
          VIPERPORT = output.split(",")[1]
          
     return VIPERTOKEN

VIPERTOKEN=getparams()
if VIPERHOST=="":
    print("ERROR: Cannot read viper.txt: VIPERHOST is empty or HPDEHOST is empty")

#############################################################################################################
#                                     CREATE TOPICS IN KAFKA

def datasetup(maintopic,preprocesstopic):
     companyname="OTICS"
     myname="Sebastian"
     myemail="Sebastian.Maurice"
     mylocation="Toronto"

     replication=1
     numpartitions=3
     enabletls=1
     brokerhost=''
     brokerport=-999
     microserviceid=''
     description="TML Use Case"

     result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,maintopic,companyname,
                                    myname,myemail,mylocation,description,enabletls,
                                    brokerhost,brokerport,numpartitions,replication,
                                    microserviceid)
     
     try:
         y = json.loads(result,strict='False')
     except Exception as e:
         y = json.loads(result)

     for p in y:
         pid=p['ProducerId']
         tn=p['Topic']

     result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,preprocesstopic,companyname,
                                    myname,myemail,mylocation,description,enabletls,
                                    brokerhost,brokerport,numpartitions,replication,
                                    microserviceid)
         
     return tn,pid


def sendtransactiondata(maintopic,mainproducerid,VIPERPORT,index,preprocesstopic):

     maxrows=500
     offset=-1
     topic=maintopic
     producerid=mainproducerid
     brokerhost=''
     brokerport=-999
     microserviceid=''
     preprocessconditions=''
     delay=70
     enabletls=1
     array=0
     saveasarray=1
     topicid=-999
     rawdataoutput=1
     asynctimeout=120
     timedelay=0

     jsoncriteria='uid=metadata.dsn,filter:allrecords~\
subtopics=metadata.property_name~\
values=datapoint.value~\

identifiers=metadata.display_name~\
datetime=datapoint.updated_at~\

msgid=datapoint.id~\
latlong=lat:long'     

     tmlfilepath=''
     usemysql=1

     streamstojoin="" 
     identifier = "IoT device performance and failures"

<<<<<<< HEAD
     # if dataage - use:dataage_utcoffset_timetype
=======
     # UPDATED preprocesslogic
>>>>>>> 3e5a388e9 (Updated preprocess logic to include MIN, MAX, COUNT, VARIANCE, OUTLIERS, ANOMPROB)
     preprocesslogic='MIN,MAX,COUNT,VARIANCE,OUTLIERS,ANOMPROB'

     pathtotmlattrs='oem=n/a,lat=n/a,long=n/a,location=n/a,identifier=n/a'          
     try:
        result=maadstml.viperpreprocesscustomjson(VIPERTOKEN,VIPERHOST,VIPERPORT,topic,producerid,offset,jsoncriteria,rawdataoutput,maxrows,enabletls,delay,brokerhost,
                                          brokerport,microserviceid,topicid,streamstojoin,preprocesslogic,preprocessconditions,identifier,
                                          preprocesstopic,array,saveasarray,timedelay,asynctimeout,usemysql,tmlfilepath,pathtotmlattrs)
        return result
     except Exception as e:
        print(e)
        return e
     
#############################################################################################################
#                                     SETUP THE TOPIC DATA STREAMS

maintopic='iot-mainstream'
preprocesstopic='iot-preprocess'
maintopic,producerid=datasetup(maintopic,preprocesstopic)
print("Started Preprocessing: ", maintopic,producerid)

async def startviper():
    print("Start Preprocess-iot-monitor-customdata Request:",datetime.datetime.now())
    while True:
      try:   
        sendtransactiondata(maintopic,producerid,VIPERPORT,-1,preprocesstopic)            
        time.sleep(1)
      except Exception as e:
        print("ERROR:",e)
        continue

async def spawnvipers():
    loop.run_until_complete(startviper())
  
loop = asyncio.new_event_loop()
loop.create_task(spawnvipers())
asyncio.set_event_loop(loop)
loop.run_forever()


