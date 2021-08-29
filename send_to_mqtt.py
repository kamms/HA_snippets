import sys
import os
import datetime
import json
import paho.mqtt.client as paho_client

def on_message(client,userdata,message):
 pass



activation_threshold = datetime.timedelta(seconds=0.5)
datafile = 'rtldata.json'
configfile = 'mqtt_config.json'
database = {}
if os.path.isfile(datafile):
 with open(datafile) as fil:
  database = json.load(fil)

if not os.path.isfile(configfile):
 print('No config file!')
with open(configfile) as fil:
 config = json.load(fil)

mqtt_broker = config['server']
mqtt_username = config['username']
mqtt_password = config['password']
mqtt_port = config['port']

client = paho_client.Client('mhz433Client_robert')
client.on_message=on_message
client.username_pw_set(username=config['username'],password=config['password'])
client.connect(mqtt_broker,port=mqtt_port)
client.loop_start()

for line in sys.stdin:
 try:
 #for line in sys.stdin:
  msg = json.loads(line)
  idval = str(msg['id'])
  if 'data' in msg:
   idval+=str(msg['data'])
  if idval not in database:
   database.update({idval: {k:v for k,v in msg.items()}})
  else:
   database.update({idval: {k:v for k,v in msg.items()}})

#   dbtime = datetime.datetime.strptime(database[idval]['time'],'%Y-%m-%d %H:%M:%S')
#   curtime = datetime.datetime.strptime(str(msg['time']),'%Y-%m-%d %H:%M:%S')
#   if (curtime-dbtime) < activation_threshold:
   client.publish('rflink',idval)
# for testing:
#   client.publish('rflink',msg)
   print('published')

 except Error as e:
  with open('send_to_mqtt.log','a') as fil: 
   fil.write(str(e))
 finally:
  with open(datafile,'w') as fil: 
   json.dump(database,fil)
  client.disconnect()
  client.loop_stop()

