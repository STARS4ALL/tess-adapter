author = 'egonzalez'

import paho.mqtt.client as mqtt
import json
from pymongo import MongoClient
from datetime import datetime
import config

dbClient = MongoClient(config.MONGO_HOST)
db = dbClient.photometers

def on_connect(client, userdata, flags, rc):
	print("Connected with result code:"+str(rc))
	client.subscribe(config.MOSQUITTO_TOPIC)

def on_message(client, data, msg):
	print(msg.topic+" "+str(msg.payload))
	timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        message = json.loads(msg.payload)
	message["tstamp"]=timestamp
	db.observations.insert_one(message)
	print("Message inserted:"+str(message))		
		
		
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.username_pw_set(config.MOSQUITTO_USER,config.MOSQUITTO_PASSWORD)
client.connect(config.MOSQUITTO_HOST,config.MOSQUITTO_PORT,config.MOSQUITTO_RECONNECT)
print("Conecting with brocker")

client.loop_forever()


