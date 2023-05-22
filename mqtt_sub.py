import paho.mqtt.client as mqtt_client
import random
import time
import datetime

broker = "localhost"
port = 1883
username = "jk"
password = "password"
topic = "python/mqtt"
clientId = f"client-{random.randint(1,1000)}"


def on_message(client, userdata, msg):
    # print(f"{clientId} receives:`{msg.payload.decode()} from `{msg.topic}` at {datetime.datetime.now()}")
    # publish msg to kafka
    #{'dummy_id':1, 'lidar_timestamp':timestamp, 'lidar_data':str(data)} --> format to be published to kafka
    # str 


def subscribe(client: mqtt_client):
    client.subscribe(topic)
    client.on_message = on_message


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT broker")
        else:
            print(f"Failed to connect, return code {rc}\n")
    client = mqtt_client.Client()
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == "__main__":
    run()
