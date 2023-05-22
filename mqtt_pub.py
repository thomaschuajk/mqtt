import paho.mqtt.client as mqtt_client
import random
import time
import datetime

broker = "localhost"
port = 1883
username = "jk"
password = "password"
topic = "python/mqtt"
sensorId = f"sensor-{random.randint(1,1000)}"


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
    else:
        print(f"Failed to connect, return code {rc}\n")


def publish(client: mqtt_client):
    msg_count = 0
    while True:
        time.sleep(5)
        msg = f"message {msg_count}"
        result = client.publish(topic, msg)
        status = result[0]
        if status == 0:
            print(
                f"{sensorId} publishes `{msg}` to topic `{topic}` at {datetime.datetime.now()}")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1


def create_client(sensorId: str) -> mqtt_client:
    client = mqtt_client.Client(sensorId)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def run():
    publisher = create_client(sensorId)
    publisher.loop_start()
    publish(publisher)


if __name__ == "__main__":
    run()
