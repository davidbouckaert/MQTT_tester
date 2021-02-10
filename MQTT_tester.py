#!/usr/bin/env python
# coding: utf-8
import json
import logging
import time
import random
import paho.mqtt.client as mqtt
logging.basicConfig(level=logging.INFO)
CLIENT_ID = "vernemq-test-producer"
BROKER_URLS = ["172.17.33.53"]
TOPIC = "ll/effect/start"
TEST_DURATION = 1000
SLEEP_TIME = 0.01
def timer(method):
    def timed(*args, **kw):
        start_time = time.time()
        result = method(*args, **kw)
        logging.info(f"Function {method.__name__.upper()} took {round((time.time() - start_time) * 1000, 3)}ms")
        return result
    return timed
class MqttClient:
    def __init__(self, broker_list, client_id, sub_topics=[], username=None, password=None):
        # Set variables
        self.broker_list = broker_list
        self.client_id = client_id
        self.username = username
        self.password = password
        self.sub_topics = sub_topics
        # Create MQTT client
        self.client = mqtt.Client(client_id=self.client_id)
        if username:
            self.client.username_pw_set(self.username, self.password)
        self.client.on_message = self.on_message
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        # Random broker ip for load balancing
        self.current_broker_idx = random.randint(0, len(broker_list) - 1)
        # Keep track of amount of messages sent/received
        self.msg_rcv_counter = 0
        self.msg_sent_counter = 0
        self.clean_disconnect = False
        self.connected = False
    def connect(self):
        # try to connect to brokers
        for _ in range(len(self.broker_list)):
            try:
                logging.info(f"trying to connect to {self.broker_list[self.current_broker_idx]}")
                self.client.connect(self.broker_list[self.current_broker_idx])
                return
            except Exception as e:
                logging.warning(f"unable to connect to broker {self.broker_list[self.current_broker_idx]}")
                logging.error(e)
            self.current_broker_idx = (self.current_broker_idx + 1) % len(self.broker_list)
        logging.error("unable to connect to any broker - retrying in 60 seconds")
        time.sleep(60)
        self.connect()
    def disconnect(self):
        self.clean_disconnect = True
    def on_message(self, client, userdata, message):
        try:
            self.msg_rcv_counter += 1
            msg = json.loads(message.payload.decode('utf-8'))
            logging.info(f"msg received: {msg}")
        except Exception as e:
            logging.error(e)
            exit(1)
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            try:
                client.connected_flag = True
                self.connected = True
                logging.info(f"successfully connected to {self.broker_list[self.current_broker_idx]}")
                if len(self.sub_topics):
                    self.client.subscribe(*self.sub_topics)
            except Exception as ex:
                logging.error(ex)
        else:
            logging.warning("bad connect! ")
            client.bad_connection_flag = True
    def on_disconnect(self, client, userdata, flags, rc=0):
        if self.clean_disconnect:
            logging.info(f"disconnected from broker {self.broker_list[self.current_broker_idx]}")
            return
        else:
            logging.warning(f"disconnected from broker {self.broker_list[self.current_broker_idx]} - reconnecting ...")
            self.connect()
    def on_log(self, client, userdata, level, buf):
        logging.debug("log: ", buf)
    def publish(self, topic, payload, qos=0):
        self.client.publish(topic, payload, qos)
        self.msg_sent_counter += 1
    def start(self):
        self.client.loop_start()
lah_payloads = [{
    "effect_type": "lah",
    "target_type": "customrange",
    "custom_device_effects": [
        {
            "lldevice_id": 4,
            "effects": [
                {
                    "roomstate_id": 68,
                    "lleffect_id": 15
                }
            ]
        }
    ]
},
{
    "effect_type": "lah",
    "target_type": "customrange",
    "custom_device_effects": [
        {
            "lldevice_id": 4,
            "effects": []
        }
    ]
}]
def run_mqtt_producer():
    # mqtt client init
    mqtt_client = MqttClient(BROKER_URLS, CLIENT_ID, sub_topics=[], username="Lynx", password="LynX1234")
    # connect mqtt & listen
    mqtt_client.connect()
    mqtt_client.start()
    connect_try = 0
    while not mqtt_client.connected and connect_try < 10:
        time.sleep(2)
    start = time.time()
    try:
        # while time.time() < start + TEST_DURATION:
        for i in range(75000):
            payload = lah_payloads[i % 2]
            mqtt_client.publish(TOPIC, json.dumps(payload), qos=1)
            if (i % 500) == 0:
                print(f"{i} effects send in {round(time.time() - start)} seconds!")
            time.sleep(SLEEP_TIME)
    except KeyboardInterrupt:
        pass
    finally:
        end = time.time()
        logging.info(f"test took {round(end - start)} seconds")
        logging.info(f"sent {mqtt_client.msg_sent_counter} messages")
        logging.info(f"received {mqtt_client.msg_rcv_counter} messages")
        mqtt_client.disconnect()
        exit()
if __name__ == "__main__":
    run_mqtt_producer()
