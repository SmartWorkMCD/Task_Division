import paho.mqtt.client as mqtt
import logging
import time
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MQTTConnection:
    def __init__(self, broker_id: str, host: str, port: int, token: str, on_message_callback=None):
        self.broker_id = broker_id
        self.client = mqtt.Client(client_id=f"producer_{broker_id}")
        self.client.username_pw_set(token)
        self.host = host
        self.port = port
        self.connected = False
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish
        self.client.on_subscribe = self.on_subscribe
        self.on_message_callback = on_message_callback
        self.client.on_message = self._internal_on_message
        self.subscribed_topics = [] 

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info(f"Connected to broker {self.broker_id}")
            self.connected = True
        else:
            logging.error(f"Failed to connect to broker {self.broker_id}: {rc}")

    def on_publish(self, client, userdata, mid):
        logging.info(f"Message published to broker {self.broker_id} with ID {mid}")
    
    def on_subscribe(self, client, userdata, mid, granted_qos):
        logging.info(f"Subscribed to broker {self.broker_id} with ID {mid} and QoS {granted_qos}")
    
    def _internal_on_message(self, client, userdata, msg):
        if self.on_message_callback:
            self.on_message_callback(self.broker_id, msg)
        else:
            logging.warning(f"No callback set for message from {self.broker_id}")

    def connect(self):
        try:
            self.client.connect(self.host, self.port)
            self.client.loop_start()
            while not self.connected:
                time.sleep(0.1)
            return True
        except Exception as e:
            logging.error(f"Error connecting to broker {self.broker_id}: {e}")
            return False
        
    def publish(self, topic: str, message: dict):
        try:
            payload = json.dumps(message)
            result = self.client.publish(topic, payload)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logging.info(f"Message published to {topic}: {payload}")
                return True
            else:
                logging.error(f"Failed to publish message to {topic}: {result.rc}")
                return False
        except Exception as e:
            logging.error(f"Error publishing message: {e}")
            return False
        
    def subscribe(self, topic: str):
        try:
            result, mid = self.client.subscribe(topic)
            if result == mqtt.MQTT_ERR_SUCCESS:
                logging.info(f"Subscribed to topic {topic}")
                self.subscribed_topics.append(topic)
                return True
            else:
                logging.error(f"Failed to subscribe to topic {topic}: {result.rc}")
                return False
        except Exception as e:
            logging.error(f"Error subscribing to topic: {e}")
            return False
        
    def unsubscribe(self, topic: str):
        try:
            result = self.client.unsubscribe(topic)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logging.info(f"Unsubscribed from topic {topic}")
                return True
            else:
                logging.error(f"Failed to unsubscribe from topic {topic}: {result.rc}")
                return False
        except Exception as e:
            logging.error(f"Error unsubscribing from topic: {e}")
            return False
        

    def disconnect(self):
        try:
            self.client.unsubscribe(self.subscribed_topics)
            self.client.loop_stop()
            self.client.disconnect()
            logging.info(f"Disconnected from broker {self.broker_id}")
        except Exception as e:
            logging.error(f"Error disconnecting from broker {self.broker_id}: {e}")
            return False
        
        return True
