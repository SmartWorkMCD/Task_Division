import paho.mqtt.client as mqtt
import logging
from mqtt_connection import MQTTConnection

TOPIC_PUBLISH_TASKS = "v1/devices/me/attributes"
TOPIC_SUBSCRIBE_BRAIN = "v1/devices/me/telemetry"

class MQTTSystem:
    def __init__(self, connections_config: dict, message_handler=None):
        self.connections_config = connections_config
        self.message_handler = message_handler
        self.connections = {}
        self._setup_connections()
        if not self.connect_all():
            logging.error("Failed to connect to all brokers.")
        elif not self.subscribe_all():
            logging.error("Failed to subscribe to all brokers.")
        else:
            logging.info("Connected and subscribed to all brokers successfully.")

    def _setup_connections(self):
        try:
            for broker_id, conn in self.connections_config['server'].items():
                connection = MQTTConnection(broker_id, conn['host'], conn['port'], conn['token'], self.message_handler)
                self.connections[broker_id] = connection
            return self.connections
        except Exception as e:
            logging.error(f"Error setting up connections: {e}")
            return {}

    def connect_all(self):
        try:
            for conn in self.connections.values():
                if not conn.connect():
                    logging.error(f"Failed to connect {conn.broker_id}")
                    return False
            return True
        except Exception as e:
            logging.error(f"Error connecting brokers: {e}")
            return False

    def disconnect_all(self):
        for conn in self.connections.values():
            conn.disconnect()

    def subscribe_all(self):
        try:
            for conn in self.connections.values():
                if not conn.subscribe(TOPIC_SUBSCRIBE_BRAIN):
                    logging.error(f"Failed to subscribe on {conn.broker_id}")
                    return False
            logging.info("Subscribed to all brokers successfully.")
            return True
        except Exception as e:
            logging.error(f"Error subscribing to topics: {e}")
            return False
