import yaml
import json
import paho.mqtt.client as mqtt
import time
import logging

from yaml_loader import YamlLoader
from mqtt_connection import MQTTConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TOPIC_PUBLISH_TASKS = "v1/devices/me/attributes"
TOPIC_SUBSCRIBE_BRAIN = "v1/devices/me/telemetry"

REFERENCE_VALUES = {
    'T1.A': 10,
    'T1.B': 10,
    'T1.C': 10,
    'T1.D': 10,
    'T1.E': 10,
    'T2': 30,
    'T3.A': 6,
    'T3.B': 7,
    'T3.C': 5,
}


class TaskManager:
    def __init__(self, rules_yaml: dict):
        self.tasks = rules_yaml.get('tasks', {})
        self.rules = rules_yaml.get('rules', {})
    
    def get_task(self, task_id: str):
        return self.tasks.get(task_id, {})
    
    def get_subtasks(self, task_id: str):
        return self.get_task(task_id).get('subtasks', {})
    
    def get_rules(self, subtask: dict):
        return [self.rules.get(rule) for rule in subtask.get('rules', [])]


class ProductManager:
    def __init__(self, products_yaml: dict):
        self.products = products_yaml.get('produtos', {})
    
    def get_product(self, product_id: str):
        return self.products.get(product_id, {})


class MQTTProducerSystem:
    def __init__(self, connections_file: str, products_file: str, rules_file: str):
        self.connections_config = YamlLoader.load_yaml(connections_file)
        self.products_manager = ProductManager(YamlLoader.load_yaml(products_file))
        self.task_manager = TaskManager(YamlLoader.load_yaml(rules_file))
        self.broker_task_mapping = {'ws1': 'Task1', 'ws2': 'Task2', 'ws3': 'Task3'}
        self.connections = {}
        self._setup_connections()

    def _setup_connections(self):
        try:
            for broker_id, conn in self.connections_config['server'].items():
                self.connections[broker_id] = MQTTConnection(broker_id, conn['host'], conn['port'], conn['token'])
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
            return True
        except Exception as e:
            logging.error(f"Error subscribing to topics: {e}")
            return False

    def process_product(self, product_id: str):
        for broker_id in ['ws1', 'ws2', 'ws3']:
            product = self.products_manager.get_product(product_id)
            task_id = self.broker_task_mapping.get(broker_id)
            task = self.task_manager.get_task(task_id)

            if not (product and task):
                logging.error(f"Insufficient data for {broker_id}")
                return False

            message = {
                'task_id': task_id,
                'task_description': task.get('description'),
                'product_id': product_id,
                'product_name': product.get('name'),
                'product_config': product.get('config'),
                'subtasks': {
                    sub_id: {**sub, 'rules': self.task_manager.get_rules(sub)}
                    for sub_id, sub in self.task_manager.get_subtasks(task_id).items()
                }
            }

            if not self.connections[broker_id].publish(TOPIC_PUBLISH_TASKS, message):
                logging.error(f"Error publishing task to {broker_id}")
                return False
            time.sleep(1)  # Delay to avoid flooding the brokers
        return True


if __name__ == "__main__":
    producer_system = MQTTProducerSystem("../yaml/connections.example.yaml", "../yaml/products.yaml", "../yaml/rules.yaml")

    try:
        if producer_system.connect_all():
            if producer_system.subscribe_all():
                logging.info("Subscribed to all brokers successfully.")

            logging.info("Application is running. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)  
    except KeyboardInterrupt:
        logging.info("Application stopped by user.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        producer_system.disconnect_all()
        logging.info("Disconnected from all brokers.")
