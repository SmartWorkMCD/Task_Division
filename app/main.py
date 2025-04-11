import paho.mqtt.client as mqtt
import time
import logging

from yaml_loader import YamlLoader
from product_manager import ProductManager
from task_manager import TaskManager
from mqtt_manager import MQTTSystem

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

class TaskDivisionManager:
    def __init__(self, connections_file: str, products_file: str, rules_file: str):
        self.connections_config = YamlLoader.load_yaml(connections_file)
        self.mqtt_connections = MQTTSystem(self.connections_config)
        self.products_manager = ProductManager(YamlLoader.load_yaml(products_file))
        self.task_manager = TaskManager(YamlLoader.load_yaml(rules_file))
        self.possible_server_tasks = {
            'W1': ['T1A', 'T1B', 'T1C', 'T1D', 'T1E'],
            'W2': ['T1C', 'T1D', 'T1E', 'T2'],
            'W3': ['T3A', 'T3B', 'T3C'],
        }
        self.input_times = {
        "T1A": {"W1": {"EWMA(lag)": None, "atual": None}},
        "T1B": {"W1": {"EWMA(lag)": None, "atual": None}},
        "T1C": {"W1": {"EWMA(lag)": None, "atual": None},
                "W2": {"EWMA(lag)": None, "atual": None}},
        "T1D": {"W1": {"EWMA(lag)": None, "atual": None},
                "W2": {"EWMA(lag)": None, "atual": None}},
        "T1E": {"W1": {"EWMA(lag)": None, "atual": None},
                "W2": {"EWMA(lag)": None, "atual": None}},
        "T2": {"W2": {"EWMA(lag)": None, "atual": None}},
        "T3A": {"W3": {"EWMA(lag)": None, "atual": None}},
        "T3B": {"W3": {"EWMA(lag)": None, "atual": None}},
        "T3C": {"W3": {"EWMA(lag)": None, "atual": None}},
        }

if __name__ == "__main__":
    producer_system = TaskDivisionManager("../yaml/connections.example.yaml", "../yaml/products.yaml", "../yaml/rules.yaml")

    try:

        logging.info("Application is running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)  
    except KeyboardInterrupt:
        logging.info("Application stopped by user.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        producer_system.mqtt_connections.disconnect_all()
        logging.info("Disconnected from all brokers.")
