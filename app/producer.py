import yaml
import json
import paho.mqtt.client as mqtt
import time
import logging

from yaml_loader import YamlLoader
from mqtt_connection import MQTTConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
            logging.error(f"Erro ao configurar conexões: {e}")
            return {}
    
    def connect_all(self):
        try:
            for conn in self.connections.values():
                if not conn.connect():
                    logging.error(f"Falha ao conectar {conn.broker_id}")
                    return False
            return True
        except Exception as e:
            logging.error(f"Erro ao conectar brokers: {e}")
            return False
    
    def disconnect_all(self):
        for conn in self.connections.values():
            conn.disconnect()
    
    def publish_task(self, broker_id: str, product_id: str):
        if broker_id not in self.connections:
            logging.error(f"Broker {broker_id} não encontrado")
            return False
        
        product = self.products_manager.get_product(product_id)
        task_id = self.broker_task_mapping.get(broker_id)
        task = self.task_manager.get_task(task_id)
        
        if not (product and task):
            logging.error(f"Dados insuficientes para {broker_id}")
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
        
        return self.connections[broker_id].publish(f"v1/devices/me/telemetry", message)
    
    def process_product(self, product_id: str):
        for broker_id in ['ws1', 'ws2', 'ws3']:
            if not self.publish_task(broker_id, product_id):
                logging.error(f"Erro ao processar {product_id} em {broker_id}")
                return False
            time.sleep(1)
        return True

if __name__ == "__main__":

    producer_system = MQTTProducerSystem("../yaml/connections.example.yaml", "../yaml/products.yaml", "../yaml/rules.yaml")
    try:
        if producer_system.connect_all():
            for product_id in ['produtoA', 'produtoB', 'produtoC', 'produtoD']:
                if producer_system.process_product(product_id):
                    logging.info(f"Produto {product_id} processado com sucesso!")
    finally:
        producer_system.disconnect_all()
        logging.info("Sistema desconectado de todos os brokers.")

