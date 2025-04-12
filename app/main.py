import paho.mqtt.client as mqtt
import time
import logging
import json
from yaml_loader import YamlLoader
from product_manager import ProductManager
from task_manager import TaskManager
from mqtt_manager import MQTTSystem
from task_assigment_alg import TaskAssignmentSolver

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
        self.mqtt_connections = MQTTSystem(self.connections_config, self._handle_message)
        self.products_manager = ProductManager(YamlLoader.load_yaml(products_file))
        self.task_manager = TaskManager(YamlLoader.load_yaml(rules_file))
        self.possible_server_tasks = {
            'ws1': ['T1A', 'T1B', 'T1C', 'T1D', 'T1E'],
            'ws2': ['T1C', 'T1D', 'T1E', 'T2'],
            'ws3': ['T3A', 'T3B', 'T3C'],
        }
        self.input_times = { # in the first inicialization, the EWMA(lag) is None, and the atual is the reference value
        "T1A": {"ws1": {"EWMA(lag)": None, "atual": None}},
        "T1B": {"ws1": {"EWMA(lag)": None, "atual": None}},
        "T1C": {"ws1": {"EWMA(lag)": None, "atual": None}, "ws2": {"EWMA(lag)": None, "atual": None}},
        "T1D": {"ws1": {"EWMA(lag)": None, "atual": None}, "ws2": {"EWMA(lag)": None, "atual": None}},
        "T1E": {"ws1": {"EWMA(lag)": None, "atual": None}, "ws2": {"EWMA(lag)": None, "atual": None}},
        "T2A": {"ws2": {"EWMA(lag)": None, "atual": None}},
        "T3A": {"ws3": {"EWMA(lag)": None, "atual": None}},
        "T3B": {"ws3": {"EWMA(lag)": None, "atual": None}},
        "T3C": {"ws3": {"EWMA(lag)": None, "atual": None}}
        }
        self.all_tasks = {}
        self.tasks_assigned = {}
        self.tasks_completed = {}
        self._transform_prods_to_tasks(self.products_manager.get_products())

    def _transform_prods_to_tasks(self, products: dict):
        for product_id, config in products.items():
            product_subtasks = self.map_product_to_tasks(config)
            logging.info(f"Product {product_id} with config {config} mapped to subtasks: {product_subtasks}")
            self.all_tasks[product_id] = product_subtasks
        
    def map_product_to_tasks(self, product: dict):
        subtasks = []
        for color, quantity in product.items():
            task_id = self.products_manager.get_task_by_color(color)
            if task_id:
                subtasks.extend([task_id] * quantity)
        return subtasks
    
    def divide_tasks_among_servers(self, subtasks: list):
        """
        Divide as subtarefas pelos servidores com base nas tasks que eles podem executar.
        """
        server_assignments = {server: [] for server in self.possible_server_tasks}

        for subtask_id, quantity in subtasks:
            assigned = False
            for server, task_list in self.possible_server_tasks.items():
                if subtask_id in task_list:
                    server_assignments[server].append((subtask_id, quantity))
                    assigned = True
                    break
            if not assigned:
                logging.warning(f"Subtask {subtask_id} not assigned to any server.")
        
        return server_assignments
    
    def update_times(inp_times, weight = 0.2):
        """update inpt_times with the EWMA(lag) value"""
        for task, workers in inp_times.items():
            for worker, times in workers.items():
                # if the task time was not updated
                if times["atual"] is None:
                    continue
                # if the task time was updated
                if times["EWMA(lag)"] is None:
                    times["EWMA(lag)"] = times["atual"]
                    times["atual"] = None
                else:
                    # calculate the EWMA(lag) using the formula
                    times["EWMA(lag)"] = (weight * times["atual"]) + ((1-weight) * times["EWMA(lag)"])
                    times["atual"] = None
        return inp_times

    def _handle_message(self, worker_id, msg):  # o suposto é receber a {"tarefa_id": tempo(segundos)}
        try:
            payload = json.loads(msg.payload.decode())
            logging.info(f"Received message from {worker_id}: {payload}")
            # Atualiza os tempos de execução, temos o worker_id, a tarefa_id e o tempo
            for task_id, time in payload.items():
                if task_id in self.input_times:
                    # Atualiza o tempo de execução
                    self.input_times[task_id][worker_id]["atual"] = time
                    logging.info(f"Updated input times for {task_id} on {worker_id}: {time}")
                    print(self.input_times)
                else:
                    logging.warning(f"Received unknown task ID {task_id} from {worker_id}")
        except Exception as e:
            logging.error(f"Erro ao tratar mensagem de {worker_id}: {e}")
    


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
