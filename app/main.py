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
    'T1.A': 20,
    'T1.B': 20,
    'T1.C': 10,
    'T1.D': 10,
    'T1.E': 10,
    'T2': 10,
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
        self.input_times = { # in the first initialization, the EWMA(lag) is None, and the atual is the reference value
            "T1A": {"ws1": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.A']}},
            "T1B": {"ws1": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.B']}},
            "T1C": {"ws1": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.C']}, "ws2": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.C']}},
            "T1D": {"ws1": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.D']}, "ws2": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.D']}},
            "T1E": {"ws1": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.E']}, "ws2": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.E']}},
            "T2A": {"ws2": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T2']}},
            "T3A": {"ws3": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T3.A']}},
            "T3B": {"ws3": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T3.B']}},
            "T3C": {"ws3": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T3.C']}}
        }
        self.input_times = self.update_times(self.input_times)
        self.all_tasks = {}
        self.tasks_assigned = {}
        self.products_assigned = {}
        self.tasks_completed = {}
        self.products_completed = {}
        self._transform_prods_to_tasks(self.products_manager.get_products())
        self.first_run(list(self.all_tasks.values())[:5], list(self.all_tasks.keys())[:5])

    def first_run(self, first5, list_ids):
        solver = TaskAssignmentSolver(self.input_times, first5)
        self.optimal_value, assignment_matrix = solver.solve()
        tsk = solver.get_assignments_list()
        self.tasks_assigned = tsk
        for i in list_ids:
            self.products_assigned[i] = self.products_manager.get_product(i)
        self.send_task(self.tasks_assigned, self.products_assigned, self.task_manager.get_rules_per_task())


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
            # No fim adiciona sempre a tarefa T2, T3A, T3B e T3C
        subtasks.extend(['T2A'] * 1)
        subtasks.extend(['T3A'] * 1)
        subtasks.extend(['T3B'] * 1)
        subtasks.extend(['T3C'] * 1)
        return subtasks
    
    def update_times(self, inp_times, weight = 0.2):
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
            for task_id, time in payload.items():
                if task_id in self.input_times:
                    self.input_times[task_id][worker_id]["atual"] = time
                    logging.info(f"Updated input times for {task_id} on {worker_id}: {time}")
                    self.input_times = self.update_times(self.input_times)
                else:
                    logging.warning(f"Received unknown task ID {task_id} from {worker_id}")
        except Exception as e:
            logging.error(f"Erro ao tratar mensagem de {worker_id}: {e}")

    
    def send_task(self, assignment: dict, products: dict, rules: list = None):
        try:
            for worker_id, task_list in assignment.items():
                payload = {"tasks": task_list}
                if worker_id =='ws2':
                    payload["products"] = products
                if rules:
                    payload["rules"] = rules
                self.mqtt_connections.send_task(worker_id, payload)
                logging.info(f"Sent task to {worker_id}: {task_list}")
        except Exception as e:
            logging.error(f"Error sending task to {worker_id}: {e}")
    


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
