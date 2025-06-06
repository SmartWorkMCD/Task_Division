import paho.mqtt.client as mqtt
import time
import logging
import json
import os
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
    'T2': 10,
    'T3.A': 6,
    'T3.B': 7,
    'T3.C': 5,
}

class TaskDivisionManager:
    def __init__(self, connections_file: str, products_file: str, rules_file: str, number_remaining: int = None, number_next_products: int = None, weight: float = None):
        # Get values from environment variables if not provided
        if number_remaining is None:
            number_remaining = int(os.environ.get('number_remaining_before_assign', 2))
        if number_next_products is None:
            number_next_products = int(os.environ.get('number_next_products_to_assign_tasks', 3))
        if weight is None:
            weight = float(os.environ.get('EWMA_weight', 0.2))
        self.number_next_products = number_next_products
        self.tasks_remaining = number_remaining
        self.weight = weight
        self.connections_config = YamlLoader.load_yaml(connections_file)
        self.mqtt_connections = MQTTSystem(self.connections_config, self._handle_message) 
        self.products_manager = ProductManager(YamlLoader.load_yaml(products_file))
        self.task_manager = TaskManager(YamlLoader.load_yaml(rules_file))
        self.possible_server_tasks = {
            'ws1': ['T1A', 'T1B', 'T1C', 'T1D'],
            'ws2': ['T1C', 'T1D', 'T2'],
            'ws3': ['T3A', 'T3B', 'T3C'],
        }
        self.input_times = { # in the first initialization, the EWMA(lag) is None, and the atual is the reference value
            "T1A": {"ws1": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.A']}},
            "T1B": {"ws1": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.B']}},
            "T1C": {"ws1": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.C']}, "ws2": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.C']}},
            "T1D": {"ws1": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.D']}, "ws2": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T1.D']}},
            "T2A": {"ws2": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T2']}},
            "T3A": {"ws3": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T3.A']}},
            "T3B": {"ws3": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T3.B']}},
            "T3C": {"ws3": {"EWMA(lag)": None, "atual": REFERENCE_VALUES['T3.C']}}
        }
        self.input_times = self.update_times(self.input_times, weight=self.weight)
        self.all_tasks = {}
        self.tasks_assigned = {}
        self.products_assigned = {}
        self.tasks_in_progress = {
            "ws1": {"assigned": {}, "completed": []},
            "ws2": {"assigned": {}, "completed": [], "products_completed": []},
            "ws3": {"assigned": {}, "completed": []},
        }
        self.tasks_completed = {}
        self.products_completed = {}
        self._transform_prods_to_tasks(self.products_manager.get_products())
        self.first_run(list(self.all_tasks.values())[:self.number_next_products], list(self.all_tasks.keys())[:self.number_next_products])

    def first_run(self, first5, list_ids):
        solver = TaskAssignmentSolver(self.input_times, first5)
        self.optimal_value, assignment_matrix = solver.solve()
        tsk = solver.get_assignments_list()
        
        # Map generic product_ids to actual product IDs
        self.tasks_assigned = {}
        for worker, products_dict in tsk.items():
            self.tasks_assigned[worker] = {}
            for i, (generic_id, tasks) in enumerate(products_dict.items()):
                if i < len(list_ids):
                    product_id = list_ids[i]
                    self.tasks_assigned[worker][product_id] = tasks
        
        for i in list_ids:
            self.products_assigned[i] = self.products_manager.get_product(i)
        
        self.send_task(self.tasks_assigned, self.products_assigned)

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
    
    def update_times(self, inp_times, weight = None):
        if weight is None:
            weight = self.weight if hasattr(self, 'weight') else 0.2
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
    
    def _handle_message(self, worker_id, msg): 
        try:  # o suposto é receber a {"tarefa_id": tempo(segundos)}, quando é um T2A tem de receber assim {"tarefa_id": tempo(segundos), "produto_id": produto_id}
            payload = json.loads(msg.payload.decode())
            logging.info(f"Received message from {worker_id}: {payload}")
            
            for task_id, time in payload.items():
                if task_id == "produto_id":
                    continue  # Skip this field as it's not a task
                
                if task_id in self.input_times:
                    self.input_times[task_id][worker_id]["atual"] = time
                    self.input_times = self.update_times(self.input_times)
                
                if worker_id in self.tasks_in_progress:
                    # Add to general completed tasks list
                    self.tasks_in_progress[worker_id]["completed"].append(task_id)
                    
                    # Handle product completion for ws2
                    if worker_id == 'ws2' and task_id == "T2A" and "produto_id" in payload:
                        pid = payload["produto_id"]
                        self.tasks_in_progress[worker_id]["products_completed"].append(pid)
                        logging.info(f"Marked product {pid} as completed by {worker_id}")
                    
                    logging.info(f"Marked {task_id} as completed by {worker_id}")
                    logging.debug(f"Tasks in progress: {self.tasks_in_progress}")
                else:
                    logging.warning(f"Received unknown task ID {task_id} from {worker_id}")
            
            self.check_and_assign_new_tasks()
        except Exception as e:
            logging.error(f"Erro ao tratar mensagem de {worker_id}: {e}")

    def check_and_assign_new_tasks(self):
        try:
            for ws, progress in self.tasks_in_progress.items():
                # Count remaining tasks from the assigned product-task dictionary
                assigned_tasks_count = 0
                assigned_flat = []
                
                # Create a flattened list of all assigned tasks for this worker
                for product_id, tasks in progress["assigned"].items():
                    assigned_flat.extend(tasks)
                
                # Calculate remaining tasks
                completed = progress["completed"]
                remaining = [task for task in assigned_flat if task not in completed]
                remaining_count = len(remaining)
                
                logging.info(f"{ws} has {remaining_count} tasks remaining.")

                if remaining_count <= self.tasks_remaining:  # tarefas a faltar antes de atribuir novas
                    remaining_product_ids = [
                        pid for pid in self.all_tasks.keys()
                        if pid not in self.products_assigned
                    ]
                    if not remaining_product_ids:
                        logging.info(f"No more products to assign.")
                        continue
                    
                    # Seleciona próximos produtos
                    new_product_ids = remaining_product_ids[:self.number_next_products]
                    new_tasks = [self.all_tasks[pid] for pid in new_product_ids]
                    
                    # Resolve
                    solver = TaskAssignmentSolver(self.input_times, new_tasks)
                    self.optimal_value, assignment_matrix = solver.solve()
                    
                    # Get new assignments but need to map generic product IDs to real ones
                    generic_assignments = solver.get_assignments_list()
                    new_assignments = {}
                    
                    # Map the generic product IDs to actual product IDs
                    for worker, products_dict in generic_assignments.items():
                        new_assignments[worker] = {}
                        for i, (generic_id, tasks) in enumerate(products_dict.items()):
                            if i < len(new_product_ids):
                                product_id = new_product_ids[i]
                                new_assignments[worker][product_id] = tasks
                    
                    # Update products_assigned
                    for pid in new_product_ids:
                        self.products_assigned[pid] = self.products_manager.get_product(pid)

                    # Process completed tasks
                    temp = ['ws1', 'ws2', 'ws3']
                    try:
                        for t in temp:
                            # Process completed tasks
                            for task_id in list(self.tasks_in_progress[t]["completed"]):
                                # Find which product this task belongs to
                                for product_id, tasks in list(self.tasks_in_progress[t]["assigned"].items()):
                                    if task_id in tasks:
                                        # Remove task from that product
                                        self.tasks_in_progress[t]["assigned"][product_id].remove(task_id)
                                        logging.info(f"Removed completed task {task_id} from product {product_id}.")
                                        
                                        # If all tasks for this product are complete, clean up the product entry
                                        if not self.tasks_in_progress[t]["assigned"][product_id]:
                                            del self.tasks_in_progress[t]["assigned"][product_id]
                                            logging.info(f"All tasks for product {product_id} completed by {t}.")
                                        break
                                
                                # Process completed products for ws2
                                if t == 'ws2':
                                    for pid in list(self.tasks_in_progress[t]["products_completed"]):
                                        self.products_completed[pid] = self.products_manager.get_product(pid)
                                        self.tasks_in_progress[t]["products_completed"].remove(pid)
                                        logging.info(f"Product {pid} marked as completed.")
                                
                                # Move task from completed to tasks_completed tracking
                                self.tasks_in_progress[t]["completed"].remove(task_id)
                                
                                if t not in self.tasks_completed:
                                    self.tasks_completed[t] = []
                                
                                self.tasks_completed[t].append(task_id)
                                logging.info(f"Task {task_id} marked as completed.")
                    except Exception as e:
                        logging.error(f"Error removing completed tasks: {e}")
                    
                    # Send new tasks to workers
                    self.send_task(new_assignments, self.products_assigned)
                    break
            
            logging.debug(f"Current tasks in progress: {self.tasks_in_progress}")
        except Exception as e:
            logging.error(f"Error checking and assigning new tasks: {e}")

    def send_task(self, assignment: dict, products: dict):
        try:
            for worker_id, products_tasks in assignment.items():
                # The assignment is now a dictionary of product_id -> task_list
                payload = {"tasks": products_tasks}
                if worker_id == 'ws2':
                    payload["products"] = products
                
                self.mqtt_connections.send_task(worker_id, payload)
                
                # Update tasks_in_progress with product-task mapping
                for product_id, tasks in products_tasks.items():
                    self.tasks_in_progress[worker_id]["assigned"][product_id] = tasks
                
                logging.info(f"Sent task to {worker_id}: {products_tasks}")
        except Exception as e:
            logging.error(f"Error sending task to {worker_id}: {e}")
    


if __name__ == "__main__":
    producer_system = TaskDivisionManager("../yaml/connections.yaml", "../yaml/products.yaml", "../yaml/rules.yaml")

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
