import numpy as np
import pulp

class TaskAssignmentSolver:
    def __init__(self, input_times: dict, input_tasks: list):
        """
        Args:
            input_times (dict): Dict com estrutura {task: {worker: {"EWMA(lag)": float}}}
            input_tasks (list): Lista de listas de tarefas (ex: [['T1A', 'T1B'], ['T2']])
        """
        self.workers_info = self.WorkersInfo(input_times)
        self.tasks = input_tasks
        self.times_matrix = self.TimesMatrix(self.workers_info, self.tasks)
        self.assignment_matrix = None
        self.optimal_value = None

    class WorkersInfo:
        def __init__(self, data):
            if not isinstance(data, dict):
                raise TypeError("Input must be a dictionary.")
            
            self.data = data
            self.unique_tasks = len(data)
            self.workers = {worker for task in data.values() for worker in task.keys()}
            self.encoded_workers = {w: i for i, w in enumerate(self.workers)}
            self.unique_workers = len(self.workers)

    class TimesMatrix:
        def __init__(self, workers_times, tasks):
            if not isinstance(workers_times, TaskAssignmentSolver.WorkersInfo):
                raise TypeError("workers_times must be an instance of WorkersInfo.")
            if not isinstance(tasks, list):
                raise TypeError("tasks must be a list.")

            self.flattened_tasks = [task for sublist in tasks for task in sublist]
            self.times = workers_times.data
            self.total_tasks = len(self.flattened_tasks)
            self.total_workers = workers_times.unique_workers
            self.encoded_workers = workers_times.encoded_workers
            self.matrix = self._build_matrix()

        def _build_matrix(self):
            M = np.full((self.total_tasks, self.total_workers), np.inf)
            for i, task in enumerate(self.flattened_tasks):
                for worker, time in self.times[task].items():
                    j = self.encoded_workers[worker]
                    M[i, j] = time["EWMA(lag)"]
            return M
        
        def __array__(self, dtype=float):
            return self.matrix.astype(dtype) if dtype else self.matrix

    def solve(self):
        """
        Solve the task assignment problem using MILP to minimize the maximum worker time.
        Args:
            times_matrix (TimesMatrix class): A 2D numpy array representing the task times for each worker.
        Returns:
            optimal_value (float): The minimized maximum worker time.
            optimal_x (np.ndarray): A binary matrix indicating the assignment of tasks to workers.
        """
        M = np.array(self.times_matrix)
        num_rows, num_cols = M.shape
        prob = pulp.LpProblem("Minimize_Max_Col_Sum", pulp.LpMinimize)

        # Variáveis
        x = [[pulp.LpVariable(f"x_{i}_{j}", cat="Binary") for j in range(num_cols)] for i in range(num_rows)]
        z = pulp.LpVariable("z", lowBound=0)

        # Restrições
        for i in range(num_rows):
            prob += pulp.lpSum(x[i][j] for j in range(num_cols)) == 1

        for i in range(num_rows):
            for j in range(num_cols):
                if not np.isfinite(M[i, j]):
                    prob += x[i][j] == 0

        for j in range(num_cols):
            col_sum = pulp.lpSum(M[i, j] * x[i][j] for i in range(num_rows) if np.isfinite(M[i, j]))
            prob += z >= col_sum

        # Objetivo
        prob += z

        # Solver
        prob.solve(pulp.PULP_CBC_CMD(msg=False))
        self.optimal_value = pulp.value(z)
        self.assignment_matrix = np.array([[pulp.value(x[i][j]) for j in range(num_cols)] for i in range(num_rows)], dtype=int)
        return self.optimal_value, self.assignment_matrix

    def get_assignments_list(self):
        """
        Print the task assignments for each worker based on the assignment matrix.
        
        Args:
            assignment_matrix (np.ndarray, optional): The binary matrix indicating task assignments.
                If None, uses self.assignment_matrix
            workers_info (WorkersInfo, optional): The WorkersInfo instance containing worker information.
                If None, uses self.workers_info
            tasks (list, optional): The list of tasks. If None, uses self.tasks
                
        Returns:
            task_distribution (dict): A dictionary mapping each worker to their assigned tasks.
        """
        # Use class attributes if parameters not provided
        assignment_matrix = self.assignment_matrix
        workers_info = self.workers_info
        tasks = self.tasks
            
        decoded_workers = {i: w for w, i in workers_info.encoded_workers.items()}
        
        # Initialize task distribution dictionary
        task_distribution = {w: [] for w in workers_info.workers}

        # Organize tasks by product for each worker
        current_task_idx = 0
        for product_idx, product_tasks in enumerate(tasks):

            # Create a mapping of tasks to workers for the current product
            worker_product_tasks = {w: [] for w in workers_info.workers}

            # Assign tasks to workers based on the assignment matrix
            for task in product_tasks:
                worker_idx = np.argmax(assignment_matrix[current_task_idx])
                worker_name = decoded_workers[worker_idx]
                worker_product_tasks[worker_name].append(task)
                current_task_idx += 1

            # Add non-empty product task lists to each worker's assignments
            for worker, worker_tasks in worker_product_tasks.items():
                task_distribution[worker].append(worker_tasks)

        return task_distribution
