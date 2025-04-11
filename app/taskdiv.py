import numpy as np
import pulp

# %%
class WorkersInfo:
    def __init__(self, data):
        if not isinstance(data, dict):
            raise TypeError("Input must be a dictionary.")
        
        self.data = data
        self.unique_tasks = len(data)
        
        self.workers = {worker for task in data.values() for worker in task.keys()}
        self.encoded_workers = {w: i for i, w in enumerate(self.workers)}
        self.unique_workers = len(self.workers)


# %%
class TimesMatrix:
    def __init__(self, workers_times, tasks):
        if not isinstance(workers_times, WorkersInfo):
            raise TypeError("workers_times must be an instance of WorkersInfo.")
        if not isinstance(tasks, list):
            raise TypeError("tasks must be a list.")

        # matrix shape
        self.total_tasks = len(tasks)
        self.total_workers = workers_times.unique_workers

        # matrix content
        self.tasks = tasks
        self.times = workers_times.data
        
        # encode workers for posterior use
        self.encoded_workers = workers_times.encoded_workers
        
        # build the matrix
        self.matrix = self._build_matrix()

    def _build_matrix(self):
        M = np.full((self.total_tasks, self.total_workers), np.inf)
        for i, task in enumerate(self.tasks):
            for worker, time in self.times[task].items():
                j = self.encoded_workers[worker]
                M[i, j] = time          
        return M
    
    def __array__(self, dtype=float):
        """Allows np.array(instance) to return the matrix."""
        return self.matrix.astype(dtype) if dtype else self.matrix
        

# %%
def solve_task_assignment(times_matrix):
    """
    Solve the task assignment problem using MILP to minimize the maximum worker time.
    Args:
        times_matrix (TimesMatrix class): A 2D numpy array representing the task times for each worker.
    Returns:
        optimal_value (float): The minimized maximum worker time.
        optimal_x (np.ndarray): A binary matrix indicating the assignment of tasks to workers.
    """
    M = np.array(times_matrix)
    num_rows, num_cols = M.shape


    # Define the MILP problem:
    prob = pulp.LpProblem("Minimize_Max_Col_Sum", pulp.LpMinimize)


    # Variables:
    # x_{i,j} = 1 if task i is assigned to worker j, 0 otherwise
    x = [[pulp.LpVariable(f"x_{i}_{j}", cat="Binary") for j in range(num_cols)] for i in range(num_rows)]

    # z = max col sum aka worker time
    z = pulp.LpVariable("z", lowBound=0)


    # Constraints:
    # ensure exactly one selection per task (row)
    for i in range(num_rows):
        prob += pulp.lpSum(x[i][j] for j in range(num_cols)) == 1

    # prohibit selections where M[i, j] = np.inf (unavailable tasks)
    for i in range(num_rows):
        for j in range(num_cols):
            if not np.isfinite(M[i, j]):  # If the matrix element is np.inf
                prob += x[i][j] == 0  # Force x[i][j] to be 0

    # ensure that the sum of the times for each worker is less than or equal to z
    for j in range(num_cols):  
        column_sum = pulp.lpSum(M[i, j] * x[i][j] for i in range(num_rows) if np.isfinite(M[i, j]))
        prob += z >= column_sum 


    # Objective:
    # minimize z, the maximum row sum aka the maximum worker time
    prob += z


    # Solve the problem:
    # solving
    prob.solve(pulp.PULP_CBC_CMD(msg=False))

    # results
    optimal_value = pulp.value(z)
    optimal_x = np.array([[pulp.value(x[i][j]) for j in range(num_cols)] for i in range(num_rows)], dtype=int)

    return optimal_value, optimal_x


# %%
def print_assignments_list(assignment_matrix, workers_info, tasks):
    """
    Print the task assignments for each worker based on the assignment matrix.
    Args:
        assignment_matrix (np.ndarray): The binary matrix indicating task assignments.
        workers_info (WorkersInfo): The WorkersInfo instance containing worker information.
        tasks (list): The list of tasks.
    Returns:
        task_distribution (dict): A dictionary mapping each worker to their assigned tasks.
    """
    decoded_workers = {i: w for w, i in workers_info.encoded_workers.items()}

    task_distribution = {w: [] for w in workers_info.workers}
    for i, task in enumerate(tasks):

        # column where x[i][j] == 1
        j = np.argmax(assignment_matrix[i])

        # get the worker name
        worker_name = decoded_workers[j]

        # add the task to the worker's list
        task_distribution[worker_name].append(task)

    return task_distribution


# %%
if __name__ == "__main__":
    inp_times = {
        "T1A": {"W1": 2},
        "T1B": {"W1": 4},
        "T1C": {"W1": 6,
                "W2": 3},
        "T2": {"W2": 9},
        "T3A": {"W3": 2},
        "T3B": {"W3": 2},
        "T3C": {"W3": 2},
    }
    inp_tasks = ["T1A", "T1B", "T1C", "T1C", "T1C", "T2", "T3A", "T3B", "T3C"]


    workers_info = WorkersInfo(inp_times)
    times_matrix = TimesMatrix(workers_info, inp_tasks)

    optimal_value, assignment_matrix = solve_task_assignment(times_matrix)
    
    task_distribution = print_assignments_list(assignment_matrix, workers_info, inp_tasks)
    print("Optimal value:", optimal_value)
    print("Task distribution:")
    for worker, tasks in task_distribution.items():
        print(f"{worker}: {tasks}")