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
