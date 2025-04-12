import logging
class TaskManager:
    def __init__(self, rules_yaml: dict):
        self.tasks = rules_yaml.get('tasks', {})
        self.rules = rules_yaml.get('rules', {})
    
    def get_task(self, task_id: str):
        return self.tasks.get(task_id, {})
    
    def get_subtasks(self, task_id: str):
        return self.get_task(task_id).get('subtasks', {})
    
    def get_rules_per_task(self):
        task_rules = {}
        for task_group in self.tasks.values():
            for task_id, task_data in task_group.get("subtasks", {}).items():
                task_rules[task_id] = [
                    self.rules[rule_id] for rule_id in task_data.get("rules", []) if rule_id in self.rules
                ]
        return task_rules