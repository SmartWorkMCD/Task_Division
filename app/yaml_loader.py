import yaml
import logging

class YamlLoader:
    def load_yaml(path):
        try:
            with open(path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            logging.error(f"File not found: {path}")
            return None
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML file: {e}")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            return None
