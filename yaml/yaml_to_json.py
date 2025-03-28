import yaml
import json

def load_yaml_and_print_json(yaml_file_path):
    """
    Load a YAML file and print its contents as formatted JSON.
    
    Args:
        yaml_file_path (str): Path to the YAML file to be converted
    """
    try:

        # Read the YAML file
        with open(yaml_file_path, 'r', encoding='utf-8') as file:
            yaml_content = file.read()
        
        # Parse the YAML content
        parsed_yaml = yaml.safe_load(yaml_content)
        print(parsed_yaml)
        
        # Convert to JSON with indentation
        json_output = json.dumps(parsed_yaml, indent=2, ensure_ascii=False)
        
        # Print the JSON
        print(json_output)
    
    except FileNotFoundError:
        print(f"Error: File not found at {yaml_file_path}")
    except yaml.YAMLError as e:
        print(f"Error parsing YAML: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Example usage
if __name__ == "__main__":
    # Replace this with the path to your YAML file
    yaml_file_path = 'regras.yaml'
    print("Loading YAML file and converting to JSON...")
    load_yaml_and_print_json(yaml_file_path)