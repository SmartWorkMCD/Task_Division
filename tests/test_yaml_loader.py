import unittest
import yaml
from unittest.mock import mock_open, patch
from app.yaml_loader import YamlLoader

class TestYamlLoader(unittest.TestCase):

    @patch("builtins.open", mock_open(read_data="key: value"))
    def test_load_yaml_valid_file(self):
        """Teste se carrega um YAML válido"""
        result = YamlLoader.load_yaml("valid.yaml")
        self.assertIsNotNone(result)
        self.assertEqual(result.get("key"), "value") 

    @patch("builtins.open", mock_open(read_data=": invalid_yaml"))
    def test_load_yaml_invalid_file(self):
        """Teste para um YAML inválido"""
        with patch("yaml.safe_load", side_effect=yaml.YAMLError):
            result = YamlLoader.load_yaml("invalid.yaml")
            self.assertIsNone(result) 

    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_load_yaml_file_not_found(self, mock_open):
        """Teste para arquivo não encontrado"""
        result = YamlLoader.load_yaml("non_existent.yaml")
        self.assertIsNone(result)

if __name__ == "__main__":
    unittest.main()
