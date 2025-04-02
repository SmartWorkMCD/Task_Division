import unittest
from unittest.mock import patch, MagicMock
import json
import paho.mqtt.client as mqtt
import sys
import os

from app.mqtt_connection import MQTTConnection


class TestMQTTConnection(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.broker_id = "test_broker"
        self.host = "mqtt.example.com"
        self.port = 1883
        self.token = "test_token"
        
        # Create a patcher for the mqtt.Client class
        self.client_patcher = patch('paho.mqtt.client.Client')
        self.mock_client_class = self.client_patcher.start()
        self.mock_client = MagicMock()
        self.mock_client_class.return_value = self.mock_client
        
        # Create the MQTTConnection instance
        self.mqtt_connection = MQTTConnection(self.broker_id, self.host, self.port, self.token)
    
    def tearDown(self):
        """Tear down test fixtures after each test method."""
        self.client_patcher.stop()
    
    def test_init(self):
        """Test the initialization of MQTTConnection."""
        self.assertEqual(self.mqtt_connection.broker_id, self.broker_id)
        self.assertEqual(self.mqtt_connection.host, self.host)
        self.assertEqual(self.mqtt_connection.port, self.port)
        self.assertFalse(self.mqtt_connection.connected)
        self.mock_client_class.assert_called_once_with(client_id=f"producer_{self.broker_id}")
        self.mock_client.username_pw_set.assert_called_once_with(self.token)
        
    def test_on_connect_success(self):
        """Test the on_connect callback when connection is successful."""
        self.mqtt_connection.on_connect(None, None, None, 0)
        self.assertTrue(self.mqtt_connection.connected)
    
    def test_on_connect_failure(self):
        """Test the on_connect callback when connection fails."""
        self.mqtt_connection.on_connect(None, None, None, 1)
        self.assertFalse(self.mqtt_connection.connected)
    
    def test_on_publish(self):
        """Test the on_publish callback."""
        # This test just ensures the method runs without errors
        self.mqtt_connection.on_publish(None, None, 123)
    
    @patch('time.sleep')
    def test_connect_success(self, mock_sleep):
        """Test successful connection to the broker."""
        # Set up the mock to trigger on_connect callback
        def side_effect(*args, **kwargs):
            self.mqtt_connection.connected = True
        
        self.mock_client.connect.return_value = None
        mock_sleep.side_effect = side_effect
        
        result = self.mqtt_connection.connect()
        
        self.mock_client.connect.assert_called_once_with(self.host, self.port)
        self.mock_client.loop_start.assert_called_once()
        self.assertTrue(result)
    
    def test_connect_exception(self):
        """Test connection failure due to exception."""
        self.mock_client.connect.side_effect = Exception("Connection error")
        
        result = self.mqtt_connection.connect()
        
        self.mock_client.connect.assert_called_once_with(self.host, self.port)
        self.assertFalse(result)
    
    def test_publish_success(self):
        """Test successful message publication."""
        topic = "test/topic"
        message = {"key": "value"}
        expected_payload = json.dumps(message)
        
        # Mock the publish result
        mock_result = MagicMock()
        mock_result.rc = mqtt.MQTT_ERR_SUCCESS
        self.mock_client.publish.return_value = mock_result
        
        result = self.mqtt_connection.publish(topic, message)
        
        self.mock_client.publish.assert_called_once_with(topic, expected_payload)
        self.assertTrue(result)
    
    def test_publish_failure(self):
        """Test failed message publication."""
        topic = "test/topic"
        message = {"key": "value"}
        expected_payload = json.dumps(message)
        
        # Mock the publish result
        mock_result = MagicMock()
        mock_result.rc = mqtt.MQTT_ERR_NO_CONN
        self.mock_client.publish.return_value = mock_result
        
        result = self.mqtt_connection.publish(topic, message)
        
        self.mock_client.publish.assert_called_once_with(topic, expected_payload)
        self.assertFalse(result)
    
    def test_publish_exception(self):
        """Test exception during message publication."""
        topic = "test/topic"
        message = {"key": "value"}
        
        # Mock an exception during publish
        self.mock_client.publish.side_effect = Exception("Publish error")
        
        result = self.mqtt_connection.publish(topic, message)
        
        self.assertFalse(result)
    
    def test_disconnect_success(self):
        """Test successful disconnection from the broker."""
        result = self.mqtt_connection.disconnect()
        
        self.mock_client.loop_stop.assert_called_once()
        self.mock_client.disconnect.assert_called_once()
        self.assertTrue(result)
    
    def test_disconnect_exception(self):
        """Test exception during disconnection."""
        self.mock_client.disconnect.side_effect = Exception("Disconnect error")
        
        result = self.mqtt_connection.disconnect()
        
        self.mock_client.loop_stop.assert_called_once()
        self.mock_client.disconnect.assert_called_once()
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()