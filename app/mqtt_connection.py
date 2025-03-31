import paho.mqtt.client as mqtt
import logging
import time
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MQTTConnection:
    def __init__(self, broker_id: str, host: str, port: int, token: str):
        self.broker_id = broker_id
        self.client = mqtt.Client(client_id=f"producer_{broker_id}")
        self.client.username_pw_set(token)
        self.host = host
        self.port = port
        self.connected = False
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info(f"Conectado ao broker {self.broker_id}")
            self.connected = True
        else:
            logging.error(f"Falha ao conectar ao broker {self.broker_id}: {rc}")

    def on_publish(self, client, userdata, mid):
        logging.info(f"Mensagem publicada no broker {self.broker_id} com ID {mid}")
    
    def connect(self):
        try:
            self.client.connect(self.host, self.port)
            self.client.loop_start()
            while not self.connected:
                time.sleep(0.1)
            return True
        except Exception as e:
            logging.error(f"Erro ao conectar ao broker {self.broker_id}: {e}")
            return False
        
    def publish(self, topic: str, message: dict):
        try:
            payload = json.dumps(message)
            result = self.client.publish(topic, payload)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logging.info(f"Mensagem publicada em {topic}: {payload}")
                return True
            else:
                logging.error(f"Falha ao publicar mensagem em {topic}: {result.rc}")
                return False
        except Exception as e:
            logging.error(f"Erro ao publicar mensagem: {e}")
            return False
        

    def disconnect(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
            logging.info(f"Desconectado do broker {self.broker_id}")
        except Exception as e:
            logging.error(f"Erro ao desconectar do broker {self.broker_id}: {e}")
            return False
        
        return True
    
