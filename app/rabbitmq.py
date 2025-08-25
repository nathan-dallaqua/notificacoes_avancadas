import pika
import threading
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RabbitMQConnection:
    _connections = {}
    _lock = threading.Lock()
    
    @classmethod
    def get_connection(cls, name="default"):
        with cls._lock:
            if name not in cls._connections or cls._connections[name].is_closed:
                try:
                    credentials = pika.PlainCredentials('bjnuffmq', 'gj-YQIiEXyfxQxjsZtiYDKeXIT8ppUq7')
                    parameters = pika.ConnectionParameters(
                        host='jaragua-01.lmq.cloudamqp.com',
                        port=5672,
                        virtual_host='bjnuffmq',
                        credentials=credentials,
                        heartbeat=300,
                        blocked_connection_timeout=150,
                        connection_attempts=3,
                        retry_delay=5,
                        socket_timeout=10
                    )
                    cls._connections[name] = pika.BlockingConnection(parameters)
                    logger.info(f"Conexão RabbitMQ '{name}' estabelecida com sucesso")
                except Exception as e:
                    logger.error(f"Erro ao conectar com RabbitMQ '{name}': {e}")
                    raise
            return cls._connections[name]
    
    @classmethod
    def close_connection(cls, name):
        with cls._lock:
            if name in cls._connections and not cls._connections[name].is_closed:
                try:
                    cls._connections[name].close()
                    logger.info(f"Conexão RabbitMQ '{name}' fechada")
                except Exception as e:
                    logger.error(f"Erro ao fechar conexão '{name}': {e}")
                finally:
                    if name in cls._connections:
                        del cls._connections[name]
    
    @classmethod
    def close_all(cls):
        with cls._lock:
            for name in list(cls._connections.keys()):
                cls.close_connection(name)