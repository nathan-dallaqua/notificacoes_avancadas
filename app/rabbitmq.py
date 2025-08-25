import pika
import threading

class RabbitMQConnection:
    _connections = {}
    _lock = threading.Lock()
    
    @classmethod
    def get_connection(cls, name="default"):
        with cls._lock:
            if name not in cls._connections or cls._connections[name].is_closed:
                credentials = pika.PlainCredentials('bjnuffmq', 'gj-YQIiEXyfxQxjsZtiYDKeXIT8ppUq7')
                parameters = pika.ConnectionParameters(
                    host='jaragua-01.lmq.cloudamqp.com',
                    port=5672,
                    virtual_host='bjnuffmq',
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
                cls._connections[name] = pika.BlockingConnection(parameters)
            return cls._connections[name]
    
    @classmethod
    def close_all(cls):
        with cls._lock:
            for name, connection in cls._connections.items():
                if not connection.is_closed:
                    connection.close()
            cls._connections.clear()