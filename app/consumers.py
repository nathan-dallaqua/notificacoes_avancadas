import json
import random
import time
import threading
from uuid import UUID
from pika import BasicProperties
from .rabbitmq import RabbitMQConnection

# Estrutura em memória para armazenar status
notificacoes_status = {}
notificacoes_lock = threading.Lock()

def atualizar_status(traceId, status, dados):
    with notificacoes_lock:
        if traceId not in notificacoes_status:
            notificacoes_status[traceId] = {
                "traceId": traceId,
                "mensagemId": UUID(dados["mensagemId"]),
                "conteudoMensagem": dados["conteudoMensagem"],
                "tipoNotificacao": dados["tipoNotificacao"],
                "status": status,
                "historico": [status]
            }
        else:
            notificacoes_status[traceId]["status"] = status
            notificacoes_status[traceId]["historico"].append(status)

def processador_entrada():
    """Processador principal com sua própria conexão"""
    try:
        connection = RabbitMQConnection.get_connection("entrada")
        channel = connection.channel()
        
        # Declarar a fila
        channel.queue_declare(queue='fila.notificacao.entrada.ROBSON', durable=True)
        
        def callback(ch, method, properties, body):
            try:
                dados = json.loads(body.decode())
                trace_id = UUID(dados["traceId"])
                
                # Simular falha aleatória (10-15% de chance)
                if random.random() < 0.12:
                    atualizar_status(trace_id, "FALHA_PROCESSAMENTO_INICIAL", dados)
                    
                    # Publicar na fila de retry
                    retry_connection = RabbitMQConnection.get_connection("retry_pub")
                    retry_channel = retry_connection.channel()
                    retry_channel.queue_declare(queue='fila.notificacao.retry.ROBSON', durable=True)
                    
                    retry_channel.basic_publish(
                        exchange='',
                        routing_key='fila.notificacao.retry.ROBSON',
                        body=json.dumps(dados),
                        properties=BasicProperties(delivery_mode=2)
                    )
                    
                else:
                    # Simular processamento
                    time.sleep(random.uniform(1, 1.5))
                    atualizar_status(trace_id, "PROCESSADO_INTERMEDIARIO", dados)
                    
                    # Publicar na fila de validação
                    validacao_connection = RabbitMQConnection.get_connection("validacao_pub")
                    validacao_channel = validacao_connection.channel()
                    validacao_channel.queue_declare(queue='fila.notificacao.validacao.ROBSON', durable=True)
                    
                    validacao_channel.basic_publish(
                        exchange='',
                        routing_key='fila.notificacao.validacao.ROBSON',
                        body=json.dumps(dados),
                        properties=BasicProperties(delivery_mode=2)
                    )
                    
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            except Exception as e:
                print(f"Erro no processador de entrada: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        channel.basic_consume(
            queue='fila.notificacao.entrada.ROBSON',
            on_message_callback=callback,
            auto_ack=False
        )
        
        print("Processador de entrada iniciado")
        channel.start_consuming()
        
    except Exception as e:
        print(f"Erro fatal no processador de entrada: {e}")
        time.sleep(5)  # Esperar antes de tentar reconectar
        processador_entrada()  # Reiniciar

def processador_retry():
    """Processador de retry com sua própria conexão"""
    try:
        connection = RabbitMQConnection.get_connection("retry")
        channel = connection.channel()
        
        channel.queue_declare(queue='fila.notificacao.retry.ROBSON', durable=True)
        
        def callback(ch, method, properties, body):
            try:
                time.sleep(3)  # Atraso antes do reprocessamento
                
                dados = json.loads(body.decode())
                trace_id = UUID(dados["traceId"])
                
                # Nova chance de falha (20%)
                if random.random() < 0.2:
                    atualizar_status(trace_id, "FALHA_FINAL_REPROCESSAMENTO", dados)
                    
                    # Publicar na DLQ
                    dlq_connection = RabbitMQConnection.get_connection("dlq_pub")
                    dlq_channel = dlq_connection.channel()
                    dlq_channel.queue_declare(queue='fila.notificacao.dlq.ROBSON', durable=True)
                    
                    dlq_channel.basic_publish(
                        exchange='',
                        routing_key='fila.notificacao.dlq.ROBSON',
                        body=json.dumps(dados),
                        properties=BasicProperties(delivery_mode=2)
                    )
                    
                else:
                    atualizar_status(trace_id, "REPROCESSADO_COM_SUCESSO", dados)
                    
                    # Publicar na fila de validação
                    validacao_connection = RabbitMQConnection.get_connection("validacao_pub2")
                    validacao_channel = validacao_connection.channel()
                    validacao_channel.queue_declare(queue='fila.notificacao.validacao.ROBSON', durable=True)
                    
                    validacao_channel.basic_publish(
                        exchange='',
                        routing_key='fila.notificacao.validacao.ROBSON',
                        body=json.dumps(dados),
                        properties=BasicProperties(delivery_mode=2)
                    )
                    
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            except Exception as e:
                print(f"Erro no processador de retry: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        channel.basic_consume(
            queue='fila.notificacao.retry.ROBSON',
            on_message_callback=callback,
            auto_ack=False
        )
        
        print("Processador de retry iniciado")
        channel.start_consuming()
        
    except Exception as e:
        print(f"Erro fatal no processador de retry: {e}")
        time.sleep(5)
        processador_retry()

def processador_validacao():
    """Processador de validação com sua própria conexão"""
    try:
        connection = RabbitMQConnection.get_connection("validacao")
        channel = connection.channel()
        
        channel.queue_declare(queue='fila.notificacao.validacao.ROBSON', durable=True)
        
        def callback(ch, method, properties, body):
            try:
                dados = json.loads(body.decode())
                trace_id = UUID(dados["traceId"])
                tipo = dados["tipoNotificacao"]
                
                if tipo == "EMAIL":
                    time.sleep(random.uniform(0.5, 1.0))
                elif tipo == "SMS":
                    time.sleep(random.uniform(0.3, 0.7))
                else:
                    time.sleep(random.uniform(0.2, 0.5))
                
                if random.random() < 0.05:
                    atualizar_status(trace_id, "FALHA_ENVIO_FINAL", dados)
                    
                    dlq_connection = RabbitMQConnection.get_connection("dlq_pub2")
                    dlq_channel = dlq_connection.channel()
                    dlq_channel.queue_declare(queue='fila.notificacao.dlq.ROBSON', durable=True)
                    
                    dlq_channel.basic_publish(
                        exchange='',
                        routing_key='fila.notificacao.dlq.ROBSON',
                        body=json.dumps(dados),
                        properties=BasicProperties(delivery_mode=2)
                    )
                    
                else:
                    atualizar_status(trace_id, "ENVIADO_SUCESSO", dados)
                    
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            except Exception as e:
                print(f"Erro no processador de validação: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        channel.basic_consume(
            queue='fila.notificacao.validacao.ROBSON',
            on_message_callback=callback,
            auto_ack=False
        )
        
        print("Processador de validação iniciado")
        channel.start_consuming()
        
    except Exception as e:
        print(f"Erro fatal no processador de validação: {e}")
        time.sleep(5)
        processador_validacao()

def processador_dlq():
    try:
        connection = RabbitMQConnection.get_connection("dlq")
        channel = connection.channel()
        channel.queue_declare(queue='fila.notificacao.dlq.ROBSON', durable=True)
        
        def callback(ch, method, properties, body):
            try:
                dados = json.loads(body.decode())
                trace_id = UUID(dados["traceId"])
                print(f"Mensagem com traceId {trace_id} enviada para DLQ e não será mais processada")
                
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            except Exception as e:
                print(f"Erro no processador DLQ: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        channel.basic_consume(
            queue='fila.notificacao.dlq.ROBSON',
            on_message_callback=callback,
            auto_ack=False
        )
        
        print("Processador DLQ iniciado")
        channel.start_consuming()
        
    except Exception as e:
        print(f"Erro fatal no processador DLQ: {e}")
        time.sleep(5)
        processador_dlq()

def iniciar_consumidores():
    threads = []
    consumers = [
        ("Entrada", processador_entrada),
        ("Retry", processador_retry),
        ("Validação", processador_validacao),
        ("DLQ", processador_dlq)
    ]
    
    for name, consumer_func in consumers:
        thread = threading.Thread(
            target=consumer_func, 
            name=f"Consumer-{name}",
            daemon=True
        )
        thread.start()
        threads.append(thread)
        time.sleep(1)
    
    print("Todos os consumidores iniciados")
    
    while True:
        time.sleep(1)