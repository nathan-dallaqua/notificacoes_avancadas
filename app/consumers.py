import json
import random
import time
import threading
import logging
from uuid import UUID
from pika import BasicProperties
from .rabbitmq import RabbitMQConnection

logger = logging.getLogger(__name__)

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

def criar_conexao_segura(nome):
    """Cria uma conexão com tratamento de erros"""
    max_tentativas = 5
    tentativa = 0
    
    while tentativa < max_tentativas:
        try:
            connection = RabbitMQConnection.get_connection(nome)
            channel = connection.channel()
            return connection, channel
        except Exception as e:
            tentativa += 1
            logger.warning(f"Tentativa {tentativa}/{max_tentativas} para conexão '{nome}' falhou: {e}")
            time.sleep(2 ** tentativa)
    
    raise Exception(f"Não foi possível estabelecer conexão '{nome}' após {max_tentativas} tentativas")

def processador_entrada():
    """Processador principal com reconexão robusta"""
    while True:
        try:
            connection, channel = criar_conexao_segura("entrada")
            channel.queue_declare(queue='fila.notificacao.entrada.NATHAN', durable=True)
            
            def callback(ch, method, properties, body):
                try:
                    dados = json.loads(body.decode())
                    trace_id = UUID(dados["traceId"])
                    
                    if random.random() < 0.12:
                        atualizar_status(trace_id, "FALHA_PROCESSAMENTO_INICIAL", dados)
                        retry_conn, retry_channel = criar_conexao_segura("retry_pub")
                        retry_channel.queue_declare(queue='fila.notificacao.retry.NATHAN', durable=True)
                        retry_channel.basic_publish(
                            exchange='',
                            routing_key='fila.notificacao.retry.NATHAN',
                            body=json.dumps(dados),
                            properties=BasicProperties(delivery_mode=2)
                        )
                        retry_conn.close()
                        
                    else:
                        time.sleep(random.uniform(1, 1.5))
                        atualizar_status(trace_id, "PROCESSADO_INTERMEDIARIO", dados)
                        
                        validacao_conn, validacao_channel = criar_conexao_segura("validacao_pub")
                        validacao_channel.queue_declare(queue='fila.notificacao.validacao.NATHAN', durable=True)
                        validacao_channel.basic_publish(
                            exchange='',
                            routing_key='fila.notificacao.validacao.NATHAN',
                            body=json.dumps(dados),
                            properties=BasicProperties(delivery_mode=2)
                        )
                        validacao_conn.close()
                        
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    logger.error(f"Erro no processamento da mensagem: {e}")
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    except:
                        pass
            
            channel.basic_consume(
                queue='fila.notificacao.entrada.NATHAN',
                on_message_callback=callback,
                auto_ack=False
            )
            
            logger.info("Processador de entrada iniciado e consumindo")
            channel.start_consuming()
            
        except Exception as e:
            logger.error(f"Erro fatal no processador de entrada: {e}")
            time.sleep(5)
            try:
                RabbitMQConnection.close_connection("entrada")
            except:
                pass

def processador_retry():
    """Processador de retry com reconexão robusta"""
    while True:
        try:
            connection, channel = criar_conexao_segura("retry")
            
            channel.queue_declare(queue='fila.notificacao.retry.NATHAN', durable=True)
            
            def callback(ch, method, properties, body):
                try:
                    time.sleep(3)
                    
                    dados = json.loads(body.decode())
                    trace_id = UUID(dados["traceId"])
                    
                    if random.random() < 0.2:
                        atualizar_status(trace_id, "FALHA_FINAL_REPROCESSAMENTO", dados)
                        dlq_conn, dlq_channel = criar_conexao_segura("dlq_pub")
                        dlq_channel.queue_declare(queue='fila.notificacao.dlq.NATHAN', durable=True)
                        dlq_channel.basic_publish(
                            exchange='',
                            routing_key='fila.notificacao.dlq.NATHAN',
                            body=json.dumps(dados),
                            properties=BasicProperties(delivery_mode=2)
                        )
                        dlq_conn.close()
                        
                    else:
                        atualizar_status(trace_id, "REPROCESSADO_COM_SUCESSO", dados)
                        
                        validacao_conn, validacao_channel = criar_conexao_segura("validacao_pub2")
                        validacao_channel.queue_declare(queue='fila.notificacao.validacao.NATHAN', durable=True)
                        validacao_channel.basic_publish(
                            exchange='',
                            routing_key='fila.notificacao.validacao.NATHAN',
                            body=json.dumps(dados),
                            properties=BasicProperties(delivery_mode=2)
                        )
                        validacao_conn.close()
                        
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    logger.error(f"Erro no processamento de retry: {e}")
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    except:
                        pass
            
            channel.basic_consume(
                queue='fila.notificacao.retry.NATHAN',
                on_message_callback=callback,
                auto_ack=False
            )
            
            logger.info("Processador de retry iniciado e consumindo")
            channel.start_consuming()
            
        except Exception as e:
            logger.error(f"Erro fatal no processador de retry: {e}")
            time.sleep(5)
            try:
                RabbitMQConnection.close_connection("retry")
            except:
                pass

def processador_validacao():
    """Processador de validação com reconexão robusta"""
    while True:
        try:
            connection, channel = criar_conexao_segura("validacao")
            
            channel.queue_declare(queue='fila.notificacao.validacao.NATHAN', durable=True)
            
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
                        dlq_conn, dlq_channel = criar_conexao_segura("dlq_pub2")
                        dlq_channel.queue_declare(queue='fila.notificacao.dlq.NATHAN', durable=True)
                        dlq_channel.basic_publish(
                            exchange='',
                            routing_key='fila.notificacao.dlq.NATHAN',
                            body=json.dumps(dados),
                            properties=BasicProperties(delivery_mode=2)
                        )
                        dlq_conn.close()
                        
                    else:
                        atualizar_status(trace_id, "ENVIADO_SUCESSO", dados)
                        
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    logger.error(f"Erro no processamento de validação: {e}")
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    except:
                        pass
            
            channel.basic_consume(
                queue='fila.notificacao.validacao.NATHAN',
                on_message_callback=callback,
                auto_ack=False
            )
            
            logger.info("Processador de validação iniciado e consumindo")
            channel.start_consuming()
            
        except Exception as e:
            logger.error(f"Erro fatal no processador de validação: {e}")
            time.sleep(5)
            try:
                RabbitMQConnection.close_connection("validacao")
            except:
                pass

def processador_dlq():
    """Processador DLQ com reconexão robusta"""
    while True:
        try:
            connection, channel = criar_conexao_segura("dlq")
            
            channel.queue_declare(queue='fila.notificacao.dlq.NATHAN', durable=True)
            
            def callback(ch, method, properties, body):
                try:
                    dados = json.loads(body.decode())
                    trace_id = UUID(dados["traceId"])
                    logger.info(f"Mensagem com traceId {trace_id} enviada para DLQ e não será mais processada")
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    logger.error(f"Erro no processamento DLQ: {e}")
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    except:
                        pass
            
            channel.basic_consume(
                queue='fila.notificacao.dlq.NATHAN',
                on_message_callback=callback,
                auto_ack=False
            )
            
            logger.info("Processador DLQ iniciado e consumindo")
            channel.start_consuming()
            
        except Exception as e:
            logger.error(f"Erro fatal no processador DLQ: {e}")
            time.sleep(5)
            try:
                RabbitMQConnection.close_connection("dlq")
            except:
                pass

def iniciar_consumidores():
    """Iniciar todos os consumidores em threads separadas"""
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
        time.sleep(2)
    
    logger.info("Todos os consumidores iniciados")
    
    while True:
        time.sleep(1)