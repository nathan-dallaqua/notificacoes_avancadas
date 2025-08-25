from flask import Flask, request, jsonify
from uuid import UUID, uuid4
import json
import threading
import time
from pika import BasicProperties
from .rabbitmq import RabbitMQConnection
from .consumers import notificacoes_status, iniciar_consumidores

app = Flask(__name__)

consumidores_iniciados = False
consumidores_lock = threading.Lock()

def start_consumers():
    print("Iniciando consumidores RabbitMQ...")
    iniciar_consumidores()

@app.before_request
def before_request():
    global consumidores_iniciados
    with consumidores_lock:
        if not consumidores_iniciados:
            consumer_thread = threading.Thread(target=start_consumers, daemon=True)
            consumer_thread.start()
            consumidores_iniciados = True
            print("Consumidores RabbitMQ agendados para iniciar")

@app.route('/api/notificar', methods=['POST'])
def enviar_notificacao():
    try:
        data = request.get_json()
        if not data or 'conteudoMensagem' not in data or 'tipoNotificacao' not in data:
            return jsonify({'error': 'Dados inválidos'}), 400
        
        conteudo_mensagem = data['conteudoMensagem']
        tipo_notificacao = data['tipoNotificacao']
        mensagem_id = UUID(data.get('mensagemId', str(uuid4())))
        
        if tipo_notificacao not in ['EMAIL', 'SMS', 'PUSH']:
            return jsonify({'error': 'Tipo de notificação inválido'}), 400
        
        trace_id = uuid4()
        
        from .consumers import atualizar_status
        atualizar_status(trace_id, "RECEBIDO", {
            "mensagemId": str(mensagem_id),
            "conteudoMensagem": conteudo_mensagem,
            "tipoNotificacao": tipo_notificacao
        })
        
        dados = {
            "traceId": str(trace_id),
            "mensagemId": str(mensagem_id),
            "conteudoMensagem": conteudo_mensagem,
            "tipoNotificacao": tipo_notificacao
        }
        
        connection = RabbitMQConnection.get_connection("publisher")
        channel = connection.channel()
        channel.queue_declare(queue='fila.notificacao.entrada.ROBSON', durable=True)
        channel.basic_publish(
            exchange='',
            routing_key='fila.notificacao.entrada.ROBSON',
            body=json.dumps(dados),
            properties=BasicProperties(delivery_mode=2)
        )
        
        return jsonify({
            'mensagemId': str(mensagem_id),
            'traceId': str(trace_id)
        }), 202
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/notificacao/status/<trace_id>', methods=['GET'])
def consultar_status(trace_id):
    try:
        trace_uuid = UUID(trace_id)
        
        if trace_uuid not in notificacoes_status:
            return jsonify({'error': 'Notificação não encontrada'}), 404
        
        dados = notificacoes_status[trace_uuid]
        return jsonify({
            'traceId': str(dados['traceId']),
            'mensagemId': str(dados['mensagemId']),
            'conteudoMensagem': dados['conteudoMensagem'],
            'tipoNotificacao': dados['tipoNotificacao'],
            'status': dados['status'],
            'historico': dados['historico']
        })
        
    except ValueError:
        return jsonify({'error': 'TraceId inválido'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    consumer_thread = threading.Thread(target=start_consumers, daemon=True)
    consumer_thread.start()
    app.run(host='0.0.0.0', port=8000, debug=True)