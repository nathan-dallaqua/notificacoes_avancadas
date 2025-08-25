import pytest
from unittest.mock import patch, MagicMock
from uuid import UUID, uuid4
from app.app import enviar_notificacao

@pytest.fixture
def mock_rabbitmq():
    with patch('app.app.RabbitMQConnection.get_connection') as mock_get_connection:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        
        mock_connection.channel.return_value = mock_channel
        mock_get_connection.return_value = mock_connection
        
        yield mock_channel

@pytest.fixture
def mock_atualizar_status():
    with patch('app.app.atualizar_status') as mock:
        yield mock

def test_enviar_notificacao_publishes_to_correct_queue(mock_rabbitmq, mock_atualizar_status):
    # Arrange
    from flask import Flask
    app = Flask(__name__)
    
    with app.test_request_context(
        '/api/notificar',
        method='POST',
        json={
            'conteudoMensagem': 'Teste de mensagem',
            'tipoNotificacao': 'EMAIL'
        }
    ):

        response = enviar_notificacao()
        assert mock_rabbitmq.basic_publish.called
        
        call_args = mock_rabbitmq.basic_publish.call_args
        routing_key = call_args[1]['routing_key']
        body = call_args[1]['body']
        
        assert routing_key == 'fila.notificacao.entrada.ROBSON'
        
        body_data = json.loads(body)
        assert 'traceId' in body_data
        assert 'mensagemId' in body_data
        assert body_data['conteudoMensagem'] == 'Teste de mensagem'
        assert body_data['tipoNotificacao'] == 'EMAIL'
        assert mock_atualizar_status.called

def test_enviar_notificacao_generates_traceId(mock_rabbitmq, mock_atualizar_status):
    # Arrange
    from flask import Flask
    app = Flask(__name__)
    
    with app.test_request_context(
        '/api/notificar',
        method='POST',
        json={
            'conteudoMensagem': 'Teste',
            'tipoNotificacao': 'SMS'
        }
    ):

        response = enviar_notificacao()
        response_data = response.get_json()

        assert 'traceId' in response_data
        assert 'mensagemId' in response_data
    
        call_args = mock_atualizar_status.call_args
        traceId_arg = call_args[0][0]
        assert str(traceId_arg) == response_data['traceId']