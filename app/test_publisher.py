import pytest
from unittest.mock import patch, MagicMock
from uuid import UUID, uuid4
import json
from app.app import app
from app.consumers import notificacoes_status, atualizar_status

@pytest.fixture
def client():
    """Fixture para criar um cliente de teste Flask"""
    with app.test_client() as client:
        yield client

@pytest.fixture
def mock_rabbitmq_connection():
    """Mock da conexão RabbitMQ"""
    with patch('app.app.RabbitMQConnection.get_connection') as mock_get_connection:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        
        mock_connection.channel.return_value = mock_channel
        mock_get_connection.return_value = mock_connection
        
        yield mock_channel

@pytest.fixture
def mock_consumers_import():
    """Mock da importação dentro da função enviar_notificacao"""
    with patch('app.consumers.atualizar_status') as mock:
        yield mock

@pytest.fixture(autouse=True)
def clean_notificacoes_status():
    """Limpa o estado das notificações antes de cada teste"""
    notificacoes_status.clear()
    yield
    notificacoes_status.clear()

def test_enviar_notificacao_sucesso(client, mock_rabbitmq_connection, mock_consumers_import):
    """Teste de envio de notificação bem-sucedido"""
    test_data = {
        'conteudoMensagem': 'Teste de mensagem',
        'tipoNotificacao': 'EMAIL'
    }
    
    response = client.post(
        '/api/notificar',
        json=test_data,
        content_type='application/json'
    )
    
    assert response.status_code == 202
    response_data = response.get_json()

    assert 'mensagemId' in response_data
    assert 'traceId' in response_data
    assert isinstance(UUID(response_data['mensagemId']), UUID)
    assert isinstance(UUID(response_data['traceId']), UUID)
    
    mock_rabbitmq_connection.queue_declare.assert_called_once_with(
        queue='fila.notificacao.entrada.NATHAN', 
        durable=True
    )
    
    mock_rabbitmq_connection.basic_publish.assert_called_once()
    
    call_args = mock_rabbitmq_connection.basic_publish.call_args
    message_body = json.loads(call_args[1].get('body'))

    assert call_args[1]['routing_key'] == 'fila.notificacao.entrada.NATHAN'
    assert message_body['conteudoMensagem'] == 'Teste de mensagem'
    assert message_body['tipoNotificacao'] == 'EMAIL'
    assert 'traceId' in message_body
    assert 'mensagemId' in message_body
    assert mock_consumers_import.called

def test_enviar_notificacao_com_mensagem_id_customizado(client, mock_rabbitmq_connection, mock_consumers_import):
    """Teste com mensagemId customizado"""
    custom_id = '123e4567-e89b-12d3-a456-426614174000'
    
    test_data = {
        'mensagemId': custom_id,
        'conteudoMensagem': 'Teste com ID customizado',
        'tipoNotificacao': 'SMS'
    }
    
    response = client.post(
        '/api/notificar',
        json=test_data,
        content_type='application/json'
    )

    response_data = response.get_json()
    
    assert response.status_code == 202
    assert response_data['mensagemId'] == custom_id
    assert mock_consumers_import.called

def test_enviar_notificacao_tipo_push(client, mock_rabbitmq_connection, mock_consumers_import):
    """Teste com tipo PUSH"""

    test_data = {
        'conteudoMensagem': 'Notificação push',
        'tipoNotificacao': 'PUSH'
    }
    
    response = client.post(
        '/api/notificar',
        json=test_data,
        content_type='application/json'
    )
    
    assert response.status_code == 202
    assert mock_consumers_import.called

def test_enviar_notificacao_dados_invalidos(client):
    """Teste com dados inválidos"""
    test_data = {
        'conteudoMensagem': 'Teste'
    }
    
    response = client.post(
        '/api/notificar',
        json=test_data,
        content_type='application/json'
    )
    
    assert response.status_code == 400
    assert 'error' in response.get_json()

def test_enviar_notificacao_tipo_invalido(client):
    """Teste com tipo de notificação inválido"""
    test_data = {
        'conteudoMensagem': 'Teste',
        'tipoNotificacao': 'INVALIDO'
    }
    
    response = client.post(
        '/api/notificar',
        json=test_data,
        content_type='application/json'
    )
    
    assert response.status_code == 400
    assert 'error' in response.get_json()

def test_consultar_status_existente(client):
    """Teste de consulta de status existente"""
    trace_id = uuid4()
    mensagem_id = uuid4()
    
    # Adicionar manualmente ao status usando a função real
    atualizar_status(trace_id, "RECEBIDO", {
        "mensagemId": str(mensagem_id),
        "conteudoMensagem": "Mensagem de teste",
        "tipoNotificacao": "EMAIL"
    })
    
    response = client.get(f'/api/notificacao/status/{trace_id}')
    response_data = response.get_json()

    assert response.status_code == 200
    assert response_data['traceId'] == str(trace_id)
    assert response_data['mensagemId'] == str(mensagem_id)
    assert response_data['conteudoMensagem'] == 'Mensagem de teste'
    assert response_data['tipoNotificacao'] == 'EMAIL'
    assert response_data['status'] == 'RECEBIDO'

def test_consultar_status_nao_existente(client):
    """Teste de consulta de status não existente"""

    trace_id_inexistente = '00000000-0000-0000-0000-000000000000'
    
    response = client.get(f'/api/notificacao/status/{trace_id_inexistente}')
    assert response.status_code == 404
    assert 'error' in response.get_json()

def test_consultar_status_traceid_invalido(client):
    """Teste com traceId inválido"""

    response = client.get('/api/notificacao/status/invalid-trace-id')
    assert response.status_code == 400
    assert 'error' in response.get_json()

def test_health_check(client):
    """Teste do endpoint health check"""

    response = client.get('/health')
    assert response.status_code == 200
    assert response.get_json() == {'status': 'healthy'}

def test_rabbitmq_publication_parameters(mock_rabbitmq_connection, mock_consumers_import):
    """Teste específico dos parâmetros de publicação no RabbitMQ"""
    from app.app import enviar_notificacao
    from flask import Flask
    
    with Flask(__name__).test_request_context(
        '/api/notificar',
        method='POST',
        json={
            'conteudoMensagem': 'Teste de parâmetros',
            'tipoNotificacao': 'EMAIL'
        }
    ):
        response = enviar_notificacao()
        if isinstance(response, tuple):
            response_obj = response[0]
        else:
            response_obj = response
        
        response_data = response_obj.get_json()
        
        mock_rabbitmq_connection.basic_publish.assert_called_once()
        call_args = mock_rabbitmq_connection.basic_publish.call_args
        properties = call_args[1].get('properties')
        body_data = json.loads(call_args[1].get('body'))
        
        assert call_args[1]['exchange'] == ''
        assert call_args[1]['routing_key'] == 'fila.notificacao.entrada.NATHAN'
        assert properties.delivery_mode == 2
        assert body_data['conteudoMensagem'] == 'Teste de parâmetros'
        assert body_data['tipoNotificacao'] == 'EMAIL'
        assert body_data['traceId'] == response_data.get('traceId')
        assert mock_consumers_import.called