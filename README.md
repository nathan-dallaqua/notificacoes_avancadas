# Sistema de Notificações Assíncronas Avançado

Sistema de notificações assíncronas construído com Python, Flask e RabbitMQ, implementando um pipeline completo de processamento de mensagens com mecanismos de resiliência e retry.

## 📋 Funcionalidades
- ✅ **API RESTful** para envio de notificações
- ✅ **Processamento assíncrono** com RabbitMQ
- ✅ **Múltiplos consumidores** para diferentes estágios do pipeline
- ✅ **Sistema de retry** automático para falhas
- ✅ **Dead Letter Queue (DLQ)** para mensagens não processáveis
- ✅ **Rastreamento completo** com traceId único
- ✅ **Consulta de status** em tempo real
- ✅ **Testes unitários** com pytest

## 🏗️ Arquitetura


### Pipeline de Processamento
1. **Entrada**: `fila.notificacao.entrada.NATHAN`
2. **Retry**: `fila.notificacao.retry.NATHAN` (12% de falha simulada)
3. **Validação**: `fila.notificacao.validacao.NATHAN` 
4. **DLQ**: `fila.notificacao.dlq.NATHAN` (5% de falha final)


### Tipos de Notificação Suportados
- **EMAIL**: Notificações por email
- **SMS**: Notificações por SMS  
- **PUSH**: Notificações push


### Pré-requisitos

- Python 3.8+
- RabbitMQ (servidor remoto configurado)
- pip

## 🚀 Como Executar

### Instalação

```bash
# Clone o repositório
git clone <url-do-repositorio>
cd notificacoes_avancadas

# Crie um ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate # No Windows

# Instale as dependências
pip install -r requirements.txt

# Executar
python -m app.app

# Executar Testes
pytest app/test_publisher.py

