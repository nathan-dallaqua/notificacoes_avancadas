from pydantic import BaseModel, Field
from uuid import UUID, uuid4
from typing import Optional, List
from enum import Enum

class TipoNotificacao(str, Enum):
    EMAIL = "EMAIL"
    SMS = "SMS"
    PUSH = "PUSH"

class StatusNotificacao(str, Enum):
    RECEBIDO = "RECEBIDO"
    FALHA_PROCESSAMENTO_INICIAL = "FALHA_PROCESSAMENTO_INICIAL"
    PROCESSADO_INTERMEDIARIO = "PROCESSADO_INTERMEDIARIO"
    FALHA_FINAL_REPROCESSAMENTO = "FALHA_FINAL_REPROCESSAMENTO"
    REPROCESSADO_COM_SUCESSO = "REPROCESSADO_COM_SUCESSO"
    FALHA_ENVIO_FINAL = "FALHA_ENVIO_FINAL"
    ENVIADO_SUCESSO = "ENVIADO_SUCESSO"

class NotificacaoRequest(BaseModel):
    mensagemId: Optional[UUID] = Field(default_factory=uuid4)
    conteudoMensagem: str
    tipoNotificacao: TipoNotificacao

    class Config:
        use_enum_values = True

class NotificacaoResponse(BaseModel):
    mensagemId: UUID
    traceId: UUID

class NotificacaoStatus(BaseModel):
    traceId: UUID
    mensagemId: UUID
    conteudoMensagem: str
    tipoNotificacao: str
    status: str
    historico: List[str] = []