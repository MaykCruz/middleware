import logging
from fastapi import APIRouter, Header, Request
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

from app.infrastructure.celery import celery_app
from app.utils.validators import validate_cpf, clean_digits, formatar_telefone_br
from app.utils.formatters import limpar_nome
from app.services.bot.memory.session import SessionManager

router = APIRouter(prefix="/webhooks/chatguru", tags=["Webhook ChatGuru"])
logger = logging.getLogger(__name__)

class BotContext(BaseModel):
    Erro: Optional[bool] = False
    Timer: Optional[bool] = False
    URA: Optional[str] = None
    Contexto: Optional[str] = None
    Produto: Optional[str] = None

class ChatGuruPayload(BaseModel):
    chat_id: str
    celular: str
    nome: str
    texto_mensagem: str
    bot_context: BotContext
    phone_id: Optional[str] = None

    class Config:
        extra = "allow"

@router.post("/")
async def receber_webhook_chatguru(payload: ChatGuruPayload):
    """
    Recebe todos os eventos do ChatGuru e roteia para a ação correta com base no 'bot_context.Contexto'.
    """
    contexto_atual = payload.bot_context.Contexto
    chat_id = payload.chat_id

    logger.info(f"📥 [ChatGuru] Webhook recebido! ChatID: {chat_id} | Contexto: {contexto_atual}")

    if contexto_atual == "aguardando_simulacao_clt":
        cpf_limpo = clean_digits(payload.texto_mensagem)
        telefone_formatado = formatar_telefone_br(payload.celular)
        nome_limpo = limpar_nome(payload.nome)

        if not validate_cpf(cpf_limpo) or not telefone_formatado:
            logger.warning(f"🚫 [ChatGuru] Dados Inválidos: CPF {cpf_limpo} ou Tel {payload.celular}")
            return {"status": "recebido", "msg": "Dados inválidos"}
        
        logger.info(f"🚀 [ChatGuru] Iniciando simulação CLT para {cpf_limpo}")

        session = SessionManager()
        session.set_context(chat_id, {
            "cpf": cpf_limpo,
            "nome": nome_limpo,
            "celular": telefone_formatado,
            "contact_id": payload.phone_id
        })

        celery_app.send_task(
            "app.tasks.api_processor.executar_fluxo_clt_chatguru",
            kwargs={
                "chat_id": chat_id,
                "cpf": cpf_limpo,
                "celular": telefone_formatado,
                "nome": nome_limpo,
                "contact_id": payload.phone_id
            }
        )