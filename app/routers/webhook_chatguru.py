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

    if not contexto_atual:
        logger.info(f"📥 [ChatGuru] Webhook recebido (Chat {chat_id}), mas ignorado (Sem contexto).")
        return {"status": "ignorado", "msg": "Sem contexto definido."}

    logger.info(f"📥 [ChatGuru] Webhook recebido! ChatID: {chat_id} | Contexto: {contexto_atual}")
    session = SessionManager()

    if contexto_atual == "aguardando_simulacao_clt":
        cpf_limpo = clean_digits(payload.texto_mensagem)
        telefone_formatado = formatar_telefone_br(payload.celular)
        nome_limpo = limpar_nome(payload.nome)

        if not validate_cpf(cpf_limpo) or not telefone_formatado:
            logger.warning(f"🚫 [ChatGuru] Dados Inválidos: CPF {cpf_limpo} ou Tel {payload.celular}")
            return {"status": "recebido", "msg": "Dados inválidos"}
        
        logger.info(f"🚀 [ChatGuru] Disparando Task: Simulação CLT para {cpf_limpo}")

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
        return {"status": "ok", "fluxo": "simulacao_clt"}

    elif contexto_atual == "aguardando_simulacao_fgts":
        cpf_limpo = clean_digits(payload.texto_mensagem)
        telefone_formatado = formatar_telefone_br(payload.celular)
        nome_limpo = limpar_nome(payload.nome)

        if not validate_cpf(cpf_limpo) or not telefone_formatado:
            logger.warning(f"🚫 [ChatGuru] Dados Inválidos no FGTS: CPF {cpf_limpo} ou Tel {payload.celular}")
            return {"status": "erro", "msg": "Dados inválidos"}
        
        logger.info(f"🚀 [ChatGuru] Disparando Task: Simulação FGTS para {cpf_limpo}")

        session.set_context(chat_id, {
            "cpf": cpf_limpo,
            "nome": nome_limpo,
            "celular": telefone_formatado,
            "contact_id": payload.phone_id
        })

        celery_app.send_task(
            "app.tasks.api_processor.executar_fluxo_fgts_chatguru",
            kwargs={
                "chat_id": chat_id,
                "cpf": cpf_limpo,
                "celular": telefone_formatado,
                "nome": nome_limpo,
                "contact_id": payload.phone_id
            }
        )
        return {"status": "ok", "fluxo": "simulacao_fgts"}
    
    elif contexto_atual == "aguardando_digitacao_fgts":
        logger.info(f"🚀 [ChatGuru] Disparando Task: Digitação FGTS para o Chat {chat_id}")
        
        celery_app.send_task(
            "app.tasks.api_processor.executar_digitacao_fgts_chatguru",
            kwargs={"chat_id": chat_id}
        )
        return {"status": "ok", "fluxo": "digitacao_fgts"}
    
    elif contexto_atual == "aguardando_digitacao_clt":
        logger.info(f"🚀 [ChatGuru] Disparando Task: Digitação CLT para o Chat {chat_id}")

        celery_app.send_task(
            "app.tasks.api_processor.executar_digitacao_clt_chatguru",
            kwargs={"chat_id": chat_id}
        )
        return {"status": "ok", "fluxo": "digitacao_clt"}
    
    elif contexto_atual == "verificar_autorizacao_clt":
        logger.info(f"🔄 [ChatGuru] Verificação manual de autorização solicitada (Chat {chat_id})")

        contexto_salvo = session.get_context(chat_id)
        if not contexto_salvo or not contexto_salvo.get("cpf"):
            logger.error(f"❌ [ChatGuru] Sessão perdida para o Chat {chat_id}")
            return {"status": "erro", "msg": "Sessão expirada"}
        
        cpf = contexto_salvo.get("cpf")
        nome = contexto_salvo.get("nome", "")
        celular = contexto_salvo.get("celular", "")
        contact_id = contexto_salvo.get("contact_id")

        celery_app.send_task(
            "app.tasks.api_processor.executar_fluxo_clt_chatguru",
            kwargs={
                "chat_id": chat_id,
                "cpf": cpf,
                "celular": celular,
                "nome": nome,
                "contact_id": contact_id,
                "verificacao_manual": True
            }
        )
        return {"status": "ok", "fluxo": "verificar_autorizacao_clt"}

    elif contexto_atual == "atualizar_telefone_clt":
        logger.info(f"📱 [ChatGuru] Atualização de telefone solicitada (Chat {chat_id})")

        contexto_salvo = session.get_context(chat_id)
        if not contexto_salvo or not contexto_salvo.get("cpf"):
            return {"status": "erro", "msg": "Sessão expirada"}
        
        cpf = contexto_salvo.get("cpf")
        nome = contexto_salvo.get("nome", "")
        contact_id = contexto_salvo.get("contact_id")

        novo_telefone = formatar_telefone_br(payload.texto_mensagem)

        if not novo_telefone:
            return {"status": "erro", "msg": "Telefone inválido"}
        
        contexto_salvo["celular"] = novo_telefone
        session.set_context(chat_id, contexto_salvo)

        celery_app.send_task(
            "app.tasks.api_processor.executar_fluxo_clt_chatguru",
            kwargs={
                "chat_id": chat_id,
                "cpf": cpf,
                "celular": novo_telefone,
                "nome": nome,
                "contact_id": contact_id
            }
        )
        return {"status": "ok", "fluxo": "atualizar_telefone_clt"}

    else:
        logger.warning(f"⚠️ [ChatGuru] Contexto desconhecido recebido: '{contexto_atual}'. Nenhuma task disparada.")
        return {"status": "ignorado", "msg": f"Contexto '{contexto_atual}' não mapeado no roteador."}