import logging
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from app.integrations.chatguru.service import ChatGuruService
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
    campos_personalizados: Optional[dict] = None

    class Config:
        extra = "allow"

@router.post("/")
async def receber_webhook_chatguru(payload: ChatGuruPayload):
    """
    Recebe todos os eventos do ChatGuru e roteia para a ação correta com base no 'bot_context.Contexto'.
    """
    contexto_atual = payload.bot_context.Contexto

    chat_id = payload.celular

    if not contexto_atual:
        logger.info(f"📥 [ChatGuru] Webhook recebido (Chat {chat_id}), mas ignorado (Sem contexto).")
        return {"status": "ignorado", "msg": "Sem contexto definido."}

    logger.info(f"📥 [ChatGuru] Webhook recebido! ChatID: {chat_id} | Contexto: {contexto_atual}")
    session = SessionManager()
    chatguru = ChatGuruService(chat_id=chat_id, phone_id=payload.phone_id)

    if contexto_atual == "aguardando_simulacao_clt":
        cpf_limpo = clean_digits(payload.texto_mensagem)

        if not validate_cpf(cpf_limpo) and payload.campos_personalizados:
            cpf_custom = payload.campos_personalizados.get("CPF", "")
            if cpf_custom:
                cpf_limpo = clean_digits(cpf_custom)

        telefone_formatado = formatar_telefone_br(payload.celular)
        nome_limpo = limpar_nome(payload.nome)

        if not validate_cpf(cpf_limpo) or not telefone_formatado:
            logger.warning(f"🚫 [ChatGuru] Dados Inválidos: CPF {cpf_limpo} ou Tel {payload.celular}")
            msg_interna = f"🚫 [Erro de Validação] Cliente tentou iniciar simulação CLT com dados inválidos.\nCPF: {payload.texto_mensagem}\nTel: {payload.celular}"
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": msg_interna}, force_internal=True)
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": "⚠️ Alguns dados informados parecem incorretos. Vou transferir você para um de nossos especialistas ajudar, ok?"})
            chatguru.start_put_in_queue(chat_id)
            return {"status": "recebido", "msg": "Dados inválidos"}
        
        logger.info(f"🚀 [ChatGuru] Disparando Task: Simulação CLT para {cpf_limpo}")

        session.set_context(chat_id, {
            "cpf": cpf_limpo,
            "nome": nome_limpo,
            "celular": telefone_formatado,
            "phone_id": payload.phone_id,
            "contact_id": payload.phone_id
        })

        celery_app.send_task(
            "app.tasks.api_processor.executar_fluxo_clt_chatguru",
            kwargs={
                "chat_id": chat_id,
                "cpf": cpf_limpo,
                "celular": telefone_formatado,
                "nome": nome_limpo,
                "phone_id": payload.phone_id,
                "contact_id": payload.phone_id
            }
        )
        return {"status": "ok", "fluxo": "simulacao_clt"}

    elif contexto_atual == "aguardando_simulacao_fgts":
        cpf_limpo = clean_digits(payload.texto_mensagem)

        if not validate_cpf(cpf_limpo) and payload.campos_personalizados:
            cpf_custom = payload.campos_personalizados.get("CPF", "")
            if cpf_custom:
                cpf_limpo = clean_digits(cpf_custom)

        telefone_formatado = formatar_telefone_br(payload.celular)
        nome_limpo = limpar_nome(payload.nome)

        if not validate_cpf(cpf_limpo) or not telefone_formatado:
            logger.warning(f"🚫 [ChatGuru] Dados Inválidos no FGTS: CPF {cpf_limpo} ou Tel {payload.celular}")
            msg_interna = f"🚫 [Erro de Validação] Cliente tentou iniciar simulação FGTS com dados inválidos.\nCPF: {payload.texto_mensagem}\nTel: {payload.celular}"
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": msg_interna}, force_internal=True)
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": "⚠️ Ocorreu um erro na leitura dos seus dados. Vou chamar um consultor para continuarmos o atendimento!"})
            chatguru.start_put_in_queue(chat_id)
            return {"status": "erro", "msg": "Dados inválidos"}
        
        logger.info(f"🚀 [ChatGuru] Disparando Task: Simulação FGTS para {cpf_limpo}")

        session.set_context(chat_id, {
            "cpf": cpf_limpo,
            "nome": nome_limpo,
            "celular": telefone_formatado,
            "phone_id": payload.phone_id,
            "contact_id": payload.phone_id
        })

        celery_app.send_task(
            "app.tasks.api_processor.executar_fluxo_fgts_chatguru",
            kwargs={
                "chat_id": chat_id,
                "cpf": cpf_limpo,
                "celular": telefone_formatado,
                "nome": nome_limpo,
                "phone_id": payload.phone_id,
                "contact_id": payload.phone_id
            }
        )
        return {"status": "ok", "fluxo": "simulacao_fgts"}
    
    elif contexto_atual == "verificar_autorizacao_fgts":
        logger.info(f"🔄 [ChatGuru] Verificação manual de autorização FGTS solicitada (Chat {chat_id})")

        contexto_salvo = session.get_context(chat_id)
        if not contexto_salvo or not contexto_salvo.get("cpf"):
            logger.error(f"❌ [ChatGuru] Sessão perdida para o Chat {chat_id}")
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": "❌ [Sessão Expirada] Cliente clicou para verificar autorização FGTS, mas o contexto no Redis expirou ou foi perdido."}, force_internal=True)
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": "⚠️ Puxa, parece que demoramos um pouquinho e nossa sessão expirou. Um consultor humano vai dar continuidade no seu atendimento de onde paramos!"})
            chatguru.start_put_in_queue(chat_id)
            return {"status": "erro", "msg": "Sessão expirada"}
        
        cpf = contexto_salvo.get("cpf")
        nome = contexto_salvo.get("nome", "")
        celular = contexto_salvo.get("celular", "")
        phone_id = contexto_salvo.get("phone_id")
        contact_id = contexto_salvo.get("contact_id")

        celery_app.send_task(
            "app.tasks.api_processor.executar_fluxo_fgts_chatguru",
            kwargs={
                "chat_id": chat_id,
                "cpf": cpf,
                "celular": celular,
                "nome": nome,
                "phone_id": phone_id,
                "contact_id": contact_id,
                "verificacao_manual": True
            }
        )
        return {"status": "ok", "fluxo": "verificar_autorizacao_fgts"}
    
    elif contexto_atual == "aguardando_digitacao_fgts":
        logger.info(f"🚀 [ChatGuru] Disparando Task: Digitação FGTS para o Chat {chat_id}")
        
        celery_app.send_task(
            "app.tasks.api_processor.executar_digitacao_fgts_chatguru",
            kwargs={"chat_id": chat_id, "phone_id": payload.phone_id}
        )
        return {"status": "ok", "fluxo": "digitacao_fgts"}
    
    elif contexto_atual == "aguardando_digitacao_clt":
        logger.info(f"🚀 [ChatGuru] Disparando Task: Digitação CLT para o Chat {chat_id}")

        celery_app.send_task(
            "app.tasks.api_processor.executar_digitacao_clt_chatguru",
            kwargs={"chat_id": chat_id, "phone_id": payload.phone_id}
        )
        return {"status": "ok", "fluxo": "digitacao_clt"}
    
    elif contexto_atual == "verificar_autorizacao_clt":
        logger.info(f"🔄 [ChatGuru] Verificação manual de autorização solicitada (Chat {chat_id})")

        contexto_salvo = session.get_context(chat_id)
        if not contexto_salvo or not contexto_salvo.get("cpf"):
            logger.error(f"❌ [ChatGuru] Sessão perdida para o Chat {chat_id}")
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": "❌ [Sessão Expirada] Cliente clicou para verificar autorização, mas o contexto no Redis expirou ou foi perdido."}, force_internal=True)
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": "⚠️ Puxa, parece que demoramos um pouquinho e nossa sessão expirou. Um consultor humano vai dar continuidade no seu atendimento de onde paramos!"})
            chatguru.start_put_in_queue(chat_id)
            return {"status": "erro", "msg": "Sessão expirada"}
        
        cpf = contexto_salvo.get("cpf")
        nome = contexto_salvo.get("nome", "")
        celular = contexto_salvo.get("celular", "")
        phone_id = contexto_salvo.get("phone_id")
        contact_id = contexto_salvo.get("contact_id")

        celery_app.send_task(
            "app.tasks.api_processor.executar_fluxo_clt_chatguru",
            kwargs={
                "chat_id": chat_id,
                "cpf": cpf,
                "celular": celular,
                "nome": nome,
                "phone_id": phone_id,
                "contact_id": contact_id,
                "enviar_link": False,
                "verificacao_manual": True
            }
        )
        return {"status": "ok", "fluxo": "verificar_autorizacao_clt"}

    elif contexto_atual == "atualizar_telefone_clt":
        logger.info(f"📱 [ChatGuru] Atualização de telefone solicitada (Chat {chat_id})")

        contexto_salvo = session.get_context(chat_id)
        if not contexto_salvo or not contexto_salvo.get("cpf"):
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": "❌ [Sessão Expirada] Cliente tentou informar um novo telefone, mas o contexto no Redis expirou."}, force_internal=True)
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": "⚠️ Ops, nossa sessão expirou. Vou te passar para um consultor finalizar isso para você!"})
            chatguru.start_put_in_queue(chat_id)
            return {"status": "erro", "msg": "Sessão expirada"}
        
        tentativas = contexto_salvo.get("tentativas_telefone", 1)
        if tentativas >= 1:
            logger.warning(f"🚫 [ChatGuru] Loop de telefone detectado para o Chat {chat_id}. Transferindo para humano.")
            msg_interna = f"🚫 [Limite de Tentativas] Cliente tentou inserir um novo telefone {tentativas + 1} vezes, mas todos foram barrados pela regra de negócio."
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": msg_interna}, force_internal=True)
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": "⚠️ Parece que os telefones informados já estão associados a outros cadastros. Para agilizar, vou transferir seu atendimento para um de nossos especialistas analisar isso para você, ok?"})
            chatguru.start_put_in_queue(chat_id)

            session.clear_session(chat_id)
            return {"status": "erro", "msg": "Limite de tentativas excedido"}
        
        cpf = contexto_salvo.get("cpf")
        nome = contexto_salvo.get("nome", "")
        contact_id = contexto_salvo.get("contact_id")
        phone_id = contexto_salvo.get("phone_id")

        novo_telefone = formatar_telefone_br(payload.texto_mensagem)

        if not novo_telefone:
            msg_interna = f"🚫 [Telefone Inválido] O cliente enviou um telefone inválido no fluxo de correção:\nDigitou: {payload.texto_mensagem}"
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": msg_interna}, force_internal=True)
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": "⚠️ O formato do telefone não foi reconhecido pelo nosso sistema. Vou chamar um atendente para te ajudar com isso."})
            chatguru.start_put_in_queue(chat_id)
            return {"status": "erro", "msg": "Telefone inválido"}
        
        contexto_salvo["tentativas_telefone"] = tentativas + 1
        contexto_salvo["celular"] = novo_telefone
        session.set_context(chat_id, contexto_salvo)

        celery_app.send_task(
            "app.tasks.api_processor.executar_fluxo_clt_chatguru",
            kwargs={
                "chat_id": chat_id,
                "cpf": cpf,
                "celular": novo_telefone,
                "nome": nome,
                "phone_id": phone_id,
                "contact_id": contact_id
            }
        )
        return {"status": "ok", "fluxo": "atualizar_telefone_clt"}

    else:
        logger.warning(f"⚠️ [ChatGuru] Contexto desconhecido recebido: '{contexto_atual}'. Nenhuma task disparada.")
        return {"status": "ignorado", "msg": f"Contexto '{contexto_atual}' não mapeado no roteador."}