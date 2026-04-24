import logging
from app.core.logger import chat_id_var
from app.infrastructure.database import supabase_client
from app.core.vendedores import EQUIPE_VENDAS
from fastapi import APIRouter
from pydantic import BaseModel
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, Union
from app.integrations.chatguru.service import ChatGuruService
from app.infrastructure.celery import celery_app
from app.utils.validators import validate_cpf, clean_digits, formatar_telefone_br
from app.utils.formatters import limpar_nome, identificar_tipo_chave_pix, sanitizar_valor_pix, obter_codigo_tipo_chave_pix_facta
from app.services.bot.memory.session import SessionManager

router = APIRouter(prefix="/webhooks/chatguru", tags=["Webhook ChatGuru"])
logger = logging.getLogger(__name__)

class BotContext(BaseModel):
    Erro: Optional[bool] = False
    Timer: Optional[bool] = False
    URA: Union[str, bool, None] = None
    Contexto: Union[str, bool, None] = None
    Produto: Union[str, bool, None] = None

class ChatGuruPayload(BaseModel):
    chat_id: str
    celular: str
    nome: str
    texto_mensagem: str
    bot_context: BotContext
    phone_id: Optional[str] = None
    campos_personalizados: Optional[Dict[str, Any]] = None
    executado_por: Optional[str] = None

    class Config:
        extra = "allow"

@router.post("/")
async def receber_webhook_chatguru(payload: ChatGuruPayload):
    """
    Recebe todos os eventos do ChatGuru e roteia para a ação correta com base no 'bot_context.Contexto'.
    """
    contexto_atual = payload.bot_context.Contexto

    chat_id = payload.celular

    chat_id_var.set(chat_id)

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
    
    elif contexto_atual == "aguardando_digitacao_clt_pix":
        logger.info(f"🚀 [ChatGuru] Disparando Task: Digitação CLT com Pix para o Chat {chat_id}")

        contexto_salvo = session.get_context(chat_id)
        if not contexto_salvo or not contexto_salvo.get("cpf"):
            logger.error(f"❌ [ChatGuru] Sessão perdida para o Chat {chat_id} na Digitação.")
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": "⚠️ Nossa sessão expirou. Um consultor vai te ajudar a finalizar a proposta!"})
            chatguru.start_put_in_queue(chat_id)
            return {"status": "erro", "msg": "Sessão expirada"}
        
        cpf_cliente = contexto_salvo.get("cpf", "")
        campos = payload.campos_personalizados or {}
        chave_pix_raw = campos.get("Chave_Pix")
        
        if not chave_pix_raw:
            logger.warning(f"⚠️ [ChatGuru] Cliente {chat_id} não informou a Chave PIX.")
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": "⚠️ Não consegui ler a sua chave PIX. Vou chamar um atendente humano!"})
            chatguru.start_put_in_queue(chat_id)
            return {"status": "erro", "msg": "Chave PIX ausente"}

        tipo_detectado = identificar_tipo_chave_pix(chave_pix_raw, cpf_cliente)
        chave_limpa = sanitizar_valor_pix(chave_pix_raw, tipo_detectado)
        codigo_facta = obter_codigo_tipo_chave_pix_facta(tipo_detectado)

        dados_bancarios_pix = {
            "tipo_dado": "PIX",
            "chave_pix": chave_limpa,
            "tipo_chave_pix": tipo_detectado,
            "codigo_tipo_chave_pix": codigo_facta,
            "origem": "digitacao_manual_bot"
        }

        oferta = contexto_salvo.get("oferta_selecionada", {})
        detalhes = oferta.get("detalhes", {})

        detalhes["dados_bancarios"] = dados_bancarios_pix
        oferta["detalhes"] = detalhes
        contexto_salvo["oferta_selecionada"] = oferta

        session.set_context(chat_id, contexto_salvo)
        logger.info(f"💾 [ChatGuru] Chave PIX ({tipo_detectado}) salva no contexto para {chat_id}.")

        celery_app.send_task(
            "app.tasks.api_processor.executar_digitacao_clt_chatguru",
            kwargs={"chat_id": chat_id, "phone_id": payload.phone_id}
        )
        return {"status": "ok", "fluxo": "digitacao_clt_pix"}
    
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
    
    elif contexto_atual == "agendamento":
        logger.info(f"📅 [ChatGuru] Processando agendamento para o Chat {chat_id}")

        campos = payload.campos_personalizados or {}
        data_str = campos.get("Data")
        hora_str = campos.get("Hora")
        motivo = campos.get("Motivo_agendamento", "Não informado")
        atendente_email = payload.executado_por
        perfil_atendente = EQUIPE_VENDAS.get(atendente_email, {})
        atendente_nome = perfil_atendente.get("nome", atendente_email)

        if not data_str or not hora_str:
            logger.error(f"❌ [Agendamento] Data ou Hora ausentes no Chat {chat_id}")
            return {"status": "erro", "msg": "Campos de data/hora incompletos."}
        
        if not supabase_client:
            return {"status": "erro", "msg": "Banco de dados não configurado."}
        
        try:
            agendamento_dt = datetime.strptime(f"{data_str} {hora_str}", "%Y-%m-%d %H:%M")
            fuso_br = ZoneInfo("America/Sao_Paulo")
            agora_dt = datetime.now(fuso_br).replace(tzinfo=None)

            if agendamento_dt < agora_dt:
                logger.warning(f"⚠️ [Agendamento] Vendedor tentou agendar no passado: {data_str} {hora_str}")
                chatguru.send_message(
                    chat_id=chat_id,
                    message_key="blank",
                    variables={"blank": "⚠️ *O agendamento falhou:*\nA data e hora informadas já passaram! Por favor, insira um horário no futuro."},
                    force_internal=True
                )
                return {"status": "erro", "msg": "Data no passado."}
            
            data_formatada_br = agendamento_dt.strftime("%d/%m/%Y")
            agendamento_iso = agendamento_dt.isoformat()
            
            supabase_client.table("agendamentos").insert({
                "chat_id": chat_id,
                "phone_id": payload.phone_id,
                "atendente": atendente_email,
                "motivo": motivo,
                "data_agendada": agendamento_iso,
                "status": "PENDENTE"
            }).execute()

            logger.info(f"✅ [Agendamento] Salvo no banco para {chat_id} às {agendamento_iso}")
            
            msg_sucesso = f"✅ *Agendamento Confirmado!*\n📅 Data: {data_formatada_br} às {hora_str}\n👤 Atendente: {atendente_nome}\n📝 Motivo: {motivo}\n\nO chat será reaberto automaticamente no horário estipulado."
            chatguru.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": msg_sucesso},
                force_internal=True
            )
            return {"status": "ok", "msg": "Agendamento salvo no banco com sucesso"}
        
        except ValueError as e:
            logger.error(f"❌ [Agendamento] Erro ao formatar data/hora: {e}")
            return {"status": "erro", "msg": "Formato de data/hora inválido."}
        
        except Exception as e:
            logger.error(f"❌ [Agendamento] Falha grave ao salvar no banco: {e}")
            return {"status": "erro", "msg": "Erro interno do servidor."}
        
    elif contexto_atual == "validar_pix_cliente":
        logger.info(f"🔑 [ChatGuru] Validando chave PIX digitada para o Chat {chat_id}")

        contexto_salvo = session.get_context(chat_id)
        cpf_cliente = contexto_salvo.get("cpf", "") if contexto_salvo else ""

        chave_raw = payload.texto_mensagem

        tipo_detectado = identificar_tipo_chave_pix(chave_raw, cpf_cliente)

        if tipo_detectado == "DESCONHECIDO":
            logger.warning(f"⚠️ [ChatGuru] PIX inválido no Chat {chat_id}: {chave_raw}")
            chatguru.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": "Chave pix não identificada."}
            )
            return {"status": "erro", "msg": "PIX inválido"}
        
        chave_limpa = sanitizar_valor_pix(chave_raw, tipo_detectado)
        logger.info(f"✅ [ChatGuru] PIX Válido ({tipo_detectado}). Disparando diálogo de confirmação.")

        msg_confirmacao = (
            f"🔎 *Confirmação de PIX*\n\n"
            f"Identifiquei sua chave como *{tipo_detectado}*.\n"
            f"Chave: *{chave_limpa}*\n\n"
        )
        chatguru.preparar_mensagem_dialogo(
            message_key="blank",
            variables={"blank": msg_confirmacao}
        )

        chatguru.client.update_custom_fields(chat_id, {
            "Chave_Pix": chave_limpa
        })
        
        chatguru.execute_dialog(chat_number=chat_id, dialog_id="69d805b7911870f1d13daa22")

        return {"status": "ok", "fluxo": "validar_pix_cliente"}

    else:
        logger.warning(f"⚠️ [ChatGuru] Contexto desconhecido recebido: '{contexto_atual}'. Nenhuma task disparada.")
        return {"status": "ignorado", "msg": f"Contexto '{contexto_atual}' não mapeado no roteador."}