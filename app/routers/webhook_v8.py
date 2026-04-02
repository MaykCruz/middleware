import logging
from fastapi import APIRouter, Request
from app.infrastructure.celery import celery_app
from app.services.bot.memory.session import SessionManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhook/v8", tags=["Webhook V8"])

@router.post("/consultas")
async def receber_webhook_v8(request: Request):
    try:
        payload = await request.json()
    except Exception as e:
        logger.error(f"❌ [Webhook V8] Erro ao processar JSON: {str(e)}")
        return {"status": "erro", "mensagem": "JSON inválido"}
    
    event_type = payload.get("type")
    consult_id = payload.get("consultId")
    status = payload.get("status")

    logger.info(f"📥 [Webhook V8] Evento recebido: {event_type} | ConsultId: {consult_id} | Status: {status}")

    if not event_type or not consult_id:
        logger.info("ℹ️ [Webhook V8] Payload sem 'type' ou 'consultId'. Possível ping de validação.")
        return {"status": "ok", "mensagem": "Webhook validado"}
    
    if event_type == "private.consignment.consult.updated":
        if status in ["SUCCESS", "REJECTED"]:
            session_manager = SessionManager()
            contexto_v8 = session_manager.get_v8_context(consult_id)

            if not contexto_v8:
                logger.info(f"♻️ [Webhook V8] Requisição ignorada: Contexto já resolvido ou expirado para o consultId {consult_id}.")
                return {"status": "ignorado", "mensagem": "Contexto não encontrado no Redis"}
            
            session_manager.delete_v8_context(consult_id)  # Evita reprocessamento de webhooks duplicados
            
            chat_id = contexto_v8.get("chat_id")
            logger.info(f"🚀 [Webhook V8] Contexto recuperado para o Chat {chat_id}. A despachar Task de continuação...")

            motivo_rejeicao = payload.get("description") or "Reprovado na política da API (Dataprev)"
            margem_liberada = 0.0
            max_parcelas = 0

            if status == "SUCCESS":
                margem_liberada = float(payload.get("availableMarginValue"))
                sim_limit = payload.get("simulationLimit", {})
                max_parcelas = sim_limit.get("installmentsMax")

            celery_app.send_task(
                "app.tasks.api_processor.continuar_fluxo_v8_chatguru",
                kwargs={
                    "chat_id": chat_id,
                    "consult_id": consult_id,
                    "status_v8": status,
                    "margem": margem_liberada,
                    "max_parcelas": max_parcelas,
                    "motivo_rejeicao": motivo_rejeicao,
                    "contexto_v8": contexto_v8
                }
            )
            
        else:
            logger.info(f"⏳ [Webhook V8] Evento ignorado (Status intermédio: {status}).")

    return {"status": "recebido_com_sucesso"}
