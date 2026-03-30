import json
from fastapi import APIRouter, HTTPException, Request, status
from app.tasks.processor import process_webhook_event, processar_retorno_v8_task
from app.events.dispatcher import EventDispatcher
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/webhook")
async def receive_webhook(request: Request):
    """
    Recebe o payload da Huggy.
    1. Verifica se deve ser ignorado (Filtro Rápido).
    2. Se válido (receivedAllMessage aprovada OU closedChat), enfileira.
    """
    try:
        payload = await request.json()

        if EventDispatcher.should_filter_payload(payload):
            return {"status": "ignored", "reason": "Filtered by business rules"}

        if logger.isEnabledFor(logging.DEBUG):
            payload_str = json.dumps(payload, indent=2, ensure_ascii=False)
            logger.debug(f"📦 [DEBUG] Payload Recebido:\n{payload_str}")

        task = process_webhook_event.delay(payload)

        logger.debug(f"📨 [API] Webhook recebido e enfileirado. Task ID: {task.id}")

        return {
            "status": "received",
            "task_id": task.id,
            "message": "Event queued for background processing."
        }
    except Exception as e:
        logger.error(f"Erro ao processar webhook: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.post("/v8/clt", status_code=status.HTTP_200_OK)
async def v8_clt_webhook(request: Request):
    """
    Recebe os eventos do V8 referentes à esteira de CLT.
    """
    try:
        payload = await request.json()
        evento_tipo = payload.get("type")
        status_consulta = payload.get("status")
        consult_id = payload.get("consultId")

        logger.info(f"🔔 [Webhook V8] Evento recebido: {evento_tipo} | Status: {status_consulta} | ID: {consult_id}")

        if evento_tipo == "private.consignment.consult.updated":
            processar_retorno_v8_task.delay(payload)
        else:
            logger.debug(f"⏳ [Webhook V8] Consulta {consult_id} em andamento ({status_consulta}). A ignorar.")

        return {"message": "Webhook processado com sucesso"}

    except Exception as e:
        logger.error(f"❌ [Webhook V8] Erro ao processar payload: {str(e)}")
        return {"message": "Erro interno, mas webhook capturado"}