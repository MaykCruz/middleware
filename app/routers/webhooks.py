import json
from fastapi import APIRouter, HTTPException, Request, status
from app.tasks.processor import process_webhook_event
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
        consult_id = payload.get("consultaId")

        logger.info(f"🔔 [Webhook V8] Evento recebido: {evento_tipo} | Status: {status_consulta} | ID: {consult_id}")

        if evento_tipo == "private.consignment.consult.updated":

            if status_consulta == "SUCCESS":
                logger.info(f"🎉 [Webhook V8] Consulta {consult_id} APROVADA no Dataprev!")
                # Em breve: Aqui disparamos a Task do Celery para buscar a margem e gerar a simulação (tabela)
                # Exemplo: processar_simulacao_v8_task.delay(payload)
                
            elif status_consulta == "REJECTED":
                motivo = payload.get("description", "Motivo não informado")
                logger.warning(f"🚫 [Webhook V8] Consulta {consult_id} REJEITADA. Motivo: {motivo}.")
                # Em breve: Aqui disparamos a Task para enviar mensagem de recusa ao cliente via ChatGuru
                
            else:
                # Eventos intermediários como WAITING_CREDIT_ANALYSIS ou WAITING_CONSENT
                logger.debug(f"⏳ [Webhook V8] Consulta {consult_id} em andamento ({status_consulta}). A ignorar.")

        # Retornamos SEMPRE 200 OK rapidamente para a V8 não travar a fila deles nem tentar reenviar o evento
        return {"message": "Webhook processado com sucesso"}

    except Exception as e:
        logger.error(f"❌ [Webhook V8] Erro ao processar payload: {str(e)}")
        # Mesmo com erro interno (ex: JSON malformado), devolvemos 200 para fechar o circuito com a V8
        return {"message": "Erro interno, mas webhook capturado"}