from app.infrastructure.celery import celery_app
from app.events.dispatcher import EventDispatcher
from celery import shared_task
from app.services.bot.memory.session import SessionManager
from app.integrations.v8.clt.service import V8CLTService
import logging
import httpx

logger = logging.getLogger(__name__)

@celery_app.task(
        name="process_webhook_event",
        bind=True,
        acks_late=True,
        ignore_result=True,
        autoretry_for=(httpx.HTTPError, ConnectionError, TimeoutError),
        retry_backoff=True,
        retry_backoff_max=60,
        max_retries=5
        )
def process_webhook_event(self, payload: dict):
    """
    Task puramente técnica.
    Recebe o payload bruto e entrega para a camada de serviço.
    """
    try:
        request_id = self.request.id
        retry_count = self.request.retries

        if retry_count > 0:
            logger.warning(f"🔄 [Task] Tentativa {retry_count + 1}/5 para Task {request_id}")

        EventDispatcher.dispatch(payload)
        return "Dispatched successfully"
    
    except Exception as e:
        # Se for um erro que definimos no autoretry_for, o Celery já cuidou.
        # Se for um erro de Lógica (Code Error), ele cai aqui e morre (não faz retry).
        logger.error(f"❌ Erro FATAL na task {self.request.id}: {str(e)}")
        # Não damos raise aqui para não gerar loop de retry em erros de código (ex: KeyError)
        # Em um sistema avançado, aqui enviaríamos para uma Dead Letter Queue.
        return f"Failed: {str(e)}"

@shared_task(name="processar_retorno_v8", bind=True, max_retries=3)
def processar_retorno_v8_task(self, payload: dict):
    
    # 1. Extrai os dados direto do payload completo
    consult_id = payload.get("consultId")
    status_v8 = payload.get("status")
    motivo = payload.get("description", "")
    margem_v8 = float(payload.get("availableMarginValue", 0.0))
    
    if not consult_id:
        logger.error("❌ [Worker V8] Payload recebido sem consultId. Abortando.")
        return "Falha"

    # 2. Abre a Cápsula do Tempo
    session = SessionManager()
    contexto = session.get_v8_context(consult_id)
    if not contexto: 
        logger.error(f"❌ [Worker V8] Cápsula do tempo não encontrada para {consult_id}.")
        return "Contexto ausente"
        
    chat_id = contexto.get("chat_id")
    oferta_pausada = contexto.get("oferta_pausada", {})

    v8_service = V8CLTService()

    if status_v8 == "SUCCESS":
        logger.info(f"🎉 [Worker V8] Consulta aprovada! Gerando simulação para Chat {chat_id} (Margem: {margem_v8})...")

        resultado_simulacao = v8_service.gerar_simulacao_final(consult_id, margem_v8)

        if resultado_simulacao["acao"] == "SIMULACAO_CONCLUIDA":
            dados = resultado_simulacao["dados"]
            valor_liquido = dados.get("disbursed_issue_amount", 0.0)

            msg_cliente = f"✅ Ótimas notícias! Sua análise VIP com a V8 foi aprovada.\nConseguimos liberar um valor aproximado de *R$ {valor_liquido}*!\n\nVamos dar andamento?"
            nota_interna = f"🟢 V8 Aprovada (Simulação Gerada)\n• ID: {consult_id}\n• Valor: R$ {valor_liquido}"

            logger.info(f"✅ [Worker V8] Simulação gerada: R$ {valor_liquido}. Enviando para o WhatsApp...")

        else:
            logger.warning(f"⚠️ [Worker V8] Aprovado no Dataprev, mas sem tabelas/ofertas para o Chat {chat_id}.")
            msg_cliente = "Infelizmente, embora sua análise tenha avançado, não há tabelas disponíveis para o seu perfil no momento."

    elif status_v8 == "REJECTED":
        logger.info(f"🚫 [Worker V8] Consulta negada para Chat {chat_id}. Motivo: {motivo}")
        
        msg_cliente = "Poxa, acabamos de receber o retorno da sua análise VIP e infelizmente o banco não aprovou a liberação neste momento."
        nota_interna = f"🔴 V8 Recusada\n• ID: {consult_id}\n• Motivo Dataprev: {motivo}"
        
        # chatguru_service.enviar_mensagem(chat_id, msg_cliente)
        # chatguru_service.adicionar_nota_interna(chat_id, nota_interna)
        
    return f"Processamento V8 finalizado com status {status_v8}"

