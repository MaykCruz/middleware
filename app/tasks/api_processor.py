import logging
from celery.exceptions import MaxRetriesExceededError, Retry
from app.infrastructure.celery import celery_app
from app.services.products.fgts_service import FGTSService
from app.services.products.clt_service import CLTService
from app.integrations.huggy.service import HuggyService
from app.schemas.credit import AnalysisStatus
from app.utils.formatters import formatar_moeda

logger = logging.getLogger(__name__)

@celery_app.task(name="app.tasks.api_processor.executar_fluxo_fgts", acks_late=True)
def executar_fluxo_fgts(chat_id: str, cpf: str, nome: str = None, celular: str = None, contact_id: str = None):
    """
    Executa a lógica de FGTS e responde via Huggy.
    """
    logger.info(f"⚙️ [Worker] Processando FGTS para CPF {cpf}")

    try:
        fgts_service = FGTSService()
        huggy = HuggyService()

        oferta = fgts_service.consultar_melhor_oportunidade(cpf)

        logger.info(f"📤 [Worker FGTS] Resultado: {oferta.status} | Msg: {oferta.message_key}")

        huggy.send_message(
            chat_id=chat_id,
            message_key=oferta.message_key,
            variables=oferta.variables,
            force_internal=oferta.is_internal
        )

        if oferta.status == AnalysisStatus.APROVADO:
            huggy.move_to_aprovado(chat_id)
            huggy.start_auto_distribution(chat_id)
        
        elif oferta.status == AnalysisStatus.SEM_AUTORIZACAO:
            huggy.start_flow_authorization(chat_id)
        
        elif oferta.status == AnalysisStatus.SEM_ADESAO:
            huggy.start_auto_distribution(chat_id)
        
        elif oferta.status == AnalysisStatus.MUDANCAS_CADASTRAIS:
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("MUDANCAS_CADASTRAIS"))
        
        elif oferta.status == AnalysisStatus.ANIVERSARIANTE:
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("ANIVERSARIANTE"))
        
        elif oferta.status == AnalysisStatus.SALDO_NAO_ENCONTRADO:
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("SALDO_NAO_ENCONTRADO"))

        elif oferta.status == AnalysisStatus.SEM_SALDO:
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("SEM_SALDO"))
        
        elif oferta.status == AnalysisStatus.LIMITE_EXCEDIDO_CONSULTAS_FGTS:
            huggy.start_auto_distribution(chat_id)
        
        elif oferta.status == AnalysisStatus.RETORNO_DESCONHECIDO:
            huggy.start_auto_distribution(chat_id)
    
    except Exception as e:
        logger.error(f"💥 [Worker FGTS] Erro crítico: {e}", exc_info=True)
        erro_handler = HuggyService()
        erro_handler.send_message(
            chat_id=chat_id,
            message_key="retorno_desconhecido",
            variables={"erro": str(e)},
            force_internal=True)
        erro_handler.start_auto_distribution(chat_id)
            
@celery_app.task(name="app.tasks.api_processor.executar_fluxo_clt", bind=True, acks_late=True)
def executar_fluxo_clt(self, chat_id: str, cpf: str, nome: str, celular: str, contact_id: str = None, enviar_link: bool = True):
    """
    Executa a lógica pesada de CLT e responde via Huggy.
    Suporta retry automático para status PROCESSAMENTO_PENDENTE.
    """
    tentativa = self.request.retries + 1

    logger.info(f"⚙️ [Worker] Processando CLT para CPF {cpf} (Tentativa {tentativa})")

    try:
        clt_service = CLTService()
        huggy = HuggyService()

        oferta = clt_service.consultar_oportunidade(cpf, nome, celular, enviar_link=enviar_link)

        logger.info(f"📤 [Worker] Resultado: {oferta.status} | MsgKey: {oferta.message_key} | ChatId: {chat_id}")

        if oferta.status == AnalysisStatus.PROCESSAMENTO_PENDENTE:
            if self.request.retries == 0 or self.request.retries % 3 == 0:
                huggy.send_message(
                    chat_id=chat_id,
                    message_key=oferta.message_key,
                    variables=oferta.variables,
                    force_internal=oferta.is_internal
                )
            raise self.retry(countdown=30, max_retries=10)

        huggy.send_message(
            chat_id=chat_id,
            message_key=oferta.message_key,
            variables=oferta.variables,
            force_internal=oferta.is_internal
        )

        if oferta.status == AnalysisStatus.APROVADO:
            huggy.move_to_aprovado(chat_id)
            huggy.start_auto_distribution(chat_id)
        
        elif oferta.status == AnalysisStatus.AGUARDANDO_AUTORIZACAO:
            huggy.start_flow_wait_term(chat_id)
        
        elif oferta.status == AnalysisStatus.AINDA_AGUARDANDO_AUTORIZACAO:
            huggy.start_auto_distribution(chat_id)
        
        elif oferta.status == AnalysisStatus.TELEFONE_VINCULADO_OUTRO_CPF:
            huggy.start_auto_distribution(chat_id)
        
        elif oferta.status == AnalysisStatus.RETORNO_DESCONHECIDO:
            huggy.start_auto_distribution(chat_id)
        
        elif oferta.status == AnalysisStatus.CPF_NAO_ENCONTRADO_NA_BASE:
            msg = oferta.raw_details.get("msg_tecnica")
            huggy.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg},
            force_internal=True)
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("CLT_RECUSA_DEFINITIVA"))
        
        elif oferta.status == AnalysisStatus.NAO_ELEGIVEL:
            msg = oferta.raw_details.get("msg_tecnica")
            huggy.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg},
            force_internal=True)
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("CLT_RECUSA_DEFINITIVA"))
        
        elif oferta.status == AnalysisStatus.IDADE_INSUFICIENTE_FACTA:
            idade = oferta.raw_details.get("idade")
            sugestao = oferta.raw_details.get("sugestao_bancos", "Verificar outros bancos.")
            huggy.send_message(chat_id=chat_id,
            message_key="idade_insuficiente_facta",
            variables={"sugestao": sugestao},
            force_internal=True)
            huggy.start_auto_distribution(chat_id)
        
        elif oferta.status == AnalysisStatus.IDADE_INSUFICIENTE:
            idade = oferta.raw_details.get("idade")
            huggy.send_message(chat_id=chat_id,
            message_key="idade_insuficiente",
            variables={"idade": idade},
            force_internal=True)
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("CLT_RECUSA_DEFINITIVA"))
        
        elif oferta.status == AnalysisStatus.SEM_MARGEM:
            msg = oferta.raw_details.get("msg_tecnica")
            huggy.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg},
            force_internal=True)
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("SEM_MARGEM_CLT"))
        
        elif oferta.status == AnalysisStatus.CATEGORIA_CNAE_INVALIDA:
            categoria = oferta.raw_details.get("categoria")
            huggy.send_message(chat_id=chat_id,
            message_key="categoria_invalida",
            variables={"categoria": categoria},
            force_internal=True)
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("CLT_RECUSA_DEFINITIVA"))
        
        elif oferta.status == AnalysisStatus.REPROVADO_POLITICA_FACTA:
            msg_tecnica = oferta.raw_details.get("msg_tecnica")
            huggy.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg_tecnica},
            force_internal=True)
            huggy.start_auto_distribution(chat_id)
        
        elif oferta.status == AnalysisStatus.LIMITE_CONTRATOS:
            msg_tecnica = oferta.raw_details.get("msg_tecnica")
            huggy.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg_tecnica},
            force_internal=True)
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("CLT_RECUSA_DEFINITIVA"))
        
        elif oferta.status == AnalysisStatus.MENOS_SEIS_MESES:
            msg_tecnica = oferta.raw_details.get("msg_tecnica")
            huggy.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg_tecnica},
            force_internal=True)
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("MENOS_SEIS_MESES"))

        elif oferta.status == AnalysisStatus.SEM_OFERTA:
            huggy.send_message(chat_id=chat_id,
            message_key="sem_oferta_disponivel",
            force_internal=True)
            huggy.start_auto_distribution(chat_id)
        
        elif oferta.status == AnalysisStatus.VIRADA_FOLHA:
            huggy.send_message(chat_id=chat_id,
            message_key="clt_virada_folha",
            force_internal=True)
            huggy.start_auto_distribution(chat_id)
        
        elif oferta.status == AnalysisStatus.ERRO_TECNICO:
            huggy.start_auto_distribution(chat_id)
    
    except MaxRetriesExceededError:
        logger.error(f"⏰ [Worker CLT] Timeout: Limite de tentativas excedido para {cpf}")
        timeout_handler = HuggyService()
        timeout_handler.send_message(
            chat_id=chat_id,
            message_key="blank",
            variables={"blank": "Limite de tentativas de processamento excedido."},
            force_internal=True)
        timeout_handler.start_auto_distribution(chat_id)

    except Exception as e:
        if isinstance(e, Retry):
            raise e  # Re-raise Retry exceptions to let Celery handle them
        logger.error(f"💥 [Worker CLT] Erro crítico: {e}", exc_info=True)
        erro_handler = HuggyService()
        erro_handler.send_message(
            chat_id=chat_id,
            message_key="retorno_desconhecido",
            variables={"erro": str(e)},
            force_internal=True)
        erro_handler.start_auto_distribution(chat_id)
        