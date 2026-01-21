import logging
from celery.exceptions import MaxRetriesExceededError, Retry
from app.infrastructure.celery import celery_app
from app.services.products.fgts_service import FGTSService
from app.services.products.clt_service import CLTService
from app.integrations.huggy.service import HuggyService
from app.schemas.credit import AnalysisStatus
from app.utils.formatters import formatar_moeda

logger = logging.getLogger(__name__)

def _safe_error_string(e: Exception) -> str:
    err_msg = str(e)
    return err_msg[:200]

@celery_app.task(name="app.tasks.api_processor.executar_fluxo_fgts", bind=True, acks_late=True)
def executar_fluxo_fgts(self, chat_id: str, cpf: str, nome: str = None, celular: str = None, contact_id: str = None):
    """
    Executa a lógica de FGTS e responde via Huggy.
    Agora com suporte a Retry Inteligente.
    """
    MAX_RETRIES = 10
    COUNTDOWN=30

    tentativa_atual = self.request.retries + 1
    logger.info(f"⚙️ [Worker] Processando FGTS para CPF {cpf} (Tentativa {tentativa_atual})")

    try:
        fgts_service = FGTSService()
        huggy = HuggyService()

        oferta = fgts_service.consultar_melhor_oportunidade(cpf)

        logger.info(f"📤 [Worker FGTS] Resultado: {oferta.status} | Msg: {oferta.message_key} | ChatId: {chat_id}")

        if oferta.status == AnalysisStatus.PROCESSAMENTO_PENDENTE:
            if self.request.retries == 0 or self.request.retries % 3 == 0:
                msg_original = oferta.variables.get("blank", "Processamento pendente.")

                msg_enriquecida = (
                    f"{msg_original}\n\n"
                    f"🔄 *Reconsulta automática:*\n"
                    f"Reconsultando em {COUNTDOWN}s... (Tentativa {tentativa_atual}/{MAX_RETRIES})"
                )

                oferta.variables["blank"] = msg_enriquecida

                huggy.send_message(
                    chat_id=chat_id,
                    message_key=oferta.message_key,
                    variables=oferta.variables,
                    force_internal=oferta.is_internal
                )
            raise self.retry(countdown=COUNTDOWN, max_retries=MAX_RETRIES)
        
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
    
    except MaxRetriesExceededError:
        logger.info(f"⏰ [Worker FGTS] Timeout: Desistindo após {MAX_RETRIES} tentativas.")
        try:
            timeout_handler = HuggyService()
            timeout_handler.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": "Limite de tentativas de processamento excedido."},
                force_internal=True)
            timeout_handler.send_message(
                chat_id=chat_id,
                message_key="clt_limite_tentativas"
            )
        except Exception:
            pass

        HuggyService().start_auto_distribution(chat_id)
    
    except Exception as e:
        if isinstance(e, Retry):
            raise e  # Re-raise Retry exceptions to let Celery handle them
        logger.error(f"💥 [Worker FGTS] Erro crítico: {e}", exc_info=True)
        try:
            erro_handler = HuggyService()
            erro_handler.send_message(
                chat_id=chat_id,
                message_key="retorno_desconhecido",
                variables={"erro": _safe_error_string(e)},
                force_internal=True)
        except Exception as send_error:
            logger.error(f"⚠️ [Fallback] Falha ao enviar mensagem de erro técnica para o Huggy: {send_error}")
        
        try:
            HuggyService().start_auto_distribution(chat_id)
        except Exception as final_error:
            logger.critical(f"☠️ [Fallback] Falha catastrófica ao tentar transbordo manual: {final_error}")
            
@celery_app.task(name="app.tasks.api_processor.executar_fluxo_clt", bind=True, acks_late=True)
def executar_fluxo_clt(self, chat_id: str, cpf: str, nome: str, celular: str, contact_id: str = None, enviar_link: bool = True):
    """
    Executa a lógica pesada de CLT e responde via Huggy.
    Suporta retry automático para status PROCESSAMENTO_PENDENTE.
    """
    MAX_RETRIES = 10
    COUNTDOWN=30

    tentativa_atual = self.request.retries + 1

    logger.info(f"⚙️ [Worker] Processando CLT para CPF {cpf} (Tentativa {tentativa_atual})")

    try:
        clt_service = CLTService()
        huggy = HuggyService()

        oferta = clt_service.consultar_oportunidade(cpf, nome, celular, enviar_link=enviar_link)

        logger.info(f"📤 [Worker] Resultado: {oferta.status} | MsgKey: {oferta.message_key} | ChatId: {chat_id}")

        if oferta.status == AnalysisStatus.PROCESSAMENTO_PENDENTE:
            if self.request.retries == 0 or self.request.retries % 3 == 0:
                msg_original = oferta.variables.get("blank", "Processamento pendente.")

                msg_enriquecida = (
                    f"{msg_original}\n\n"
                    f"⏳ *Fila de Espera Facta:*\n"
                    f"Reconsultando em {COUNTDOWN}s... (Tentativa {tentativa_atual}/{MAX_RETRIES})"
                )

                oferta.variables["blank"] = msg_enriquecida

                huggy.send_message(
                    chat_id=chat_id,
                    message_key=oferta.message_key,
                    variables=oferta.variables,
                    force_internal=oferta.is_internal
                )
            raise self.retry(countdown=COUNTDOWN, max_retries=MAX_RETRIES)

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
        
        elif oferta.status == AnalysisStatus.EMPREGADOR_CPF:
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
        
        elif oferta.status == AnalysisStatus.EMPRESA_RECENTE:
            msg_tecnica = oferta.raw_details.get("msg_tecnica")
            huggy.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg_tecnica},
            force_internal=True)
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("CELETISTA_RESTRICAO"))

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
        logger.info(f"⏰ [Worker CLT] Timeout: Limite de tentativas excedido para {cpf}")
        try:
            timeout_handler = HuggyService()
            timeout_handler.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": "Limite de tentativas de processamento excedido."},
                force_internal=True)
            timeout_handler.send_message(
                chat_id=chat_id,
                message_key="clt_limite_tentativas"
            )
        except Exception:
            pass

        HuggyService().start_auto_distribution(chat_id)

    except Exception as e:
        if isinstance(e, Retry):
            raise e  # Re-raise Retry exceptions to let Celery handle them
        logger.error(f"💥 [Worker CLT] Erro crítico: {e}", exc_info=True)
        try:
            erro_handler = HuggyService()
            erro_handler.send_message(
                chat_id=chat_id,
                message_key="retorno_desconhecido",
                variables={"erro": _safe_error_string(e)},
                force_internal=True)
        except Exception as send_error:
            logger.error(f"⚠️ [Fallback] Falha ao enviar mensagem de erro técnica para o Huggy: {send_error}")
        
        try:
            HuggyService().start_auto_distribution(chat_id)
        except Exception as final_error:
            logger.critical(f"☠️ [Fallback] Falha catastrófica ao tentar transbordo manual: {final_error}")
        