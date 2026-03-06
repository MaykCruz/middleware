import logging
import httpx
from celery.exceptions import MaxRetriesExceededError, Retry
from app.infrastructure.celery import celery_app
from app.integrations.facta.auth import create_client
from app.services.products.fgts_service import FGTSService
from app.services.products.clt_service import CLTService
from app.services.proposal_service import ProposalService
from app.integrations.facta.proposal.client import FactaContratoAndamentoError
from app.integrations.huggy.service import HuggyService
from app.schemas.credit import AnalysisStatus
from app.integrations.chatguru.service import ChatGuruService
from app.utils.formatters import formatar_moeda

logger = logging.getLogger(__name__)

def _safe_error_string(e: Exception) -> str:
    err_msg = str(e)
    return err_msg[:200]

@celery_app.task(name="app.tasks.api_processor.executar_fluxo_fgts", bind=True, acks_late=True, autoretry_for=(httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError), retry_backoff=True, max_retries=3, retry_jitter=True)
def executar_fluxo_fgts(self, chat_id: str, cpf: str, nome: str = None, celular: str = None, contact_id: str = None):
    """
    Executa a lógica de FGTS e responde via Huggy.
    Agora com suporte a Retry Inteligente.
    """
    MAX_RETRIES = 10
    COUNTDOWN=30

    tentativa_atual = self.request.retries + 1
    logger.info(f"⚙️ [Worker] Processando FGTS para CPF {cpf} (Tentativa {tentativa_atual})")

    facta_http_client = create_client()
    fgts_service = FGTSService(http_client=facta_http_client)
    huggy = HuggyService()

    try:
        oferta = fgts_service.consultar_melhor_oportunidade(cpf, chat_id)

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
            detalhes = oferta.raw_details.get("detalhes") or oferta.raw_details
            dados_bancarios = detalhes.get("dados_bancarios")

            if isinstance(dados_bancarios, dict) and dados_bancarios:
                logger.info(f"🎯 [Worker FGTS] Cliente {cpf} já possui conta ({dados_bancarios.get('banco')}). Disparando Fluxo de Auto-Contratação.")
        
                huggy.start_flow_digitacao_fgts(chat_id)
            
            else:
                logger.info(f"⚠️ [Worker FGTS] Cliente {cpf} aprovado mas sem dados bancários completos. Seguindo fluxo padrão.")
                huggy.move_to_aprovado(chat_id)
                huggy.start_auto_distribution(chat_id)
        
        elif oferta.status == AnalysisStatus.SEM_AUTORIZACAO:
            huggy.start_flow_authorization(chat_id)
        
        elif oferta.status == AnalysisStatus.SEM_ADESAO:
            huggy.start_flow_sem_adesao(chat_id)
        
        elif oferta.status == AnalysisStatus.MUDANCAS_CADASTRAIS:
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("MUDANCAS_CADASTRAIS"))
        
        elif oferta.status == AnalysisStatus.ANIVERSARIANTE:
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("ANIVERSARIANTE"))
        
        elif oferta.status == AnalysisStatus.SALDO_NAO_ENCONTRADO:
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("SALDO_NAO_ENCONTRADO"))

        elif oferta.status == AnalysisStatus.SEM_SALDO:
            huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("SEM_SALDO"))
        
        elif oferta.status == AnalysisStatus.LIMITE_EXCEDIDO_CONSULTAS_FGTS:
            huggy.start_put_in_queue(chat_id)
        
        elif oferta.status == AnalysisStatus.RETORNO_DESCONHECIDO:
            huggy.start_put_in_queue(chat_id)
    
    except MaxRetriesExceededError:
        logger.info(f"⏰ [Worker FGTS] Timeout: Desistindo após {MAX_RETRIES} tentativas.")
        try:
            huggy.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": "Limite de tentativas de processamento excedido."},
                force_internal=True)
            huggy.send_message(
                chat_id=chat_id,
                message_key="clt_limite_tentativas"
            )
        except Exception:
            pass
        huggy.start_put_in_queue(chat_id)
    
    except Exception as e:
        if isinstance(e, Retry):
            raise e  # Re-raise Retry exceptions to let Celery handle them
        
        if isinstance(e, (httpx.TimeoutException, httpx.ConnectError)):
            raise e

        logger.error(f"💥 [Worker FGTS] Erro crítico: {e}", exc_info=True)
        try:
            huggy.send_message(
                chat_id=chat_id,
                message_key="retorno_desconhecido",
                variables={"erro": _safe_error_string(e)},
                force_internal=True)
        except Exception as send_error:
            logger.error(f"⚠️ [Fallback] Falha ao enviar mensagem de erro técnica para o Huggy: {send_error}")
        
        try:
            huggy.start_put_in_queue(chat_id)
        except Exception as final_error:
            logger.critical(f"☠️ [Fallback] Falha catastrófica ao tentar transbordo manual: {final_error}")
   

@celery_app.task(name="app.tasks.api_processor.executar_fluxo_clt", bind=True, acks_late=True, autoretry_for=(httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError), retry_backoff=True, max_retries=3, retry_jitter=True)
def executar_fluxo_clt(self, chat_id: str, cpf: str, nome: str, celular: str, contact_id: str = None, enviar_link: bool = True, verificacao_manual=False):
    """
    Executa a lógica pesada de CLT e responde via Huggy.
    Suporta retry automático para status PROCESSAMENTO_PENDENTE.
    """
    MAX_RETRIES = 10
    COUNTDOWN=30

    tentativa_atual = self.request.retries + 1
    logger.info(f"⚙️ [Worker] Processando CLT para CPF {cpf} (Tentativa {tentativa_atual})")

    facta_http_client = create_client()
    clt_service = CLTService(http_client=facta_http_client)
    huggy = HuggyService()

    try:
        oferta = clt_service.consultar_oportunidade(cpf, nome, celular, chat_id, enviar_link=enviar_link)

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
        
        elif oferta.status == AnalysisStatus.AINDA_AGUARDANDO_AUTORIZACAO:
            MAX_AUTH_RETRIES = 10
            AUTH_DELAY = 30

            if self.request.retries < MAX_AUTH_RETRIES:
                if self.request.retries == 0 or self.request.retries % 3 == 0:
                    logger.info(f"🔄 [Termos] Autorização ainda não caiu. Retentando em {AUTH_DELAY}s... ({tentativa_atual}/{MAX_AUTH_RETRIES})")
                    msg_original = oferta.variables.get("blank", "Processamento pendente.")

                    msg_enriquecida = (
                        f"{msg_original}\n\n"
                        f"⏳ *Autorização do termo não identificada:*\n"
                        f"Reconsultando em {COUNTDOWN}s... (Tentativa {tentativa_atual}/{MAX_RETRIES})"
                    )

                    oferta.variables["blank"] = msg_enriquecida

                    huggy.send_message(
                        chat_id=chat_id,
                        message_key=oferta.message_key,
                        variables=oferta.variables,
                        force_internal=oferta.is_internal
                    )

                raise self.retry(countdown=AUTH_DELAY, max_retries=MAX_AUTH_RETRIES)
            
            else:

                if verificacao_manual:
                    logger.info(f"🛑 [Worker] Loop interrompido. Autorização não encontrada após verificação manual. Distribuindo Chat {chat_id}.")

                    huggy.send_message(
                        chat_id=chat_id,
                        message_key="blank",
                        variables={"blank": "Poxa, ainda não consegui identificar sua autorização no sistema. Vou transferir para um atendente humano te ajudar, só um momento! 👨‍💻"}
                    )
                    huggy.start_put_in_queue(chat_id)

                else:
                    logger.info(f"⚠️ [Worker] Autorização pendente. Enviando para Flow de Espera (Loop 1).")
                    huggy.send_message(
                        chat_id=chat_id,
                        message_key="clt_termo_nao_identificado"
                    )
                    huggy.start_flow_wait_term2(chat_id)

        huggy.send_message(
            chat_id=chat_id,
            message_key=oferta.message_key,
            variables=oferta.variables,
            force_internal=oferta.is_internal
        )

        if oferta.status == AnalysisStatus.APROVADO:
            detalhes = oferta.raw_details.get("detalhes") or oferta.raw_details
            dados_bancarios = detalhes.get("dados_bancarios")

            if isinstance(dados_bancarios, dict) and dados_bancarios:
                logger.info(f"🎯 [Worker CLT] Cliente {cpf} já possui conta ({dados_bancarios.get('banco')}). Disparando Fluxo de Auto-Contratação.")
        
                huggy.start_flow_digitacao_clt(chat_id)
            
            else:
                logger.info(f"⚠️ [Worker CLT] Cliente {cpf} aprovado mas sem dados bancários completos. Seguindo fluxo padrão.")
                huggy.move_to_aprovado(chat_id)
                huggy.start_auto_distribution(chat_id)
        
        elif oferta.status == AnalysisStatus.AGUARDANDO_AUTORIZACAO:
            huggy.start_flow_wait_term(chat_id)
        
        elif oferta.status == AnalysisStatus.TELEFONE_VINCULADO_OUTRO_CPF:
            huggy.start_flow_telefone_vinculado(chat_id)
        
        elif oferta.status == AnalysisStatus.RETORNO_DESCONHECIDO:
            huggy.start_put_in_queue(chat_id)
        
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
            huggy.start_put_in_queue(chat_id)
            huggy.move_to_simular_outros_bancos(chat_id)
        
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
            huggy.start_put_in_queue(chat_id)
            huggy.move_to_simular_outros_bancos(chat_id)
        
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
            msg_tecnica = oferta.raw_details.get("msg_tecnica")
            huggy.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg_tecnica},
            force_internal=True)
            huggy.start_put_in_queue(chat_id)
            huggy.move_to_simular_outros_bancos(chat_id)
        
        elif oferta.status == AnalysisStatus.VIRADA_FOLHA:
            huggy.send_message(chat_id=chat_id,
            message_key="clt_virada_folha",
            force_internal=True)
            huggy.start_put_in_queue(chat_id)
        
        elif oferta.status == AnalysisStatus.ERRO_TECNICO:
            huggy.start_put_in_queue(chat_id)
    
    except MaxRetriesExceededError:
        logger.info(f"⏰ [Worker CLT] Timeout: Limite de tentativas excedido para {cpf}")
        try:
            huggy.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": "Limite de tentativas de processamento excedido."},
                force_internal=True)
            huggy.send_message(
                chat_id=chat_id,
                message_key="clt_limite_tentativas"
            )
        except Exception:
            pass
        huggy.start_put_in_queue(chat_id)

    except Exception as e:
        if isinstance(e, Retry):
            raise e  # Re-raise Retry exceptions to let Celery handle them

        if isinstance(e, (httpx.TimeoutException, httpx.ConnectError)):
            raise e
        
        logger.error(f"💥 [Worker CLT] Erro crítico: {e}", exc_info=True)
        try:
            huggy.send_message(
                chat_id=chat_id,
                message_key="retorno_desconhecido",
                variables={"erro": _safe_error_string(e)},
                force_internal=True)
        except Exception as send_error:
            logger.error(f"⚠️ [Fallback] Falha ao enviar mensagem de erro técnica para o Huggy: {send_error}")
        
        try:
            huggy.start_put_in_queue(chat_id)
        except Exception as final_error:
            logger.critical(f"☠️ [Fallback] Falha catastrófica ao tentar transbordo manual: {final_error}")

@celery_app.task(name="app.tasks.api_processor.executar_digitacao_fgts", bind=True, acks_late=True, autoretry_for=(httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError), retry_backoff=True, max_retries=3, retry_jitter=True)
def executar_digitacao_fgts(self, chat_id: str):
    """
    Task responsável por efetivar a proposta na Facta (Digitação).
    Acionada quando o cliente confirma a contratação.
    """
    logger.info(f"✍️ [Worker] Iniciando Digitação FGTS para Chat {chat_id}")

    facta_http_client = create_client()
    proposal_service = ProposalService(facta_http_client)
    huggy = HuggyService()

    try:
        huggy.send_message(chat_id, message_key="iniciando_digitacao")
        huggy.move_to_digitacao(chat_id)

        resultado = proposal_service.executar_digitacao_fgts(chat_id)

        url_link = resultado.get("url_formalizacao")
        codigo_af = resultado.get("codigo")

        if url_link:
            logger.info(f"✅ [Worker] Sucesso! AF: {codigo_af} | Link: {url_link}")

            msg_interna = f"✅ Proposta Gerada!\n🆔 Código AF: {codigo_af}\n🔗 Link: {url_link}"

            huggy.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": msg_interna},
                force_internal=True
            )

            huggy.send_message(
                chat_id=chat_id,
                message_key="link_formalizacao",
                variables={"link": url_link}
            )

            huggy.transfer_maria_luiza(chat_id)

            huggy.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": "ag formalizar"},
                force_internal=True
            )
        
        else:
            raise ValueError("API Facta retornou sucesso mas sem URL de formalização.")
    
    except Exception as e:
        if isinstance(e, (httpx.TimeoutException, httpx.ConnectError)):
            raise e
        try:
            huggy.send_message(
                chat_id=chat_id,
                message_key="retorno_desconhecido",
                variables={"erro": _safe_error_string(e)},
                force_internal=True)
        except Exception as send_error:
            logger.error(f"⚠️ [Fallback] Falha ao enviar mensagem de erro técnica para o Huggy: {send_error}")
        
        try:
            huggy.start_put_in_queue(chat_id)
        except Exception as final_error:
            logger.critical(f"☠️ [Fallback] Falha catastrófica ao tentar transbordo manual: {final_error}")

@celery_app.task(name="app.tasks.api_processor.executar_digitacao_clt", bind=True, acks_late=True, autoretry_for=(httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError), retry_backoff=True, max_retries=3, retry_jitter=True)
def executar_digitacao_clt(self, chat_id: str):
    """
    Task responsável por efetivar a proposta na Facta (Digitação) - CLT.
    """
    logger.info(f"✍️ [Worker] Iniciando Digitação CLT para Chat {chat_id}")

    facta_http_client = create_client()
    proposal_service = ProposalService(facta_http_client)
    huggy = HuggyService()

    try:
        huggy.send_message(chat_id, message_key="iniciando_digitacao")
        huggy.move_to_digitacao(chat_id)

        resultado = proposal_service.executar_digitacao_clt(chat_id)

        url_link = resultado.get("url_formalizacao")
        codigo_af = resultado.get("codigo")

        if url_link:
            logger.info(f"✅ [Worker] Sucesso CLT! AF: {codigo_af} | Link: {url_link}")

            msg_interna = f"✅ Proposta CLT Gerada!\n🆔 Código AF: {codigo_af}\n🔗 Link: {url_link}"

            huggy.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": msg_interna},
                force_internal=True
            )

            huggy.send_message(
                chat_id=chat_id,
                message_key="link_formalizacao",
                variables={"link": url_link}
            )

            huggy.transfer_maria_luiza(chat_id)

            huggy.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": "ag formalizar"},
                force_internal=True
            )

        else:
            raise ValueError("API Facta retornou sucesso mas sem URL de formalização.")
    
    except FactaContratoAndamentoError:
        logger.warning(f"⚠️ [Worker] Digitação bloqueada: Contrato já existente para Chat {chat_id}")

        huggy.send_message(
            chat_id=chat_id,
            message_key="clt_contrato_andamento"
        )

        huggy.finish_attendance(chat_id, tabulation_id=huggy.tabulations.get("CONTRATO_ANDAMENTO"))
    
    except Exception as e:
        if isinstance(e, (httpx.TimeoutException, httpx.ConnectError)):
            raise e
        try:
            huggy.send_message(
                chat_id=chat_id,
                message_key="retorno_desconhecido",
                variables={"erro": _safe_error_string(e)},
                force_internal=True)
        except Exception as send_error:
            logger.error(f"⚠️ [Fallback] Falha ao enviar mensagem de erro técnica para o Huggy: {send_error}")
        
        try:
            huggy.start_put_in_queue(chat_id)
        except Exception as final_error:
            logger.critical(f"☠️ [Fallback] Falha catastrófica ao tentar transbordo manual: {final_error}")

@celery_app.task(name="app.tasks.api_processor.executar_fluxo_fgts_chatguru", bind=True, acks_late=True, autoretry_for=(httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError), retry_backoff=True, max_retries=3, retry_jitter=True)
def executar_fluxo_fgts_chatguru(self, chat_id: str, cpf: str, nome: str = None, celular: str = None, contact_id: str = None):
    """
    Executa a lógica de FGTS e responde via CHATGURU.
    Agora com suporte a Retry Inteligente.
    """
    MAX_RETRIES = 10
    COUNTDOWN=30

    tentativa_atual = self.request.retries + 1
    logger.info(f"⚙️ [Worker ChatGuru FGTS] Processando FGTS para CPF {cpf} (Tentativa {tentativa_atual})")

    facta_http_client = create_client()
    fgts_service = FGTSService(http_client=facta_http_client)
    chatguru = ChatGuruService(chat_id)

    try:
        oferta = fgts_service.consultar_melhor_oportunidade(cpf, chat_id)

        logger.info(f"📤 [Worker ChatGuru FGTS] Resultado: {oferta.status} | Msg: {oferta.message_key} | ChatId: {chat_id}")

        if oferta.status == AnalysisStatus.PROCESSAMENTO_PENDENTE:
            if self.request.retries == 0 or self.request.retries % 3 == 0:
                msg_original = oferta.variables.get("blank", "Processamento pendente.")

                msg_enriquecida = (
                    f"{msg_original}\n\n"
                    f"🔄 *Reconsulta automática:*\n"
                    f"Reconsultando em {COUNTDOWN}s... (Tentativa {tentativa_atual}/{MAX_RETRIES})"
                )

                oferta.variables["blank"] = msg_enriquecida

                chatguru.send_message(
                    chat_id=chat_id,
                    message_key=oferta.message_key,
                    variables=oferta.variables,
                    force_internal=oferta.is_internal
                )
            raise self.retry(countdown=COUNTDOWN, max_retries=MAX_RETRIES)
        
        STATUS_SOMENTE_DIALOGO = [
            AnalysisStatus.SALDO_NAO_ENCONTRADO,
            AnalysisStatus.SEM_AUTORIZACAO,
            AnalysisStatus.APROVADO

        ]

        if oferta.status not in STATUS_SOMENTE_DIALOGO:
            chatguru.send_message(
                chat_id=chat_id,
                message_key=oferta.message_key,
                variables=oferta.variables,
                force_internal=oferta.is_internal
            )

        if oferta.status == AnalysisStatus.APROVADO:
            detalhes = oferta.raw_details.get("detalhes") or oferta.raw_details
            dados_bancarios = detalhes.get("dados_bancarios")

            if isinstance(dados_bancarios, dict) and dados_bancarios:
                logger.info(f"🎯 [Worker ChatGuru FGTS] Cliente {cpf} já possui conta ({dados_bancarios.get('banco')}). Disparando Fluxo de Auto-Contratação.")

                chatguru.preparar_mensagem_dialogo(
                    message_key=oferta.message_key,
                    variables=oferta.variables
                )
        
                chatguru.start_flow_com_saldo_conta(chat_id)
            
            else:
                logger.info(f"⚠️ [Worker ChatGuru FGTS] Cliente {cpf} aprovado mas sem dados bancários completos. Seguindo fluxo padrão.")
                chatguru.preparar_mensagem_dialogo(
                    message_key=oferta.message_key,
                    variables=oferta.variables
                )
                chatguru.start_flow_com_valor_sem_conta(chat_id)
        
        elif oferta.status == AnalysisStatus.SEM_AUTORIZACAO:
            chatguru.start_flow_authorization(chat_id)
        
        elif oferta.status == AnalysisStatus.SEM_ADESAO:
            chatguru.start_flow_sem_adesao(chat_id)
        
        elif oferta.status == AnalysisStatus.MUDANCAS_CADASTRAIS:
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.ANIVERSARIANTE:
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.SALDO_NAO_ENCONTRADO:
            chatguru.start_saldo_nao_encontrado(chat_id)

        elif oferta.status == AnalysisStatus.SEM_SALDO:
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.LIMITE_EXCEDIDO_CONSULTAS_FGTS:
            chatguru.start_put_in_queue(chat_id)
        
        elif oferta.status == AnalysisStatus.RETORNO_DESCONHECIDO:
            chatguru.start_put_in_queue(chat_id)
    
    except MaxRetriesExceededError:
        logger.info(f"⏰ [Worker ChatGuru FGTS] Timeout: Desistindo após {MAX_RETRIES} tentativas.")
        try:
            chatguru.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": "Limite de tentativas de processamento excedido."},
                force_internal=True)
            chatguru.send_message(
                chat_id=chat_id,
                message_key="clt_limite_tentativas"
            )
        except Exception:
            pass
        chatguru.start_put_in_queue(chat_id)
    
    except Exception as e:
        if isinstance(e, Retry):
            raise e  # Re-raise Retry exceptions to let Celery handle them
        logger.error(f"💥 [Worker ChatGuru FGTS] Erro crítico: {e}", exc_info=True)
        try:
            chatguru.send_message(
                chat_id=chat_id,
                message_key="retorno_desconhecido",
                variables={"erro": _safe_error_string(e)},
                force_internal=True)
        except Exception as send_error:
            logger.error(f"⚠️ [Fallback] Falha ao enviar mensagem de erro técnica para o Huggy: {send_error}")
        
        try:
            chatguru.start_put_in_queue(chat_id)
        except Exception as final_error:
            logger.critical(f"☠️ [Fallback] Falha catastrófica ao tentar transbordo manual: {final_error}")

@celery_app.task(name="app.tasks.api_processor.executar_fluxo_clt_chatguru", bind=True, acks_late=True, autoretry_for=(httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError), retry_backoff=True, max_retries=3, retry_jitter=True)
def executar_fluxo_clt_chatguru(self, chat_id: str, cpf: str, nome: str, celular: str, contact_id: str = None, enviar_link: bool = True, verificacao_manual=False):
    """
    Executa a lógica pesada de CLT e responde via CHATGURU.
    """
    MAX_RETRIES = 10
    COUNTDOWN = 30

    tentativa_atual = self.request.retries + 1
    logger.info(f"⚙️ [Worker ChatGuru CLT] Processando CLT para CPF {cpf} (Tentativa {tentativa_atual})")

    facta_http_client = create_client()
    clt_service = CLTService(http_client=facta_http_client)
    chatguru = ChatGuruService(chat_id)

    try:
        oferta = clt_service.consultar_oportunidade(cpf, nome, celular, chat_id, enviar_link=enviar_link)

        logger.info(f"📤 [Worker ChatGuru CLT] Resultado: {oferta.status} | MsgKey: {oferta.message_key} | ChatId: {chat_id}")

        if oferta.status == AnalysisStatus.PROCESSAMENTO_PENDENTE:
            if self.request.retries == 0 or self.request.retries % 3 == 0:
                msg_original = oferta.variables.get("blank", "Processamento pendente.")

                msg_enriquecida = (
                    f"{msg_original}\n\n"
                    f"⏳ *Fila de Espera Facta:*\n"
                    f"Reconsultando em {COUNTDOWN}s... (Tentativa {tentativa_atual}/{MAX_RETRIES})"
                )

                oferta.variables["blank"] = msg_enriquecida

                chatguru.send_message(
                    chat_id=chat_id, 
                    message_key=oferta.message_key, 
                    variables=oferta.variables, 
                    force_internal=oferta.is_internal
                )

            raise self.retry(countdown=COUNTDOWN, max_retries=MAX_RETRIES)
        
        elif oferta.status == AnalysisStatus.AINDA_AGUARDANDO_AUTORIZACAO:
            MAX_AUTH_RETRIES = 10
            AUTH_DELAY = 30

            if self.request.retries < MAX_AUTH_RETRIES:
                if self.request.retries == 0 or self.request.retries % 3 == 0:
                    logger.info(f"🔄 [Termos] Autorização ainda não caiu. Retentando em {AUTH_DELAY}s... ({tentativa_atual}/{MAX_AUTH_RETRIES})")

                    msg_original = oferta.variables.get("blank", "Processamento pendente.")
                    
                    msg_enriquecida = (
                        f"{msg_original}\n\n"
                        f"⏳ *Autorização do termo não identificada:*\n"
                        f"Reconsultando em {COUNTDOWN}s... (Tentativa {tentativa_atual}/{MAX_RETRIES})"
                    )

                    oferta.variables["blank"] = msg_enriquecida

                    chatguru.send_message(
                        chat_id=chat_id, 
                        message_key=oferta.message_key, 
                        variables=oferta.variables, 
                        force_internal=oferta.is_internal
                    )

                raise self.retry(countdown=AUTH_DELAY, max_retries=MAX_AUTH_RETRIES)
            
            else:

                if verificacao_manual:
                    logger.info(f"🛑 [Worker ChatGuru CLT] Loop interrompido. Distribuindo Chat {chat_id}.")

                    chatguru.send_message(
                        chat_id=chat_id, 
                        message_key="blank", 
                        variables={"blank": "Poxa, ainda não consegui identificar sua autorização no sistema. Vou transferir para um atendente humano te ajudar, só um momento! 👨‍💻"}
                    )
                    chatguru.start_put_in_queue(chat_id)

                else:
                    logger.info(f"⚠️ [Worker ChatGuru CLT] Autorização pendente. Enviando para Flow de Espera (Loop 1).")
                    chatguru.send_message(
                        chat_id=chat_id, 
                        message_key="clt_termo_nao_identificado"
                    )
                    #chatguru.start_flow_wait_term2(chat_id) # GENÉRICO - NECESSÁRIO CRIAR FLUXO DE ESPERA DE TERMO NO CHATGURU
                    chatguru.start_put_in_queue(chat_id)
        
        STATUS_SOMENTE_DIALOGO = [
            AnalysisStatus.APROVADO,
            AnalysisStatus.AGUARDANDO_AUTORIZACAO
        ]

        if oferta.status not in STATUS_SOMENTE_DIALOGO:
            chatguru.send_message(
                chat_id=chat_id, 
                message_key=oferta.message_key, 
                variables=oferta.variables, 
                force_internal=oferta.is_internal
            )

        if oferta.status == AnalysisStatus.APROVADO:
            detalhes = oferta.raw_details.get("detalhes") or oferta.raw_details
            dados_bancarios = detalhes.get("dados_bancarios")

            if isinstance(dados_bancarios, dict) and dados_bancarios:
                logger.info(f"🎯 [Worker ChatGuru CLT] Cliente {cpf} já possui conta. Disparando Fluxo de Auto-Contratação.")

                chatguru.preparar_mensagem_dialogo(
                    message_key=oferta.message_key,
                    variables=oferta.variables
                )

                chatguru.start_flow_com_margem_conta(chat_id)

            else:
                logger.info(f"⚠️ [Worker ChatGuru CLT] Cliente {cpf} aprovado mas sem dados bancários completos. Seguindo fluxo padrão.")
                chatguru.preparar_mensagem_dialogo(
                    message_key=oferta.message_key,
                    variables=oferta.variables
                )
                chatguru.start_flow_com_valor_sem_conta(chat_id)
        
        elif oferta.status == AnalysisStatus.AGUARDANDO_AUTORIZACAO:
            chatguru.start_flow_wait_term(chat_id)

        elif oferta.status == AnalysisStatus.TELEFONE_VINCULADO_OUTRO_CPF:
            chatguru.start_flow_telefone_vinculado(chat_id)

        elif oferta.status == AnalysisStatus.RETORNO_DESCONHECIDO:
            chatguru.start_put_in_queue(chat_id)
        
        elif oferta.status == AnalysisStatus.CPF_NAO_ENCONTRADO_NA_BASE:
            msg = oferta.raw_details.get("msg_tecnica")
            chatguru.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg},
            force_internal=True)
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.NAO_ELEGIVEL:
            msg = oferta.raw_details.get("msg_tecnica")
            chatguru.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg},
            force_internal=True)
            chatguru.finish_attendance(chat_id)

        elif oferta.status == AnalysisStatus.EMPREGADOR_CPF:
            msg = oferta.raw_details.get("msg_tecnica")
            chatguru.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg},
            force_internal=True)
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.IDADE_INSUFICIENTE_FACTA:
            idade = oferta.raw_details.get("idade")
            sugestao = oferta.raw_details.get("sugestao_bancos", "Verificar outros bancos.")
            chatguru.send_message(chat_id=chat_id,
            variables={"sugestao": sugestao},
            force_internal=True)
            chatguru.start_put_in_queue(chat_id)
            chatguru.move_to_simular_outros_bancos(chat_id)

        elif oferta.status == AnalysisStatus.IDADE_INSUFICIENTE:
            idade = oferta.raw_details.get("idade")
            chatguru.send_message(chat_id=chat_id,
            message_key="idade_insuficiente",
            variables={"idade": idade},
            force_internal=True)
            chatguru.finish_attendance(chat_id)

        elif oferta.status == AnalysisStatus.SEM_MARGEM:
            msg = oferta.raw_details.get("msg_tecnica")
            chatguru.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg},
            force_internal=True)
            chatguru.finish_attendance(chat_id)

        elif oferta.status == AnalysisStatus.CATEGORIA_CNAE_INVALIDA:
            categoria = oferta.raw_details.get("categoria")
            chatguru.send_message(chat_id=chat_id,
            message_key="categoria_invalida",
            variables={"categoria": categoria},
            force_internal=True)
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.REPROVADO_POLITICA_FACTA:
            msg_tecnica = oferta.raw_details.get("msg_tecnica")
            chatguru.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg_tecnica},
            force_internal=True)
            chatguru.start_put_in_queue(chat_id)
            chatguru.move_to_simular_outros_bancos(chat_id)
        
        elif oferta.status == AnalysisStatus.LIMITE_CONTRATOS:
            msg_tecnica = oferta.raw_details.get("msg_tecnica")
            chatguru.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg_tecnica},
            force_internal=True)
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.MENOS_SEIS_MESES:
            msg_tecnica = oferta.raw_details.get("msg_tecnica")
            chatguru.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg_tecnica},
            force_internal=True)
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.EMPRESA_RECENTE:
            msg_tecnica = oferta.raw_details.get("msg_tecnica")
            chatguru.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg_tecnica},
            force_internal=True)
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.SEM_OFERTA:
            msg_tecnica = oferta.raw_details.get("msg_tecnica")
            chatguru.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg_tecnica},
            force_internal=True)
            chatguru.start_put_in_queue(chat_id)
            chatguru.move_to_simular_outros_bancos(chat_id)
        
        elif oferta.status == AnalysisStatus.VIRADA_FOLHA:
            chatguru.send_message(chat_id=chat_id,
            message_key="clt_virada_folha",
            force_internal=True)
            chatguru.start_put_in_queue(chat_id)
        
        elif oferta.status == AnalysisStatus.ERRO_TECNICO:
            chatguru.start_put_in_queue(chat_id)
        
    except MaxRetriesExceededError:
        logger.info(f"⏰ [Worker ChatGuru CLT] Timeout: Limite de tentativas excedido para {cpf}")
        try:
            chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": "Limite de tentativas de processamento excedido."}, force_internal=True)
            chatguru.send_message(chat_id=chat_id, message_key="clt_limite_tentativas")
        except Exception: pass
        chatguru.start_put_in_queue(chat_id)

    except Exception as e:
        if isinstance(e, Retry): raise e
        logger.error(f"💥 [Worker ChatGuru CLT] Erro crítico: {e}", exc_info=True)
        try:
            chatguru.send_message(chat_id=chat_id, message_key="retorno_desconhecido", variables={"erro": _safe_error_string(e)}, force_internal=True)
        except Exception as send_error: logger.error(f"⚠️ [Fallback] Falha ao enviar mensagem de erro técnica para o ChatGuru: {send_error}")
        try: chatguru.start_put_in_queue(chat_id)
        except Exception as final_error: logger.critical(f"☠️ [Fallback] Falha catastrófica ao tentar transbordo manual: {final_error}")

@celery_app.task(name="app.tasks.api_processor.executar_digitacao_fgts_chatguru", bind=True, acks_late=True, autoretry_for=(httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError), retry_backoff=True, max_retries=3, retry_jitter=True)
def executar_digitacao_fgts_chatguru(self, chat_id: str):
    """
    Task responsável por efetivar a proposta na Facta (Digitação).
    Acionada quando o cliente confirma a contratação.
    """
    logger.info(f"✍️ [Worker ChatGuru] Iniciando Digitação FGTS para Chat {chat_id}")

    facta_http_client = create_client()
    proposal_service = ProposalService(facta_http_client)
    chatguru = ChatGuruService(chat_id)

    try:
        chatguru.send_message(chat_id, message_key="iniciando_digitacao")

        resultado = proposal_service.executar_digitacao_fgts(chat_id)

        url_link = resultado.get("url_formalizacao")
        codigo_af = resultado.get("codigo")

        if url_link:
            logger.info(f"✅ [Worker ChatGuru] Sucesso! AF: {codigo_af} | Link: {url_link}")

            msg_interna = f"✅ Proposta Gerada!\n🆔 Código AF: {codigo_af}\n🔗 Link: {url_link}"

            chatguru.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": msg_interna},
                force_internal=True
            )

            chatguru.send_message(
                chat_id=chat_id,
                message_key="link_formalizacao",
                variables={"link": url_link}
            )

            chatguru.transfer_maria_luiza(chat_id)
        
        else:
            raise ValueError("API Facta retornou sucesso mas sem URL de formalização.")
    
    except Exception as e:
        try:
            chatguru.send_message(
                chat_id=chat_id,
                message_key="retorno_desconhecido",
                variables={"erro": _safe_error_string(e)},
                force_internal=True)
        except Exception as send_error:
            logger.error(f"⚠️ [Fallback] Falha ao enviar mensagem de erro técnica para o ChatGuru: {send_error}")
        
        try:
            chatguru.start_put_in_queue(chat_id)
        except Exception as final_error:
            logger.critical(f"☠️ [Fallback] Falha catastrófica ao tentar transbordo manual: {final_error}")

@celery_app.task(name="app.tasks.api_processor.executar_digitacao_clt_chatguru", bind=True, acks_late=True, autoretry_for=(httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError), retry_backoff=True, max_retries=3, retry_jitter=True)
def executar_digitacao_clt_chatguru(self, chat_id: str):
    """
    Task responsável por efetivar a proposta na Facta (Digitação) - CLT.
    """
    logger.info(f"✍️ [Worker ChatGuru] Iniciando Digitação CLT para Chat {chat_id}")

    facta_http_client = create_client()
    proposal_service = ProposalService(facta_http_client)
    chatguru = ChatGuruService(chat_id)

    try:
        chatguru.send_message(chat_id, message_key="iniciando_digitacao")

        resultado = proposal_service.executar_digitacao_clt(chat_id)

        url_link = resultado.get("url_formalizacao")
        codigo_af = resultado.get("codigo")

        if url_link:
            logger.info(f"✅ [Worker ChatGuru] Sucesso CLT! AF: {codigo_af} | Link: {url_link}")

            msg_interna = f"✅ Proposta CLT Gerada!\n🆔 Código AF: {codigo_af}\n🔗 Link: {url_link}"

            chatguru.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": msg_interna},
                force_internal=True
            )

            chatguru.send_message(
                chat_id=chat_id,
                message_key="link_formalizacao",
                variables={"link": url_link}
            )

            chatguru.transfer_maria_luiza(chat_id)

        else:
            raise ValueError("API Facta retornou sucesso mas sem URL de formalização.")
    
    except FactaContratoAndamentoError:
        logger.warning(f"⚠️ [Worker ChatGuru] Digitação bloqueada: Contrato já existente para Chat {chat_id}")

        chatguru.send_message(
            chat_id=chat_id,
            message_key="clt_contrato_andamento"
        )

        chatguru.finish_attendance(chat_id)
    
    except Exception as e:
        try:
            chatguru.send_message(
                chat_id=chat_id,
                message_key="retorno_desconhecido",
                variables={"erro": _safe_error_string(e)},
                force_internal=True)
        except Exception as send_error:
            logger.error(f"⚠️ [Fallback] Falha ao enviar mensagem de erro técnica para o ChatGuru: {send_error}")
        
        try:
            chatguru.start_put_in_queue(chat_id)
        except Exception as final_error:
            logger.critical(f"☠️ [Fallback] Falha catastrófica ao tentar transbordo manual: {final_error}")
