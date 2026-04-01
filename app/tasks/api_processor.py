import logging
import httpx
from celery.exceptions import MaxRetriesExceededError, Retry
from app.infrastructure.celery import celery_app
from app.services.bot.memory.session import SessionManager
from app.integrations.facta.auth import create_client
from app.services.products.fgts_service import FGTSService
from app.services.products.clt_service import CLTService
from app.services.proposal_service import ProposalService
from app.integrations.facta.proposal.client import FactaContratoAndamentoError
from app.schemas.credit import AnalysisStatus
from app.integrations.chatguru.service import ChatGuruService
from app.integrations.v8.clt.service import V8CLTService
from app.utils.formatters import formatar_moeda

logger = logging.getLogger(__name__)

def _safe_error_string(e: Exception) -> str:
    err_msg = str(e)
    return err_msg[:200]

@celery_app.task(name="app.tasks.api_processor.continuar_fluxo_v8_chatguru", bind=True, acks_late=True, max_retries=3)
def continuar_fluxo_v8_chatguru(self, chat_id: str, consult_id: str, status_v8: str, margem: float, max_parcelas: int, motivo_rejeicao: str = ""):
    logger.info(f"⚙️ [Worker V8] Retomando atendimento para o Chat {chat_id} | ConsultID: {consult_id}")

    session_manager = SessionManager()

    contexto_v8 = session_manager.get_v8_context(consult_id)
    if not contexto_v8:
        logger.warning(f"⚠️ [Worker V8] Contexto {consult_id} não encontrado. Possível webhook duplicado já processado.")
        return
    
    session_manager.delete_v8_context(consult_id) # Garante idempotência
    
    phone_id = contexto_v8.get("phone_id")
    idade = contexto_v8.get("idade", 0)
    meses_casa = contexto_v8.get("meses_casa", 0)
    meses_empresa = contexto_v8.get("meses_empresa", 0)
    texto_todas_matriculas = contexto_v8.get("texto_todas_matriculas", "")
    qtd_vinculos = contexto_v8.get("lista_vinculos_len", 1)
    mensagem_espera_enviada = contexto_v8.get("mensagem_espera_enviada", False)

    chatguru = ChatGuruService(chat_id=chat_id, phone_id=phone_id)
    v8_service = V8CLTService()

    clt_service = CLTService(http_client=httpx.Client(timeout=30.0))
    sugestoes = clt_service._gerar_sugestoes_transbordo(idade, meses_casa, meses_empresa)
    sugestao_v8 = next((s for s in sugestoes if "V8" in s), None)
    if sugestao_v8:
        sugestoes.remove(sugestao_v8)

    texto_conclusao_v8 = ""
    v8_simulacao_valida = False

    if status_v8 == "SUCCESS":
        logger.info(f"🎯 [Worker V8] Dataprev Aprovou! Iniciando simulação de R$ {margem} em {max_parcelas}x...")
        try:
            simulacao = v8_service.gerar_simulacao_final(consult_id, margem, max_parcelas)

            if simulacao.get("acao") == "SIMULACAO_CONCLUIDA":
                dados_sim = simulacao.get("dados", {})
                if isinstance(dados_sim, list) and len(dados_sim) > 0: 
                    dados_sim = dados_sim[0]

                valor_liberado = dados_sim.get("disbursed_issue_amount", 0.0)
                v8_simulacao_valida = True
                texto_conclusao_v8 = (
                    f"\n\n🚀 *V8: APROVADO!*\n"
                    f"• Margem Utilizada: R$ {formatar_moeda(margem)}\n"
                    f"• Prazo: {max_parcelas}x\n"
                    f"• Valor Líquido Liberado: R$ {formatar_moeda(valor_liberado)}"
                )
            else:
                texto_conclusao_v8 = f"\n\n⚠️ *V8: APROVADO!* (Dataprev validou R$ {formatar_moeda(margem)}, mas falha na simulação. Tente manual)."
        except Exception as e:
            logger.error(f"❌ [Worker V8] Falha ao processar simulação aprovada: {str(e)}")
            texto_conclusao_v8 = f"\n\n⚠️ *V8: APROVADO!* (Falha ao extrair parcelas)."
    
    elif status_v8 == "REJECTED":
         texto_conclusao_v8 = f"\n\n❌ *V8: REPROVADO!* Motivo: {motivo_rejeicao}"

    titulo = f"⚠️ *Atenção: Cliente possui {qtd_vinculos} matrícula(s) para análise!*\n\n" if qtd_vinculos > 1 else ""
    nota_final = f"{titulo}{texto_todas_matriculas}{texto_conclusao_v8}"

    beco_sem_saida = not v8_simulacao_valida and len(sugestoes) == 0

    if beco_sem_saida:
        chatguru.send_message(
            chat_id=chat_id,
            message_key="clt_recusa_definitiva",
        )
    else:
        if not mensagem_espera_enviada:
            chatguru.send_message(
                chat_id=chat_id, 
                message_key="clt_nao_elegivel", 
                variables={}
            )

    chatguru.send_message(
    chat_id=chat_id, 
    message_key="blank", 
    variables={"blank": nota_final},
    force_internal=True
    )

    if beco_sem_saida:
        chatguru.tag_recusa_definitiva(chat_id)
        chatguru.finish_attendance(chat_id)
        logger.info(f"✅ [Worker V8] Atendimento {chat_id} encerrado (Recusa Definitiva - Beco sem saída).")
    else:
        chatguru.start_put_in_queue(chat_id) 
        logger.info(f"✅ [Worker V8] Atendimento {chat_id} transferido com sucesso para a fila.")

@celery_app.task(name="app.tasks.api_processor.executar_fluxo_fgts_chatguru", bind=True, acks_late=True, autoretry_for=(httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError), retry_backoff=True, max_retries=3, retry_jitter=True)
def executar_fluxo_fgts_chatguru(self, chat_id: str, cpf: str, phone_id: str = None, nome: str = None, celular: str = None, contact_id: str = None, verificacao_manual: bool = False):
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
    chatguru = ChatGuruService(chat_id=chat_id, phone_id=phone_id)

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
            AnalysisStatus.APROVADO,
            AnalysisStatus.SEM_ADESAO

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
                chatguru.tag_com_proposta(chat_id)
            
            else:
                logger.info(f"⚠️ [Worker ChatGuru FGTS] Cliente {cpf} aprovado mas sem dados bancários completos. Seguindo fluxo padrão.")
                chatguru.preparar_mensagem_dialogo(
                    message_key=oferta.message_key,
                    variables=oferta.variables
                )
                chatguru.start_flow_com_valor_sem_conta(chat_id)
                chatguru.tag_com_proposta(chat_id)
        
        elif oferta.status == AnalysisStatus.SEM_AUTORIZACAO:
            if verificacao_manual or self.request.retries > 0:
                MAX_AUTH_RETRIES = 3
                AUTH_DELAY = 30

                if self.request.retries < MAX_AUTH_RETRIES:
                    if self.request.retries == 0 or self.request.retries % 3 == 0:
                        logger.info(f"🔄 [FGTS Termos] Autorização na Caixa ainda não refletiu. Retentando em {AUTH_DELAY}s... ({tentativa_atual}/{MAX_AUTH_RETRIES})")
                        
                        msg_enriquecida = (
                            f"Sem autorização!\n\n"
                            f"🔄 *Reconsultando em {COUNTDOWN}s... (Tentativa {tentativa_atual}/{MAX_AUTH_RETRIES})*"
                        )
                        chatguru.send_message(chat_id=chat_id, message_key="blank", variables={"blank": msg_enriquecida}, force_internal=True)
                        
                    raise self.retry(countdown=AUTH_DELAY, max_retries=MAX_AUTH_RETRIES)
                
                else:
                    logger.info(f"🛑 [Worker ChatGuru FGTS] Loop interrompido. Autorização não encontrada após limite. Distribuindo Chat {chat_id}.")
                    chatguru.send_message(
                        chat_id=chat_id, 
                        message_key="blank", 
                        variables={"blank": "Poxa, fiz várias tentativas mas o sistema da Caixa continua a dizer que não estamos autorizados a consultar. Vou transferir o seu atendimento para um atendente o ajudar a verificar o que se passa no aplicativo! 👨‍💻"}
                    )
                    chatguru.start_put_in_queue(chat_id)
            else:
                logger.info(f"⚠️ [Worker ChatGuru FGTS] Cliente sem autorização. Enviando fluxo padrão.")
                chatguru.tag_sem_autorizacao(chat_id)
                chatguru.start_flow_authorization(chat_id)
        
        elif oferta.status == AnalysisStatus.SEM_ADESAO:
            chatguru.tag_sem_adesao(chat_id)
            chatguru.start_flow_sem_adesao(chat_id)
        
        elif oferta.status == AnalysisStatus.MUDANCAS_CADASTRAIS:
            chatguru.tag_mudancas_cadastrais(chat_id)
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.ANIVERSARIANTE:
            chatguru.tag_aniversariante(chat_id)
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.SALDO_NAO_ENCONTRADO:
            chatguru.tag_saldo_nao_encontrado(chat_id)
            chatguru.start_saldo_nao_encontrado(chat_id)

        elif oferta.status == AnalysisStatus.SEM_SALDO:
            chatguru.tag_sem_saldo(chat_id)
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
        
        if isinstance(e, (httpx.TimeoutException, httpx.ConnectError)):
            raise e
        
        logger.error(f"💥 [Worker ChatGuru FGTS] Erro crítico: {e}", exc_info=True)
        try:
            chatguru.send_message(
                chat_id=chat_id,
                message_key="retorno_desconhecido",
                variables={"erro": _safe_error_string(e)},
                force_internal=True)
        except Exception as send_error:
            logger.error(f"⚠️ [Fallback] Falha ao enviar mensagem de erro técnica para o Chatguru: {send_error}")
        
        try:
            chatguru.start_put_in_queue(chat_id)
        except Exception as final_error:
            logger.critical(f"☠️ [Fallback] Falha catastrófica ao tentar transbordo manual: {final_error}")

@celery_app.task(name="app.tasks.api_processor.executar_fluxo_clt_chatguru", bind=True, acks_late=True, autoretry_for=(httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError), retry_backoff=True, max_retries=3, retry_jitter=True)
def executar_fluxo_clt_chatguru(self, chat_id: str, cpf: str, nome: str, celular: str, phone_id: str = None, contact_id: str = None, enviar_link: bool = True, verificacao_manual=False):
    """
    Executa a lógica pesada de CLT e responde via CHATGURU.
    """
    MAX_RETRIES = 10
    COUNTDOWN = 30

    tentativa_atual = self.request.retries + 1
    logger.info(f"⚙️ [Worker ChatGuru CLT] Processando CLT para CPF {cpf} (Tentativa {tentativa_atual})")

    facta_http_client = create_client()
    clt_service = CLTService(http_client=facta_http_client)
    chatguru = ChatGuruService(chat_id=chat_id, phone_id=phone_id)

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
        
        if oferta.status == AnalysisStatus.AGUARDANDO_WEBHOOK:
            chatguru.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": "Análise V8 em andamento. Aguardando resultado..."},
                force_internal=True
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
                chatguru.tag_com_proposta(chat_id)
                alerta_extra = oferta.raw_details.get("nota_interna_extra")
                if alerta_extra:
                    logger.info(f"💡 [Worker ChatGuru CLT] Múltiplas matrículas detectadas. Enviando alerta interno para {chat_id}")
                    chatguru.send_message(chat_id=chat_id,
                    message_key="blank",
                    variables={"blank": alerta_extra},
                    force_internal=True)

            else:
                logger.info(f"⚠️ [Worker ChatGuru CLT] Cliente {cpf} aprovado mas sem dados bancários completos. Seguindo fluxo padrão.")
                chatguru.preparar_mensagem_dialogo(
                    message_key=oferta.message_key,
                    variables=oferta.variables
                )
                chatguru.start_flow_com_valor_sem_conta(chat_id)
                chatguru.tag_com_proposta(chat_id)
                alerta_extra = oferta.raw_details.get("nota_interna_extra")
                if alerta_extra:
                    logger.info(f"💡 [Worker ChatGuru CLT] Múltiplas matrículas detectadas. Enviando alerta interno para {chat_id}")
                    chatguru.send_message(chat_id=chat_id,
                    message_key="blank",
                    variables={"blank": alerta_extra},
                    force_internal=True)
        
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
            chatguru.tag_recusa_definitiva(chat_id)
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.NAO_ELEGIVEL:
            msg = oferta.raw_details.get("msg_tecnica")
            chatguru.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg},
            force_internal=True)
            chatguru.tag_recusa_definitiva(chat_id)
            chatguru.finish_attendance(chat_id)

        elif oferta.status == AnalysisStatus.EMPREGADOR_CPF:
            msg = oferta.raw_details.get("msg_tecnica")
            chatguru.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg},
            force_internal=True)
            chatguru.tag_recusa_definitiva(chat_id)
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.IDADE_INSUFICIENTE_FACTA:
            idade = oferta.raw_details.get("idade")
            sugestao = oferta.raw_details.get("sugestao_bancos", "Verificar outros bancos.")
            chatguru.send_message(chat_id=chat_id,
            message_key="idade_insuficiente_facta",
            variables={"sugestao": sugestao},
            force_internal=True)
            chatguru.start_put_in_queue(chat_id)
            chatguru.move_to_simular_outros_bancos(chat_id)

        elif oferta.status == AnalysisStatus.IDADE_INSUFICIENTE:
            idade = oferta.raw_details.get("idade")
            chatguru.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": f"Idade do cliente {idade}, não atinge os critérios dos bancos."},
            force_internal=True)
            chatguru.tag_recusa_definitiva(chat_id)
            chatguru.finish_attendance(chat_id)

        elif oferta.status == AnalysisStatus.SEM_MARGEM:
            msg = oferta.raw_details.get("msg_tecnica")
            chatguru.send_message(chat_id=chat_id,
            message_key="blank",
            variables={"blank": msg},
            force_internal=True)
            chatguru.tag_sem_margem(chat_id)
            chatguru.finish_attendance(chat_id)

        elif oferta.status == AnalysisStatus.CATEGORIA_CNAE_INVALIDA:
            categoria = oferta.raw_details.get("categoria")
            chatguru.send_message(chat_id=chat_id,
            message_key="categoria_invalida",
            variables={"categoria": categoria},
            force_internal=True)
            chatguru.tag_recusa_definitiva(chat_id)
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status == AnalysisStatus.REPROVADO_POLITICA_FACTA:
            msg_tecnica = oferta.raw_details.get("sugestao_bancos") or oferta.raw_details.get("msg_tecnica")
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
            chatguru.tag_recusa_definitiva(chat_id)
            chatguru.finish_attendance(chat_id)
        
        elif oferta.status in [AnalysisStatus.EMPRESA_RECENTE, AnalysisStatus.MENOS_SEIS_MESES, AnalysisStatus.CELETISTA_RESTRICAO]:
            msg_interna = oferta.raw_details.get("msg_tecnica")

            chatguru.send_message(
                chat_id=chat_id,
                message_key="blank",
                variables={"blank": msg_interna},
                force_internal=True
            )

            if oferta.status == AnalysisStatus.EMPRESA_RECENTE:
                chatguru.tag_celestista_restricao(chat_id)
            elif oferta.status == AnalysisStatus.MENOS_SEIS_MESES:
                chatguru.tag_tempo_registro(chat_id)
                
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
            if phone_id == "69ab40f8711da456330ae71c":
                chatguru.start_macica_3589(chat_id)
            else:
                chatguru.start_macica_8037(chat_id)
        
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

        if isinstance(e, (httpx.TimeoutException, httpx.ConnectError)):
            raise e
        
        logger.error(f"💥 [Worker ChatGuru CLT] Erro crítico: {e}", exc_info=True)
        try:
            chatguru.send_message(chat_id=chat_id, message_key="retorno_desconhecido", variables={"erro": _safe_error_string(e)}, force_internal=True)
        except Exception as send_error: logger.error(f"⚠️ [Fallback] Falha ao enviar mensagem de erro técnica para o ChatGuru: {send_error}")
        try: chatguru.start_put_in_queue(chat_id)
        except Exception as final_error: logger.critical(f"☠️ [Fallback] Falha catastrófica ao tentar transbordo manual: {final_error}")

@celery_app.task(name="app.tasks.api_processor.executar_digitacao_fgts_chatguru", bind=True, acks_late=True, autoretry_for=(httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError), retry_backoff=True, max_retries=3, retry_jitter=True)
def executar_digitacao_fgts_chatguru(self, chat_id: str, phone_id: str = None):
    """
    Task responsável por efetivar a proposta na Facta (Digitação).
    Acionada quando o cliente confirma a contratação.
    """
    logger.info(f"✍️ [Worker ChatGuru] Iniciando Digitação FGTS para Chat {chat_id}")

    facta_http_client = create_client()
    proposal_service = ProposalService(facta_http_client)
    chatguru = ChatGuruService(chat_id=chat_id, phone_id=phone_id)

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
        if isinstance(e, (httpx.TimeoutException, httpx.ConnectError)):
            raise e
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
def executar_digitacao_clt_chatguru(self, chat_id: str, phone_id: str = None):
    """
    Task responsável por efetivar a proposta na Facta (Digitação) - CLT.
    """
    logger.info(f"✍️ [Worker ChatGuru] Iniciando Digitação CLT para Chat {chat_id}")

    facta_http_client = create_client()
    proposal_service = ProposalService(facta_http_client)
    chatguru = ChatGuruService(chat_id=chat_id, phone_id=phone_id)

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
        chatguru.tag_contrato_andamento(chat_id)

        chatguru.finish_attendance(chat_id)
    
    except Exception as e:
        if isinstance(e, (httpx.TimeoutException, httpx.ConnectError)):
            raise e
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

@celery_app.task(name="app.tasks.api_processor.watchdog_v8", bind=True)
def watchdog_v8(self, chat_id: str, consult_id: str):
    logger.info(f"🐕 [Watchdog V8] Verificando se a consulta {consult_id} travou no limbo...")

    session_manager = SessionManager()
    contexto_v8 = session_manager.get_v8_context(consult_id)

    if not contexto_v8:
        logger.info(f"✅ [Watchdog V8] Consulta {consult_id} já resolvida. Tudo certo!")
        return
    
    logger.warning(f"⚠️ [Watchdog V8] Consulta {consult_id} deu timeout! Resgatando cliente do limbo...")

    pphone_id = contexto_v8.get("phone_id")
    texto_original = contexto_v8.get("texto_bruto_watchdog", "")
    mensagem_espera_enviada = contexto_v8.get("mensagem_espera_enviada", False)
    qtd_vinculos = contexto_v8.get("lista_vinculos_len", 1)

    chatguru = ChatGuruService(chat_id=chat_id, phone_id=pphone_id)
    session_manager.delete_v8_context(consult_id)

    if not mensagem_espera_enviada:
        chatguru.send_message(chat_id=chat_id, message_key="clt_nao_elegivel")
    
    titulo = f"⚠️ *Atenção: Cliente possui {qtd_vinculos} matrícula(s) para análise!*\n\n" if qtd_vinculos > 1 else ""
    nota_final = (
        f"{titulo}{texto_original}\n\n"
        "🤖 *Aviso Interno:* A API automática do V8 sofreu instabilidade e não respondeu. "
        "O cliente foi devolvido para a fila para simulação manual."
    )

    chatguru.send_message(
        chat_id=chat_id, 
        message_key="blank", 
        variables={"blank": nota_final},
        force_internal=True
    )

    chatguru.start_put_in_queue(chat_id)
    logger.info(f"🚨 [Watchdog V8] Cliente {chat_id} transferido para fila com sucesso.")

