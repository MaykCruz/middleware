import logging
import httpx
from celery import current_app
from datetime import datetime
from app.integrations.facta.clt.service import FactaCLTService
from app.integrations.v8.clt.service import V8CLTService
from app.services.bank_account_service import BankAccountService
from app.schemas.credit import CreditOffer, AnalysisStatus
from app.services.bot.memory.session import SessionManager
from app.utils.formatters import formatar_moeda, obter_mes_inicio_desconto, formatar_display_tempo, calcular_meses, parse_valor_monetario
from app.utils.schedules import agendar_retentativa_automatica

logger = logging.getLogger(__name__)

class CLTService:
    """
    Service Global de CLT.
    Responsável por consultar múltiplos parceiros (Facta, etc.) e agregar/comparar os resultados.
    """
    def __init__(self, http_client: httpx.Client):
        self.facta_service = FactaCLTService(http_client)
        self.bank_service = BankAccountService(http_client)
        self.session_manager = SessionManager()
        self.v8_service = V8CLTService()

    def _gerar_sugestoes_transbordo(self, idade: int, meses_casa: int, meses_empresa: int) -> list:
        sugestoes = []
        # Mercantil: 20-58 | Casa >= 12 | Empresa >= 36
        if 20 <= idade <= 58 and meses_casa >= 12 and meses_empresa >= 36:
            sugestoes.append("Mercantil (20-58)")
        
        # V8: 21-65 | Casa >= 3 | Empresa >= 36
        if 21 <= idade <= 65 and meses_casa >= 3 and meses_empresa >= 36:
            sugestoes.append("V8 (21-65)")
        
        # C6 Bank: 21-60 | Casa >= 3 | Empresa >= 24
        if 21 <= idade <= 60 and meses_casa >= 3 and meses_empresa >= 24:
            sugestoes.append("C6 Bank (21-60)")
            
        return sugestoes
        
    def consultar_oportunidade(self, cpf: str, nome: str, celular: str, chat_id: str, enviar_link: bool = True) -> CreditOffer:
        """
        Executa o fluxo completo de CPT e retorna uma Oferta Padronizada.
        """
        logger.info(f"💼 [Global CLT] Consultando oportunidade para {cpf}")

        resultado_raw = self.facta_service.simular_clt(cpf, nome, celular, enviar_link_se_necessario=enviar_link)

        aprovado = resultado_raw.get("aprovado")
        motivo = resultado_raw.get("motivo")
        msg_tecnica = resultado_raw.get("msg_tecnica", str(motivo))

        if aprovado:
            oferta_dados = resultado_raw.get("oferta", {})
            trabalhador = oferta_dados.get("dados_trabalhador", {})

            info_conta = self.bank_service.buscar_melhor_conta(cpf)
            dados_bancarios_completo = info_conta["raw"] if info_conta else None

            outras_aprovadas = resultado_raw.get("outras_ofertas_aprovadas", [])
            if outras_aprovadas:
                linhas_alerta = [f"⚠️ *Atenção:* O cliente possui mais {len(outras_aprovadas)} matrícula(s) APROVADA(S)!\n"]
                
                for i, extra in enumerate(outras_aprovadas, 1):
                    oferta_extra = extra.get("oferta", {})
                    trab_extra = oferta_extra.get("dados_trabalhador", {})
                    
                    val_extra = oferta_extra.get("valor_liquido", 0.0)
                    parc_extra = oferta_extra.get("parcela", 0.0)
                    prazo_extra = oferta_extra.get("prazo", 0)
                    mat_extra = trab_extra.get("matricula", "N/A")
                    tab_extra = oferta_extra.get("codigo_tabela", "N/A")
                    
                    linhas_alerta.append(
                        f"*Matrícula Adicional {i}* ({mat_extra})\n"
                        f"• Liberado: R$ {formatar_moeda(val_extra)}\n"
                        f"• Parcela: {prazo_extra}x de R$ {formatar_moeda(parc_extra)}\n"
                        f"• Tabela: {tab_extra}\n"
                    )
                
                linhas_alerta.append("💡 *Dica:* Use esses valores como carta na manga para aumentar a liberação total do cliente!")
                resultado_raw["nota_interna_extra"] = "\n".join(linhas_alerta)

            if chat_id:
                detalhes_oferta = {
                    "codigo_tabela": oferta_dados.get("codigo_tabela"),
                    "prazo": oferta_dados.get("prazo"),
                    "valor_operacao": oferta_dados.get("valor_liquido"),
                    "valor_parcela": oferta_dados.get("parcela"),
                    "coeficiente": oferta_dados.get("coeficiente"),
                    "matricula": trabalhador.get("matricula"),
                    "data_admissao": trabalhador.get("dataAdmissao"),
                    "cnpj_empregador": trabalhador.get("numeroInscricaoEmpregador"),
                    "dados_bancarios": dados_bancarios_completo,
                    "nota_interna_extra": resultado_raw.get("nota_interna_extra", "")
                }
            
                self.session_manager.update_context(chat_id, {
                    "oferta_selecionada": {
                        "produto": "CLT",
                        "detalhes": detalhes_oferta
                    }
                })
                logger.info(f"💾 [CLT] Oferta salva no contexto do Chat {chat_id}")

            val_liquido = oferta_dados.get("valor_liquido", 0.0)
            mes_desconto = obter_mes_inicio_desconto()

            resultado_raw["dados_bancarios"] = dados_bancarios_completo

            if info_conta:
                return CreditOffer(
                    status=AnalysisStatus.APROVADO,
                    message_key="clt_oferta_disponivel_conta",
                    valor_liquido=val_liquido,
                    variables={
                        "valor": formatar_moeda(val_liquido),
                        "parcela": formatar_moeda(oferta_dados.get("parcela", 0.0)),
                        "prazo": str(oferta_dados.get("prazo", 0)),
                        "mes_desconto": mes_desconto,
                        "dados_bancarios": info_conta["texto_formatado"]
                    },
                    banco_origem="Facta",
                    raw_details=resultado_raw
                )
            else:
                return CreditOffer(
                    status=AnalysisStatus.APROVADO,
                    message_key="clt_oferta_disponivel",
                    valor_liquido=val_liquido,
                    variables={
                        "valor": formatar_moeda(val_liquido),
                        "parcela": formatar_moeda(oferta_dados.get("parcela", 0.0)),
                        "prazo": str(oferta_dados.get("prazo", 0)),
                        "mes_desconto": mes_desconto
                    },
                    banco_origem="Facta",
                    raw_details=resultado_raw
                )

        if motivo == "AGUARDANDO_AUTORIZACAO":
            return CreditOffer(
                status=AnalysisStatus.AGUARDANDO_AUTORIZACAO,
                message_key="clt_termo_enviado",
                raw_details=resultado_raw
            )
        
        if motivo == "TERMO_AINDA_PENDENTE":
            return CreditOffer(
                    status=AnalysisStatus.AINDA_AGUARDANDO_AUTORIZACAO,
                    message_key="blank",
                    variables={"blank": msg_tecnica},
                    is_internal=True,
                    raw_details=resultado_raw
                )
        
        if not aprovado:
            if motivo in ["IDADE_INSUFICIENTE_FACTA", "REPROVADO_POLITICA_FACTA", "SEM_OPERACOES", "SEM_PRAZO_COMPATIVEL"]:
                dados_trab = resultado_raw.get("dados_trabalhador", {})

                if not dados_trab:
                    return CreditOffer(
                        status=AnalysisStatus.ERRO_TECNICO,
                        message_key="retorno_desconhecido",
                        is_internal=True,
                        variables={
                        "erro": msg_tecnica
                        },
                        raw_details=resultado_raw
                    )
                
                lista_vinculos = [resultado_raw] + resultado_raw.get("outros_erros", [])

                dados_para_relatorio = []
                sugestoes_globais = set()
                
                idade_principal = 0
                sexo_principal = "Não informado"
                meses_casa_principal = 0
                meses_empresa_principal = 0
                
                for i, vinculo in enumerate(lista_vinculos, 1):
                    dados_trab = vinculo.get("dados_trabalhador", {})
                    if not dados_trab:
                        continue
                    
                    idade_v = int(vinculo.get("idade", 0))
                    if idade_v == 0 and dados_trab.get("dataNascimento"):
                        nasc = datetime.strptime(dados_trab.get("dataNascimento"), "%d/%m/%Y")
                        idade_v = (datetime.today() - nasc).days // 365
                        
                    sexo_v = vinculo.get("sexo", "Não informado")
                    if sexo_v == "Não informado" and dados_trab.get("sexo_codigo"):
                        sexo_v = "F" if str(dados_trab.get("sexo_codigo")) == "3" else "M"
                    
                    v_admissao = vinculo.get("data_admissao") or dados_trab.get("dataAdmissao")
                    v_inicio_empresa = dados_trab.get("dataInicioAtividadeEmpregador")

                    v_margem = float(vinculo.get("margem_disponivel", 0.0))
                    if v_margem == 0.0 and dados_trab.get("valorMargemDisponivel"):
                        v_margem = parse_valor_monetario(dados_trab.get("valorMargemDisponivel"))

                    v_meses_casa = calcular_meses(v_admissao) if v_admissao else 0
                    v_meses_empresa = calcular_meses(v_inicio_empresa) if v_inicio_empresa else 0

                    if i == 1:
                        idade_principal = idade_v
                        sexo_principal = sexo_v
                        meses_casa_principal = v_meses_casa
                        meses_empresa_principal = v_meses_empresa
                    
                    margem_minima_distribuir = 150.00 if v_meses_casa < 12 else 50.00
                    
                    motivo_vinculo = vinculo.get("motivo")
                    erros_fatais = ["NAO_ELEGIVEL", "EMPREGADOR_CPF", "CATEGORIA_CNAE_INVALIDA"]

                    if motivo_vinculo in erros_fatais:
                        v_sugestoes = []
                        msg_erro_v = vinculo.get("msg_tecnica", "Vínculo inválido ou inativo na Dataprev.")

                    elif v_margem < margem_minima_distribuir:
                        v_sugestoes = []
                        msg_erro_v = f"Margem R$ {formatar_moeda(v_margem)} insuficiente."
                    else:
                        v_sugestoes = self._gerar_sugestoes_transbordo(idade_v, v_meses_casa, v_meses_empresa)
                        msg_erro_v = vinculo.get("msg_tecnica", vinculo.get("motivo", "Reprovado na política"))
            
                    sugestoes_globais.update(v_sugestoes)

                    dados_para_relatorio.append({
                        "index": i,
                        "margem": v_margem,
                        "admissao": v_admissao,
                        "empresa": v_inicio_empresa,
                        "erro": msg_erro_v,
                        "sugestoes_locais": v_sugestoes
                    })
                
                sugestoes_visuais = set(sugestoes_globais)
                sugestao_v8_str = next((s for s in sugestoes_globais if "V8" in s), None)

                blocos_brutos = []
                for d in dados_para_relatorio:
                    sugs = d["sugestoes_locais"]
                    txt_sug = ", ".join(sugs) if sugs else "Nenhuma (Incompatível)"
                    blocos_brutos.append(
                        f"{d['erro']}\n"
                        f"📊 *Dados para análise matrícula {d['index']}*\n"
                        f"• Margem: R$ {formatar_moeda(d['margem'])}\n"
                        f"• Admissão: {formatar_display_tempo(d['admissao'])}\n"
                        f"• Empresa: {formatar_display_tempo(d['empresa'])}\n"
                        f"🎯 *Bancos Recomendados:* {txt_sug}"
                    )
                texto_bruto_watchdog = f"👤 *Cliente:* {idade_principal} anos ({sexo_principal})\n\n" + "\n\n".join(blocos_brutos)

                if sugestao_v8_str:
                    sugestoes_visuais.remove(sugestao_v8_str)
                
                blocos_texto = []
                for d in dados_para_relatorio:
                    sug_limpas = [s for s in d["sugestoes_locais"] if s in sugestoes_visuais]
                    texto_sugestao = ", ".join(sug_limpas) if sug_limpas else "Nenhuma (Incompatível)"

                    bloco = (
                        f"{d['erro']}\n"
                        f"📊 *Dados para análise matrícula {d['index']}*\n"
                        f"• Margem: R$ {formatar_moeda(d['margem'])}\n"
                        f"• Admissão: {formatar_display_tempo(d['admissao'])}\n"
                        f"• Empresa: {formatar_display_tempo(d['empresa'])}\n"
                        f"🎯 *Bancos Recomendados:* {texto_sugestao}"
                    )
                    blocos_texto.append(bloco)

                texto_todas_matriculas = f"👤 *Cliente:* {idade_principal} anos ({sexo_principal})\n\n" + "\n\n".join(blocos_texto)

                outros_bancos = [s for s in sugestoes_globais if "V8" not in s]
                tem_outros_bancos = len(outros_bancos) > 0

                texto_conclusao_v8 = ""
                acao_v8 = None
                v8_simulacao_valida = False

                if next((s for s in sugestoes_globais if "V8" in s), None):
                    logger.info(f"⚡ [CLT Service] V8 sugerido no panorama global. A iniciar validação via API...")
                    resultado_v8 = self.v8_service.processar_nova_consulta(cpf)
                    acao_v8 = resultado_v8.get("acao")

                    if acao_v8 == AnalysisStatus.AGUARDANDO_WEBHOOK:
                        consult_id = resultado_v8.get("consult_id")
                        logger.info(f"⏳ [CLT Service] V8 em processamento. A guardar contexto (ID: {consult_id}).")

                        contexto_principal = self.session_manager.get_context(chat_id)
                        phone_id = contexto_principal.get("phone_id")

                        self.session_manager.save_v8_context(consult_id, {
                            "chat_id": chat_id, "phone_id": phone_id, "cpf": cpf, "nome": nome, "celular": celular,
                            "idade": idade_principal, "meses_casa": meses_casa_principal, "meses_empresa": meses_empresa_principal,
                            "texto_todas_matriculas": texto_todas_matriculas, "texto_bruto_watchdog": texto_bruto_watchdog, "lista_vinculados_len": len(lista_vinculos), "mensagem_espera_enviada": tem_outros_bancos
                        })

                        current_app.send_task(
                            "app.tasks.api_processor.watchdog_v8",
                            kwargs={"chat_id": chat_id, "consult_id": consult_id},
                            countdown=900 
                        )

                        chave_msg_espera = "clt_nao_elegivel" if tem_outros_bancos else "blank"
                        variaveis_msg = {} if tem_outros_bancos else {"blank": "⏳ Análise V8 em andamento. Aguardando resultado..."}

                        return CreditOffer(
                            status=AnalysisStatus.AGUARDANDO_WEBHOOK,
                            message_key=chave_msg_espera,
                            variables=variaveis_msg,
                            is_internal=not tem_outros_bancos,
                            raw_details=resultado_raw
                        )
                
                    elif acao_v8 == AnalysisStatus.APROVADO:
                        consult_id = resultado_v8.get("consult_id")
                        margem_v8 = resultado_v8.get("margem")
                        parcelas_v8 = resultado_v8.get("max_parcelas")

                        logger.info(f"⚡ [CLT Service] Triagem V8 aprovada. A iniciar simulação (R$ {margem_v8} em {parcelas_v8}x)...")

                        simulacao = self.v8_service.gerar_simulacao_final(consult_id, margem_v8, parcelas_v8)

                        if simulacao.get("acao") == "SIMULACAO_CONCLUIDA":
                            dados_sim = simulacao.get("dados", {})
                            if isinstance(dados_sim, list) and len(dados_sim) > 0:
                                dados_sim = dados_sim[0]
                            valor_liberado = dados_sim.get("disbursed_issue_amount")
                            prazo_real = dados_sim.get("number_of_installments")
                            margem_real = dados_sim.get("installment_value")

                            v8_simulacao_valida = True
                            texto_conclusao_v8 = (
                                f"\n\n🚀 *V8: APROVADO!*\n"
                                f"• Margem Utilizada: R$ {formatar_moeda(margem_real)}\n"
                                f"• Prazo: {prazo_real}x\n"
                                f"• Valor Líquido Liberado: R$ {formatar_moeda(valor_liberado)}"
                            )
                            resultado_raw["v8_approval"] = True

                            if not tem_outros_bancos:
                                logger.info(f"🚀 [CLT Service] Cliente exclusivo V8. Retornando APROVADO para fluxo clt_com_proposta.")

                                info_conta = self.bank_service.buscar_melhor_conta(cpf)
                                mes_desconto = obter_mes_inicio_desconto()

                                if info_conta:
                                    resultado_raw["dados_bancarios"] = info_conta["raw"]
                                    chave_msg_v8 = "clt_oferta_disponivel_conta"
                                    variaveis_v8 = {
                                        "valor": formatar_moeda(valor_liberado),
                                        "parcela": formatar_moeda(margem_v8),
                                        "prazo": str(parcelas_v8),
                                        "mes_desconto": mes_desconto,
                                        "dados_bancarios": info_conta["texto_formatado"]
                                    }
                                else:
                                    chave_msg_v8 = "clt_oferta_disponivel"
                                    variaveis_v8 = {
                                        "valor": formatar_moeda(valor_liberado),
                                        "parcela": formatar_moeda(margem_v8),
                                        "prazo": str(parcelas_v8),
                                        "mes_desconto": mes_desconto
                                    }
                                
                                return CreditOffer(
                                    status=AnalysisStatus.APROVADO,
                                    message_key=chave_msg_v8,
                                    variables=variaveis_v8,
                                    banco_origem="V8",
                                    raw_details=resultado_raw
                                )
                            else:
                                logger.info(f"⚖️ [CLT Service] V8 Aprovado, mas há outros bancos. Seguindo para transbordo VIP.")
                                pass
                        
                        elif simulacao.get("acao") == "SIMULACAO_BLOQUEADA":
                            detalhe_erro = simulacao.get("mensagem", "Operação em andamento")
                            texto_conclusao_v8 = f"\n\n🚫 *V8: BLOQUEADO!*\nMotivo: {detalhe_erro}"
                            logger.warning(f"🚫 [CLT Service] V8 bloqueado para {consult_id}: {detalhe_erro}")

                        else:
                            texto_conclusao_v8 = f"\n\n❌ *V8: FALHA!* (Erro ao simular tabelas: {simulacao.get('mensagem')})"
                            logger.error(f"❌ [CLT Service] Falha na simulação final V8.")
                
                    elif acao_v8 == AnalysisStatus.REPROVADO_POLITICA_V8:
                        motivo = resultado_v8.get("motivo")
                        texto_conclusao_v8 = f"\n\n❌ *V8: REPROVADO!* Motivo: {motivo}"
                    
                    elif acao_v8 == AnalysisStatus.ERRO_TECNICO:
                        logger.warning(f"⚠️ [CLT Service] V8 instável. Protegendo cliente de recusa indevida.")
                        texto_conclusao_v8 = f"\n\n⚠️ *V8: API INSTÁVEL!* (Falha de comunicação com o banco. Tente simular manualmente no banco)."

                        if not tem_outros_bancos:
                            return CreditOffer(
                                status=AnalysisStatus.ERRO_TECNICO,
                                message_key="retorno_desconhecido",
                                is_internal=True,
                                variables={"erro": f"{texto_todas_matriculas}{texto_conclusao_v8}"},
                                raw_details=resultado_raw
                            )

                if tem_outros_bancos or v8_simulacao_valida:
                    titulo = f"⚠️ *Atenção: Cliente possui {len(lista_vinculos)} matrícula(s) para análise!*\n\n" if len(lista_vinculos) > 1 else ""
                    msg_final = f"{titulo}{texto_todas_matriculas}{texto_conclusao_v8}"

                    return CreditOffer(
                        status=AnalysisStatus.REPROVADO_POLITICA_FACTA,
                        message_key="clt_nao_elegivel",
                        raw_details={**resultado_raw, "sugestao_bancos": msg_final}
                    )
                else:
                    # Se NENHUMA matrícula teve transbordo, processamos a recusa definitiva baseada no Vínculo 1
                    dados_trab_1 = resultado_raw.get("dados_trabalhador", {})
                    admissao_1 = resultado_raw.get("data_admissao") or dados_trab_1.get("dataAdmissao")
                    inicio_empresa_1 = dados_trab_1.get("dataInicioAtividadeEmpregador")
                    meses_casa_1 = calcular_meses(admissao_1) if admissao_1 else 0
                    meses_empresa_1 = calcular_meses(inicio_empresa_1) if inicio_empresa_1 else 0

                    margem_1 = float(resultado_raw.get("margem_disponivel", 0.0))
                    if margem_1 == 0.0 and dados_trab_1.get("valorMargemDisponivel"):
                        margem_1 = parse_valor_monetario(dados_trab_1.get("valorMargemDisponivel"))
                    margem_minima_1 = 150.00 if meses_casa_1 < 12 else 50.00

                    texto_conflito = ""
                    chave_mensagem = "clt_recusa_definitiva"
                    variaveis_mensagem = {}

                    # 1º PRIORIDADE: Margem
                    if margem_1 < margem_minima_1:
                        status_falha = AnalysisStatus.SEM_MARGEM
                        chave_mensagem = "sem_margem_cliente"
                        texto_conflito = f"❌ Margem R$ {formatar_moeda(margem_1)} insuficiente. (Mínimo exigido: R$ {margem_minima_1} p/ {meses_casa_1} meses de casa)."
                    
                    # 2º PRIORIDADE: Idade
                    elif  idade_principal < 20 or idade_principal > 65:
                        status_falha = AnalysisStatus.IDADE_INSUFICIENTE
                        chave_mensagem = "idade_insuficiente" 
                        variaveis_mensagem = {"idade": str(idade_principal)} 
                        texto_conflito = f"❌ *Idade* ({idade_principal} anos) fora das janelas de aprovação de todos os parceiros."
                    
                    # 3º PRIORIDADE: Pouco tempo de carteira assinada
                    elif meses_casa_1 < 3:
                        status_falha = AnalysisStatus.MENOS_SEIS_MESES
                        chave_mensagem = "menos_seis_meses" 
                        texto_conflito = f"❌ *Todos os bancos* exigem no mínimo 3 meses de carteira assinada (Cliente tem apenas {meses_casa_1} meses)."

                        contexto = self.session_manager.get_context(chat_id)
                        phone_id = contexto.get("phone_id")

                        agendar_retentativa_automatica(
                            chat_id=chat_id,
                            phone_id=phone_id,
                            data_admissao_str=admissao_1,
                            meses_alvo=3
                        )
                    
                    # 4º PRIORIDADE: Empresa muito nova
                    elif meses_empresa_1 < 24:
                        status_falha = AnalysisStatus.EMPRESA_RECENTE
                        chave_mensagem = "clt_recusa_definitiva"
                        texto_conflito = f"❌ *Todos os bancos* exigem no mínimo 24 meses de CNPJ ativo (Cliente tem apenas {meses_empresa_1} meses)."
                    
                    # 5º PRIORIDADE: O "Limbo"
                    else:
                        status_falha = AnalysisStatus.CELETISTA_RESTRICAO
                        chave_mensagem = "clt_recusa_definitiva"
                        motivos = []

                        if margem_1 < 50.00:
                            motivos.append(f"❌ *Margem mínima de R$ 50,00 não atingida. (Cliente tem R$ {formatar_moeda(v_margem)}.")
                        if meses_empresa_1 < 36:
                            motivos.append(f"❌ *V8 / Mercantil:* Exigem mín. de 36 meses de empresa (Tem {meses_empresa_1}m).")
                        if idade_principal < 21 or idade_principal > 60:
                            motivos.append(f"❌ *C6 Bank:* Idade ({idade_principal} anos) fora da política (Aceita 21 a 60).")

                        texto_conflito = "\n".join(motivos) if motivos else "❌ Perfil incompatível com a matriz atual."

                    # Anexa o super relatório mesmo se deu recusa total!
                    resultado_raw["msg_tecnica"] = (
                        f"⚠️ *Análise de Restrição Cruzada*\n"
                        f"{texto_todas_matriculas}{texto_conclusao_v8}\n\n"
                        f"🔍 *Motivo da recusa global:*\n"
                        f"{texto_conflito}"
                    )

                    return CreditOffer(
                        status=status_falha,
                        message_key=chave_mensagem,
                        variables=variaveis_mensagem,
                        raw_details=resultado_raw
                    )

            if motivo == "TELEFONE_VINCULADO_OUTRO_CPF":
                return CreditOffer(
                    status=AnalysisStatus.TELEFONE_VINCULADO_OUTRO_CPF,
                    message_key="clt_telefone_ja_vinculado",
                    is_internal=True,
                    raw_details=resultado_raw
                )
            
            if motivo == "CPF_NAO_ENCONTRADO_NA_BASE":
                return CreditOffer(
                    status=AnalysisStatus.CPF_NAO_ENCONTRADO_NA_BASE,
                    message_key="clt_recusa_definitiva",
                    raw_details=resultado_raw
                )
            
            if motivo == "NAO_ELEGIVEL":
                return CreditOffer(
                    status=AnalysisStatus.NAO_ELEGIVEL,
                    message_key="clt_recusa_definitiva",
                    raw_details=resultado_raw
                )
            
            if motivo == "EMPREGADOR_CPF":
                return CreditOffer(
                    status=AnalysisStatus.EMPREGADOR_CPF,
                    message_key="empregador_cpf",
                    raw_details=resultado_raw
                )
            
            if motivo == "SEM_MARGEM":
                return CreditOffer(
                    status=AnalysisStatus.SEM_MARGEM,
                    message_key="sem_margem_cliente",
                    raw_details=resultado_raw
                )
            
            if motivo == "CATEGORIA_CNAE_INVALIDA":
                return CreditOffer(
                    status=AnalysisStatus.CATEGORIA_CNAE_INVALIDA,
                    message_key="clt_recusa_definitiva",
                    raw_details=resultado_raw
                )
            
            if motivo == "EMPRESA_RECENTE":
                return CreditOffer(
                    status=AnalysisStatus.EMPRESA_RECENTE,
                    message_key="clt_recusa_definitiva",
                    raw_details=resultado_raw
                )
            
            if motivo == "LIMITE_CONTRATOS":
                qtd = resultado_raw.get("qtd_contratos", 9)
                return CreditOffer(
                    status=AnalysisStatus.LIMITE_CONTRATOS,
                    message_key="clt_limite_contratos",
                    variables={"qtd": str(qtd)},
                    raw_details=resultado_raw
                )
            
            if motivo == "MENOS_SEIS_MESES":
                return CreditOffer(
                    status=AnalysisStatus.MENOS_SEIS_MESES,
                    message_key="menos_seis_meses",
                    raw_details=resultado_raw
                )
            
            if motivo == "VIRADA_FOLHA_CLT":
                return CreditOffer(
                    status=AnalysisStatus.VIRADA_FOLHA,
                    message_key="clt_virada_folha_cliente",
                    raw_details=resultado_raw
                )
            
            if motivo in ["ERRO_TECNICO", "ERRO_API", "TIMEOUT_FILA", "ERRO_RECALCULO", "TERMO_EXPIRADO", "ERRO_TERMO"]:
                return CreditOffer(
                    status=AnalysisStatus.ERRO_TECNICO,
                    message_key="retorno_desconhecido",
                    is_internal=True,
                    variables={
                    "erro": msg_tecnica
                    },
                    raw_details=resultado_raw
                )
            
            if motivo == "PROCESSAMENTO_PENDENTE":
                return CreditOffer(
                    status=AnalysisStatus.PROCESSAMENTO_PENDENTE,
                    message_key="blank",
                    variables={"blank": msg_tecnica},
                    is_internal=True,
                    raw_details=resultado_raw
                )
            
            return CreditOffer(
                    status=AnalysisStatus.RETORNO_DESCONHECIDO,
                    message_key="retorno_desconhecido",
                    is_internal=True,
                    variables={
                    "erro": msg_tecnica
                    },
                    raw_details=resultado_raw
                ) 