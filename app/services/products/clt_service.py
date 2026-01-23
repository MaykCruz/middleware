import logging
from app.integrations.facta.clt.service import FactaCLTService
from app.integrations.facta.complementares.funcoes_complementares import FactaDadosCadastrais
from app.schemas.credit import CreditOffer, AnalysisStatus
from app.services.bot.memory.session import SessionManager
from app.utils.formatters import formatar_moeda, obter_mes_inicio_desconto, formatar_display_tempo, calcular_meses

logger = logging.getLogger(__name__)

class CLTService:
    """
    Service Global de CLT.
    Responsável por consultar múltiplos parceiros (Facta, etc.) e agregar/comparar os resultados.
    """
    def __init__(self):
        self.facta_service = FactaCLTService()
        self.dados_cadastrais = FactaDadosCadastrais()
        self.session_manager = SessionManager()
        
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

            info_conta = self.dados_cadastrais.buscar_conta_bancaria(cpf)

            if chat_id:
                detalhes_oferta = {
                    "codigo_tabela": oferta_dados.get("codigo_tabela"),
                    "prazo": oferta_dados.get("prazo"),
                    "valor_operacao": oferta_dados.get("valor_liquido"),
                    "valor_parcela": oferta_dados.get("parcela"),
                    "matricula": trabalhador.get("matricula"),
                    "data_admissao": trabalhador.get("dataAdmissao"),
                    "cnpj_empregador": trabalhador.get("numeroInscricaoEmpregador"),
                    "dados_bancarios": None
                }

                if info_conta:
                    raw_banco = info_conta.get("raw", {})
                    detalhes_oferta["dados_bancarios"] = {
                        "banco": raw_banco.get("BANCO"),
                        "agencia": raw_banco.get("AGENCIA"),
                        "conta": raw_banco.get("CONTA"),
                        "tipo_conta": raw_banco.get("TIPO_CONTA")
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
                message_key="clt_termo_nao_identificado",
                raw_details=resultado_raw
            )
        
        if not aprovado:
            if motivo == "TELEFONE_VINCULADO_OUTRO_CPF":
                return CreditOffer(
                    status=AnalysisStatus.TELEFONE_VINCULADO_OUTRO_CPF,
                    message_key="clt_telefone_ja_vinculado",
                    is_internal=True,
                    raw_details=resultado_raw
                )
            
            if motivo == "ERRO_TERMO":
                return CreditOffer(
                    status=AnalysisStatus.RETORNO_DESCONHECIDO,
                    message_key="retorno_desconhecido",
                    is_internal=True,
                    variables={
                        "erro": msg_tecnica
                    },
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
        
            if motivo == "IDADE_INSUFICIENTE_FACTA":
                idade = int(resultado_raw.get("idade", 0))
                sexo = resultado_raw.get("sexo", "")
                margem = float(resultado_raw.get("margem_disponivel", 0.0))
                admissao = resultado_raw.get("data_admissao")
                meses_casa = calcular_meses(admissao)

                margem_minima_distribuir = 50.00

                if margem <= margem_minima_distribuir:
                    return CreditOffer(
                        status=AnalysisStatus.SEM_MARGEM,
                        message_key="sem_margem_cliente",
                        raw_details={
                            **resultado_raw,
                            "msg_tecnica": f"Idade não enquadra Facta e margem baixa ({formatar_moeda(margem)}) para distribuir."
                        }
                    )

                sugestoes = []

                # HUB: 18 a 50
                if 18 <= idade <=50:
                    sugestoes.append("HUB (18-50)")
                
                # Mercantil: 20 a 58
                if 20 <= idade <= 58 and meses_casa >= 12:
                    sugestoes.append("Mercantil (20-58)")
                
                # C6: 21 a 60
                if 21 <= idade <= 60:
                    sugestoes.append("C6 (21-60)")
                
                # V8/Presença: 21 a 65
                if 21 <= idade <= 65:
                    sugestoes.append("V8/Presença (21-65)")
                
                if sugestoes:
                    texto_sugestao = ", ".join(sugestoes)

                    msg_final = (
                        f"Cliente: {sexo}, {idade} anos\n"
                        f"💰 Margem: R$ {formatar_moeda(margem)}\n"
                        f"📅 Admissão: {formatar_display_tempo(admissao)}\n"
                        f"🏦 Tente em: {texto_sugestao}"
                    )

                    return CreditOffer(
                        status=AnalysisStatus.IDADE_INSUFICIENTE_FACTA,
                        message_key="clt_nao_elegivel",
                        raw_details={**resultado_raw, "sugestao_bancos": msg_final}
                    )
                else:
                    return CreditOffer(
                        status=AnalysisStatus.IDADE_INSUFICIENTE,
                        message_key="clt_recusa_definitiva",
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
            
            if motivo == "REPROVADO_POLITICA_FACTA":
                return CreditOffer(
                    status=AnalysisStatus.REPROVADO_POLITICA_FACTA,
                    message_key="clt_nao_elegivel",
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
            
            if motivo in ["SEM_OPERACOES", "SEM_PRAZO_COMPATIVEL"]:
                return CreditOffer(
                    status=AnalysisStatus.SEM_OFERTA,
                    message_key="clt_nao_elegivel",
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