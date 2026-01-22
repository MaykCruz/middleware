import logging
from app.integrations.facta.fgts.service import FactaFGTSService
from app.integrations.facta.complementares.funcoes_complementares import FactaDadosCadastrais
from app.schemas.credit import CreditOffer, AnalysisStatus
from app.services.bot.memory.session import SessionManager
from app.utils.formatters import formatar_moeda
from app.utils.validators import calcular_segundo_dia_util_prox_mes

logger = logging.getLogger(__name__)

class FGTSService:
    """
    Service Global de FGTS.
    Responsável por consultar múltiplos parceiros (Facta, etc.) e agregar/comparar os resultados.
    """
    def __init__(self):
        self.facta_service = FactaFGTSService()
        self.dados_cadastrais = FactaDadosCadastrais()
        self.session_manager = SessionManager()

    def consultar_melhor_oportunidade(self, cpf: str, chat_id: str) -> CreditOffer:
        """
        Executa a lógica de prioridade (Waterfall).
        Atualmente: Chama Facta.
        Futuramente: Se Facta falhar ou não for vantajoso, chama Próximo.
        """
        logger.info(f"🌐 [Global FGTS] Buscando oportunidade para CPF: {cpf}")

        resultado_raw = self.facta_service.simular_antecipacao(cpf)

        if resultado_raw.get("aprovado"):
            val_liquido = resultado_raw["detalhes"]["valor_liquido"]
            valor_fmt = formatar_moeda(val_liquido)

            info_conta = self.dados_cadastrais.buscar_conta_bancaria(cpf)

            if info_conta:
                return CreditOffer(
                    status=AnalysisStatus.APROVADO,
                    message_key="com_saldo_conta",
                    valor_liquido=val_liquido,
                    variables={
                        "valor": valor_fmt,
                        "dados_bancarios": info_conta["texto_formatado"]
                    },
                    banco_origem="Facta",
                    raw_details=resultado_raw
                )

            else:
                return CreditOffer(
                    status=AnalysisStatus.APROVADO,
                    message_key="com_saldo",
                    banco_origem="Facta",
                    valor_liquido=resultado_raw["detalhes"]["valor_liquido"],
                    variables={
                        "valor": formatar_moeda(resultado_raw["detalhes"]["valor_liquido"]),
                        "banco": "Facta"
                    },
                    raw_details=resultado_raw
                )
        
        motivo = resultado_raw.get("motivo")

        if motivo == "PROCESSAMENTO_PENDENTE":
            msg_tecnica = resultado_raw.get("msg_tecnica", "Instabilidade momentânea API Facta.")
            return CreditOffer(
                status=AnalysisStatus.PROCESSAMENTO_PENDENTE,
                message_key="blank",
                variables={"blank": msg_tecnica},
                is_internal=True,
                raw_details=resultado_raw
            )
    
        if motivo in ["SEM_AUT", "SEM_AUTORIZACAO"]:
            return CreditOffer(
                status=AnalysisStatus.SEM_AUTORIZACAO,
                message_key="sem_autorizacao",
                raw_details=resultado_raw
            )
        
        if motivo == "SEM_ADESAO":
            return CreditOffer(
                status=AnalysisStatus.SEM_ADESAO,
                message_key="sem_adesao",
                raw_details=resultado_raw
            )
        
        if motivo == "MUDANCAS_CADASTRAIS":
            return CreditOffer(
                status=AnalysisStatus.MUDANCAS_CADASTRAIS,
                message_key="mudancas_cadastrais",
                raw_details=resultado_raw
            )
        
        if motivo == "ANIVERSARIANTE":

            data = calcular_segundo_dia_util_prox_mes()

            return CreditOffer(
                status=AnalysisStatus.ANIVERSARIANTE,
                message_key="aniversariante",
                variables={
                    "data": data
                },
                raw_details=resultado_raw
            )
        
        if motivo == "SALDO_NAO_ENCONTRADO":
            return CreditOffer(
                status=AnalysisStatus.SALDO_NAO_ENCONTRADO,
                message_key="saldo_nao_encontrado",
                raw_details=resultado_raw
            )
        
        if motivo == "SEM_SALDO":
            return CreditOffer(
                status=AnalysisStatus.SEM_SALDO,
                message_key="sem_saldo",
                raw_details=resultado_raw
            )
        
        if motivo == "LIMITE_EXCEDIDO_CONSULTAS_FGTS":
            return CreditOffer(
                status=AnalysisStatus.LIMITE_EXCEDIDO_CONSULTAS_FGTS,
                message_key="limite_excedido_fgts",
                is_internal=True,
                raw_details=resultado_raw
            )
        
        msg_tecnica = resultado_raw.get("msg_tecnica", str(motivo))

        return CreditOffer(
                status=AnalysisStatus.RETORNO_DESCONHECIDO,
                message_key="retorno_desconhecido",
                is_internal=True,
                variables={
                    "erro": msg_tecnica
                },
                raw_details=resultado_raw
            )