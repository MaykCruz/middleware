import logging
from typing import Dict, Any

from app.integrations.facta.proposal.client import FactaProposalClient
from app.integrations.facta.proposal.schemas import (
    ProposalStep1FGTS,
    ProposalStep1CLT,
    ProposalStep2Base,
    ProposalStep2CLT
)

logger = logging.getLogger(__name__)

class FactaProposalService:
    """
    Serviço especialista em digitação Facta.
    Responsável por validar os dados (Schemas) e orquestrar as chamadas ao Client.
    """
    def __init__(self):
        self.client = FactaProposalClient()

    def registrar_simulacao_fgts(self, dados: Dict[str, Any]) -> int:
        """
        Etapa 1 FGTS: Cria o simulador vinulado ao cálculo.
        Retorna: id_simulador
        """
        try:
            logger.info("🚀 [Facta Service] Iniciando Digitação FGTS - Etapa 1")

            payload = ProposalStep1FGTS(**dados)

            id_simulador = self.client.registrar_etapa_1_simulacao(payload.model_dump())

            return id_simulador

        except Exception as e:
            logger.error(f"❌ [Facta Service] Erro step 1 FGTS: {e}")
            raise e
    
    def registrar_dados_pessoais_fgts(self, dados: Dict[str, Any]) -> int:
        """
        Etapa 2 FGTS: Envia dados pessoais.
        Retorna: codigo_cliente
        """
        try:
            logger.info(f"👤 [Facta Service] Enviando Dados Pessoais FGTS (Simulador: {dados.get('id_simulador')})")

            payload = ProposalStep2Base(**dados)

            codigo_cliente = self.client.registrar_etapa_2_dados_pessoais(payload.model_dump())

            return codigo_cliente
        
        except Exception as e:
            logger.error(f"❌ [Facta Service] Erro step 2 FGTS: {e}")
            raise e
        
    def registrar_simulacao_clt(self, dados: Dict[str, Any]) -> int:
        """
        Etapa 1 CLT: Cria o simulador com parâmetros de tabela/prazo
        Retorna: id_simulador
        """
        try:
            logger.info("🚀 [Facta Service] Iniciando Digitação CLT - Etapa 1")

            payload = ProposalStep1CLT(**dados)

            id_simulador = self.client.registrar_etapa_1_simulacao(payload.model_dump())

            return id_simulador
        
        except Exception as e:
            logger.error(f"❌ [Facta Service] Erro step 1 CLT: {e}")
            raise e
        
    def registrar_dados_pessoais_clt(self, dados: Dict[str, Any]) -> int:
        """
        Etapa 2 CLT: Envia dados pessoais + matrícula/admissão.
        Retorna: codigo_cliente
        """
        try:
            logger.info(f"👤 [Facta Service] Enviando Dados Pessoais CLT (Simulador: {dados.get('id_simulador')})")

            payload = ProposalStep2CLT(**dados)

            codigo_cliente = self.client.registrar_etapa_2_dados_pessoais(payload.model_dump())

            return codigo_cliente
            
        except Exception as e:
            logger.error(f"❌ [Facta Service] Erro step 2 CLT: {e}")
            raise e
    
    def finalizar_proposta(self, codigo_cliente: int, id_simulador: int) -> Dict[str, Any]:
        """
        Etapa 3: Efetivação.
        Retorna um dicionário com o link para formalização e o código AF.
        Ex: {'codigo': '123456', 'url_formalizacao': 'facta.ly/abc', ...}
        """
        try:
            logger.info(f"🏁 [Facta Service] Finalizando Proposta (Cli: {codigo_cliente}, Sim: {id_simulador})")

            resultado = self.client.registrar_etapa_3_efetivacao(codigo_cliente, id_simulador)

            return resultado
            
        except Exception as e:
            logger.error(f"❌ [Facta Service] Erro ao finalizar proposta: {e}")
            raise e