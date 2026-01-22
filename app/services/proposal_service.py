import logging
import re
from typing import Dict, Any, Optional
from datetime import datetime

from app.integrations.facta.proposal.service import FactaProposalService
from app.integrations.facta.complementares.funcoes_complementares import FactaDadosCadastrais
from app.services.data_manager import DataManager
from app.services.bot.memory.session import SessionManager

logger = logging.getLogger(__name__)

class ProposalService:
    def __init__(self):
        self.facta_service = FactaProposalService()
        self.facta_dados = FactaDadosCadastrais()
        self.data_manager = DataManager()
        self.session_manager = SessionManager()
    
    def _formatar_data_br(self, data_iso: str) -> str:
        """Converte AAAA-MM-DD para DD/MM/AAAA"""
        if not data_iso: return ""
        try:
            data_iso = data_iso.split(" ")[0]
            d = datetime.strptime(data_iso, "%Y-%m-%d")
            return d.strftime("%d/%m/%Y")
        except ValueError:
            return data_iso
    
    def _limpar_numeros(self, texto: str) -> str:
        """Remove tudo que não é digito"""
        if not texto: return ""
        return re.sub(r'\D', '', str(texto))
    
    # def _extrair_id_facta(self, texto_hibrido)


    def iniciar_proposta_fgts(self, cpf: str, data_nascimento: str, id_simulacao_fgts: int, login_certificado: str) -> int:
        """Inicia FGTS e retorna ID Simulador"""
        try:
            logger.info(f"🆕 [Proposal] Iniciando proposta FGTS para CPF {cpf}")
            dados_etapa1 = {
                "cpf": cpf,
                "data_nascimento": data_nascimento,
                "login_certificado": login_certificado,
                "simulacao_fgts": id_simulacao_fgts
            }
            id_simulador = self.facta_service.registrar_simulacao_fgts(dados_etapa1)
            return id_simulador
        except Exception as e:
            logger.error(f"❌ [Proposal] Erro ao iniciar FGTS: {e}")
            raise e
    
    def iniciar_proposta_clt(self, cpf: str, data_nascimento: str, login_certificado: str, detalhes_simulacao: Dict[str, Any]) -> int:
        """Inicia CLT e retorna ID Simulador"""
        try:
            logger.info(f"🆕 [Proposal] Iniciando proposta CLT para CPF {cpf}")
            dados_etapa1 = {
                "cpf": cpf,
                "data_nascimento": data_nascimento,
                "login_certificado": login_certificado,
                **detalhes_simulacao
            }
            id_simulador = self.facta_service.registrar_simulacao_clt(dados_etapa1)
            return id_simulador
        except Exception as e:
            logger.error(f"❌ [Proposal] Erro ao iniciar CLT: {e}")
            raise e
    
    def processar_dados_pessoais(self, tipo_produto: str, dados_coletados: Dict[str, Any]) -> int:
        """Enriquece dados (Cidades) e envia Etapa 2"""
        pass