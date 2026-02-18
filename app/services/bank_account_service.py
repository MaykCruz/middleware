import logging
from typing import Optional, Dict, Any
from app.integrations.facta.complementares.funcoes_complementares import FactaDadosCadastrais
from app.integrations.newcorban.service import NewCorbanService

logger = logging.getLogger(__name__)

class BankAccountService:
    """
    Service Especialista em Localização de Contas Bancárias.
    Atua como uma 'Fachada' para múltiplas fontes de dados (Facta, NewCorban, etc).
    """
    def __init__(self):
        self.provider_facta = FactaDadosCadastrais()
        self.provider_newcorban = NewCorbanService()
    
    def buscar_melhor_conta(self, cpf: str) -> Optional[Dict[str, Any]]:
        """
        Executa a estratégia de busca em cascata:
        1. Tenta Facta (Prioridade).
        2. Tenta NewCorban (Fallback).

        Retorna:
        {
            "raw": {
                "BANCO": "...",
                "AGENCIA": "...",
                "CONTA": "...",
                "tipo_dado": "CONTA" | "PIX",  <-- CRUCIAL PARA A DIGITAÇÃO
                "origem": "facta" | "newcorban"
                ...
            },
            "texto_formatado": "..."
        }
        """
        try:
            conta_facta = self.provider_facta.buscar_conta_bancaria(cpf)
            if conta_facta:
                logger.info(f"✅ [BankService] Conta encontrada na Facta para {cpf}")

                if "raw" in conta_facta:
                    conta_facta["raw"]["tipo_dado"] = "CONTA"
                    conta_facta["raw"]["origem"] = "facta"
                
                return conta_facta
        except Exception as e:
            logger.error(f"❌ [BankService] Erro ao consultar Facta: {e}")

        try:
            logger.info(f"⚠️ [BankService] Facta sem dados. Acionando Fallback NewCorban para {cpf}...")
            conta_new = self.provider_newcorban.consultar_conta_fallback(cpf)

            if conta_new:
                tipo_encontrado = conta_new.get("raw", {}).get("tipo_dado", "DESCONHECIDO")

                logger.info(f"✅ [BankService] Dado encontrado no NewCorban: {tipo_encontrado}")

                return conta_new
        
        except Exception as e:
            logger.error(f"❌ [BankService] Erro ao consultar NewCorban: {e}")
        
        logger.info(f"🛑 [BankService] Nenhuma conta/pix encontrada para {cpf}.")
        return None