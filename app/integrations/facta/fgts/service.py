import logging
import httpx
from typing import Dict, Any
from app.integrations.facta.fgts.client import FactaFGTSAdapter

logger = logging.getLogger(__name__)

class FactaFGTSService:
    def __init__(self, http_client: httpx.Client):
        self.adapter = FactaFGTSAdapter(http_client)

    def simular_antecipacao(self, cpf: str) -> Dict[str, Any]:
        """
        Orquestra o fluxo:
        1. Consulta Saldo
        2. Mapeia erros de negócio (Auth, Adesão, etc)
        3. Se OK, chama Simulação de Cálculo
        4. Retorna resposta final padronizada.
        """

        resp_saldo = self.adapter.consultar_saldo(cpf)

        status_saldo = resp_saldo.get("status")
        msg_original = resp_saldo.get("msg_original", "")

        logger.info(f"Status Saldo para {cpf}: {status_saldo}")

        if status_saldo != "SUCESSO":
            return {
                "aprovado": False,
                "motivo": status_saldo,
                "msg_tecnica": msg_original
            }
        
        dados_saldo = resp_saldo.get("dados", {})

        resp_calculo = self.adapter.simular_calculo(cpf, dados_saldo)

        if resp_calculo.get("status") == "APROVADO":
            return {
                "aprovado": True,
                "motivo": "VALOR_DISPONÍVEL",
                "detalhes": {
                    "valor_liquido": resp_calculo.get("valor_liquido"),
                    "taxa": resp_calculo.get("raw", {}).get("taxa"),
                    "tabela": resp_calculo.get("raw", {}).get("tabela"),
                    "simulacao_id": resp_calculo.get("raw", {}).get("simulacao_fgts")
                }
            }
        
        if resp_calculo.get("status") == "REPROVADO":
            return {
                "aprovado": False,
                "motivo": "SEM_SALDO",
                "msg_tecnica": resp_calculo.get("msg_original")
            }

        return {
            "aprovado": False,
            "motivo": "ERRO_TECNICO",
            "msg_tecnica": resp_calculo.get("msg_original")
        }