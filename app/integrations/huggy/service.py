import os
import logging
from typing import Union, Dict, Any, Optional
from app.integrations.huggy.client import HuggyClient

logger = logging.getLogger(__name__)

class HuggyService:
    """
    Cama de Facade (Fachada) que combina métodos da API para realizar ações de negócio.
    Correção: Delega chamadas de infraestrutura explicitamente para self.client.
    """
    def __init__(self):
        self.client = HuggyClient()

        self.workflow_steps = {
            "WORKFLOW_STEP_AG_FORMALIZAR": os.getenv("HUGGY_WORKFLOW_STEP_AG_FORMALIZAR"),
            "WORKFLOW_STEP_COM_SALDO_FGTS": os.getenv("HUGGY_WORKFLOW_STEP_COM_SALDO_FGTS")
        }

        self.flows = {
            "AUTO_DISTRIBUTION": os.getenv("HUGGY_FLOW_AUTO_DISTRIBUTION"),
            "AUTHORIZATION": os.getenv("HUGGY_FLOW_AUTHORIZATION"),
            "TERM_AUTHORIZATION": os.getenv("HUGGY_FLOW_TERM_AUTHORIZATION")
        }

        self.tabulations = {
            "MENOS_SEIS_MESES": os.getenv("HUGGY_TABULATION_LESS_SIX_MONTHS"),
            "SEM_SALDO": os.getenv("HUGGY_TABULATION_SEM_SALDO"),
            "MUDANCAS_CADASTRAIS": os.getenv("HUGGY_MUDANCADAS_CADASTRAIS"),
            "ANIVERSARIANTE": os.getenv("HUGGY_ANIVERSARIANTE"),
            "SALDO_NAO_ENCONTRADO": os.getenv("HUGGY_SALDO_NAO_ENCONTRADO"),
            "SEM_SALDO": os.getenv("HUGGY_SEM_SALD0"),
            "SEM_INTERESSE": os.getenv("HUGGY_SEM_INTERESSE"),
            "CLT_RECUSA_DEFINITIVA": os.getenv("HUGGY_CLT_RECUSA_DEFINITIVA"),
            "SEM_MARGEM_CLT": os.getenv("HUGGY_TABULATION_SEM_MARGEM_CLT"),
            "CELETISTA_RESTRICAO": os.getenv("HUGGY_CELETISTA_RESTRICAO"),
        }

    def send_message(self, chat_id: int, message_key: str, variables: Dict[str, Any] = None, file_url: Optional[str] = None, force_internal: bool = False) -> bool:
        return self.client.send_message(chat_id, message_key, variables=variables, file_url=file_url, force_internal=force_internal)

    def finish_attendance(self, chat_id: int, tabulation_id: Union[int, str], send_feedback: bool = False) -> bool:
        """
        Smart Wrapper: Tira do Workflow + Fecha com Tabulação.
        """
        if not tabulation_id:
            logger.error(f"❌ Tentativa de fechar Chat {chat_id} sem Tabulação! Abortando.")
            return False
        
        logger.info(f"📉 [SmartClose] Finalizando Chat {chat_id} com Tabulação {tabulation_id}...")

        self.remove_from_workflow(chat_id)

        # CORREÇÃO: Chama close_chat do client
        return self.client.close_chat(chat_id, tabulation_id=tabulation_id, send_feedback=send_feedback)

    def remove_from_workflow(self, chat_id: int) -> bool:
        """Ação: Retirar do workflow"""
        # CORREÇÃO: Usa constante do client e chama método do client
        return self.client.update_workflow_step(chat_id, self.client.API_VALUE_EXIT_WORKFLOW)
    
    def move_to_ag_formalizar(self, chat_id: int) -> bool:
        """Ação: Mover para etapa Aguardando Formalizar"""
        step_id = self.workflow_steps.get("WORKFLOW_STEP_AG_FORMALIZAR")
        if not step_id:
            logger.warning(f"⚠️ Tentativa de mover Chat {chat_id} para AG_FORMALIZAR, mas env var não configurada.")
            return False
        
        # CORREÇÃO: Chama método do client
        return self.client.update_workflow_step(chat_id, step_id)
    
    def move_to_aprovado(self, chat_id: int) -> bool:
        """Ação: Mover para etapa Com saldo FGTS"""
        step_id = self.workflow_steps.get("WORKFLOW_STEP_COM_SALDO_FGTS")
        if not step_id:
            logger.warning(f"⚠️ Tentativa de mover Chat {chat_id} para COM_SALDO_FGTS, mas env var não configurada.")
            return False
        
        # CORREÇÃO: Chama método do client
        return self.client.update_workflow_step(chat_id, step_id)
    
    def start_auto_distribution(self, chat_id: int) -> bool:
        """
        Inicia o fluxo de autodistribuição.
        """
        flow_id = self.flows.get("AUTO_DISTRIBUTION")

        if not flow_id:
            logger.warning("⚠️ HUGGY_FLOW_AUTO_DISTRIBUTION não configurado no .env")
            return False
        
        try:
            return self.client.trigger_flow(chat_id, int(flow_id))
        except ValueError:
            logger.error(f"❌ ID do Flow inválido no .env: {flow_id}")
            return False
    
    def start_flow_authorization(self, chat_id: int) -> bool:
        """
        Inicia o fluxo de autorização pré cadastrado Huggy.
        """
        flow_id = self.flows.get("AUTHORIZATION")

        if not flow_id:
            logger.warning("⚠️ HHUGGY_FLOW_AUTHORIZATION não configurado no .env")
            return False
        
        try:
            return self.client.trigger_flow(chat_id, int(flow_id))
        except ValueError:
            logger.error(f"❌ ID do Flow inválido no .env: {flow_id}")
            return False
    
    def start_flow_wait_term(self, chat_id: int) -> bool:
        """
        Inicia o fluxo de aguardando autorização termo CLT pré cadastrado Huggy.
        """
        flow_id = self.flows.get("TERM_AUTHORIZATION")

        if not flow_id:
            logger.warning("⚠️ HHUGGY_TERM_AUTHORIZATION não configurado no .env")
            return False
        
        try:
            return self.client.trigger_flow(chat_id, int(flow_id))
        except ValueError:
            logger.error(f"❌ ID do Flow inválido no .env: {flow_id}")
            return False