import httpx
import os
import logging
from typing import Union, Dict, Any, Optional
from app.services.bot.content.message_loader import MessageLoader

logger = logging.getLogger(__name__)

class HuggyClient:
    API_VALUE_EXIT_WORKFLOW = ""

    def __init__(self):
        self.api_token = os.getenv("HUGGY_API_TOKEN")
        self.base_url = "https://api.huggy.app/v3/companies/351946"

        if not self.api_token:
            logger.warning("⚠️ HUGGY_API_TOKEN não configurado. As chamadas à API falharão.")

    def _get_headers(self):
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    def send_message(self, chat_id: int, message_key: str, variables: Dict[str, Any] = None, file_url: Optional[str] = None, force_internal: bool = False) -> bool:
        """
        Envia uma mensagem completa (Texto, Arquivo, Botões, Interna).

        Args:
            chat_id: ID do chat.
            message_key: Chave no messages.json.
            variables: Dict para formatar o texto (ex: {'nome': 'João'}).
            file_url: URL de mídia (sobrescreve o do JSON se existir).
            force_internal: Se True, força a mensagem a ser interna.
        """
        template = MessageLoader.get(message_key)
        if not template and not message_key.startswith("DYNAMIC"):
            logger.error(f"❌ Template '{message_key}' não encontrado.")
            return False
        
        raw_text = template.get("text", "")
        final_text = raw_text

        if variables and raw_text:
            try:
                final_text = raw_text.format(**variables)
            except KeyError as e:
                logger.error(f"⚠️ Falta variável {e} para mensagem '{message_key}'")
                final_text = raw_text
        
        payload = {
            "text": final_text
        }

        if "options" in template:
            payload["options"] = template["options"]

        payload_file = file_url if file_url else template.get("file")
        if payload_file:
            payload["file"] = payload_file

        is_internal = force_internal or template.get("isInternal", False)
        payload["isInternal"] = is_internal

        url = f"{self.base_url}/chats/{chat_id}/messages"

        try:
            with httpx.Client(timeout=60.0) as client:
                response = client.post(url, headers=self._get_headers(), json=payload)
                response.raise_for_status()
                
                # Log rico para debug
                log_extras = []
                if "file" in payload: log_extras.append("📎 Com Arquivo")
                if "options" in payload: log_extras.append("🔘 Com Botões")
                if is_internal: log_extras.append("🔒 Interna")
                
                logger.info(f"📤 [Huggy] Msg '{message_key}' enviada. {' | '.join(log_extras)}")
                return True

        except httpx.HTTPStatusError as e:
            logger.error(f"❌ Erro HTTP Huggy no Chat {chat_id} ({e.response.status_code}): {e.response.text}")
            raise e
        except Exception as e:
            logger.error(f"❌ Erro conexão Huggy no Chat {chat_id}: {str(e)}")
            raise e
    
    def trigger_flow(self, chat_id: int, flow_id: int, variables: Dict[str, Any] = None) -> bool:
        """
        Dispara um Flow específico para o chat (POST /chats/{id}/flow).
        """
        url = f"{self.base_url}/chats/{chat_id}/flow"

        payload = {
            "flowId": flow_id
        }

        if variables:
            payload["variables"] = variables

        try:
            with httpx.Client(timeout=60.0) as client:
                response = client.post(url, headers=self._get_headers(), json=payload)
                
                # 200 OK - Sucesso (Body vazio)
                if response.status_code == 200:
                    logger.info(f"⚡ [Huggy] Flow {flow_id} disparado para Chat {chat_id}.")
                    return True
                
                # 404/400 - Erros comuns
                elif response.status_code in [400, 404]:
                    logger.warning(f"⚠️ [Huggy] Falha ao disparar Flow {flow_id} (Chat {chat_id}): {response.text}")
                    return False
                
                else:
                    response.raise_for_status() # Lança erro para 5xx
                    return False # Nunca chega aqui, mas agrada o linter

        except httpx.HTTPStatusError as e:
            logger.error(f"❌ Erro HTTP Huggy ao disparar flow no Chat {chat_id}: {e.response.text}")
            return False # Aqui retornamos False para o Engine decidir o que fazer (ex: tentar outro método)
        except Exception as e:
            logger.error(f"❌ Erro conexão Huggy no Chat {chat_id}: {str(e)}")
            return False

    def update_workflow_step(self, chat_id: int, step_id: Union[int, str]) -> bool:
        """
        Método GENÉRICO (Base).
        Executa a chamada HTTP pura.
        """
        url = f"{self.base_url}/chats/{chat_id}/workflow"
        payload = {"stepId": step_id}

        if step_id == self.API_VALUE_EXIT_WORKFLOW:
            action_name = "REMOVER do workflow"
        else:
            action_name = f"mover para etapa {step_id}"
        
        try:
            with httpx.Client(timeout=60.0) as client:
                response = client.put(url, headers=self._get_headers(), json=payload)

                if response.status_code == 200:
                    logger.info(f"✅ [Huggy] Sucesso ao {action_name} (Chat {chat_id}).")
                    return True
                elif response.status_code == 404:
                    logger.warning(f"❌ [Huggy] Falha ao {action_name} (Chat {chat_id}): {response.status_code} - Etapa informada não existe.")
                else:
                    logger.error(f"❌ [Huggy] Falha ao {action_name} (Chat {chat_id}): {response.status_code} - {response.text}")
                    return False
        except Exception as e:
            logger.error(f"❌ Erro de conexão Huggy ao {action_name} (Chat {chat_id}): {str(e)}")
            return False
    
    def close_chat(self, chat_id: int, tabulation_id: Union[int, str] = None, comment: str = None, send_feedback: bool = False) -> bool:
        """
        Método Base: Fecha o chat.
        Nota: tabulation_id agora é tratado como obrigatório pela regra de negócio, 
        embora tecnicamente a função aceite, vamos forçar o uso correto.
        """
        url = f"{self.base_url}/chats/{chat_id}/close"

        payload = {
            "sendFeedback": send_feedback,
            "tabulation": str(tabulation_id)
        }

        if comment:
            payload["comment"] = comment
        
        try:
            with httpx.Client(timeout=60.0) as client:
                response = client.put(url, headers=self._get_headers(), json=payload)
                
                if response.status_code == 200:
                    logger.info(f"checkered_flag [Huggy] Chat {chat_id} fechado com sucesso.")
                    return True
                elif response.status_code == 404:
                    logger.warning(f"⚠️ [Huggy] Tentativa de fechar chat {chat_id} que não existe (404).")
                    return False
                else:
                    logger.error(f"❌ [Huggy] Falha ao fechar chat {chat_id}: {response.status_code} - {response.text}")
                    return False

        except Exception as e:
            logger.error(f"❌ Erro conexão Huggy ao fechar chat: {str(e)}")
            return False
    
    def transfer_chat(self, chat_id: int, agent_id: int, message: str = None) -> bool:
        """
        Transfere o chat para um ID de agente específico.
        """
        url = f"{self.base_url}/chats/{chat_id}/transfer"

        payload = {
            "agentId": int(agent_id)
        }

        if message:
            payload["message"] = message
        
        try:
            with httpx.Client(timeout=60.0) as client:
                response = client.post(url, headers=self._get_headers(), json=payload)
                
                if response.status_code == 200:
                    logger.info(f"✅ [Huggy] Chat {chat_id} transferido para Agente {agent_id}.")
                    return True
                
                logger.error(f"❌ [Huggy] Falha transf. Chat {chat_id}: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ [Huggy] Erro conexão transfer_chat: {str(e)}")
            return False
    