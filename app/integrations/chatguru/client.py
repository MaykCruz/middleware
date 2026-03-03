import os
import httpx
import logging
from app.utils.retry_transport import RetryTransport

logger = logging.getLogger(__name__)

_global_chatguru_client = None

def get_chatguru_client():
    """Singleton do Client HTTP ChatGuru."""
    global _global_chatguru_client
    if _global_chatguru_client is None or _global_chatguru_client.is_closed:
        transport = RetryTransport(max_retries=3, backoff_factor=1.0, retry_status_codes=[500, 502, 503, 504])
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=50, keepalive_expiry=10.0)
        _global_chatguru_client = httpx.Client(timeout=30.0, transport=transport, limits=limits)
    return _global_chatguru_client

class ChatGuruClient:
    def __init__(self):
        self.api_url = os.getenv("CHATGURU_API_URL")
        self.api_key = os.getenv("CHATGURU_API_KEY")
        self.account_id = os.getenv("CHATGURU_ACCOUNT_ID")
        self.phone_id = os.getenv("CHATGURU_PHONE_ID")

        self.http_client = get_chatguru_client()
    
    def _request(self, action: str, chat_number: str, extra_data: dict = None):
        """Método base para fazer chamadas POST no formato x-www-form-urlencoded"""
        if extra_data is None:
            extra_data = {}
        
        payload = {
            "key": self.api_key,
            "account_id": self.account_id,
            "phone_id": self.phone_id,
            "action": action,
            "chat_number": chat_number
        }
        payload.update(extra_data)

        try:
            response = self.http_client.post(self.api_url, data=payload)

            if response.status_code >= 400:
                logger.error(f"❌ [ChatGuru API] Detalhes do erro: {response.text}")

            response.raise_for_status()

            resp_data = response.json()
            logger.debug(f"📥 [ChatGuru API Response]: {resp_data}")
            return resp_data
            
        except Exception as e:
            logger.error(f"❌ [ChatGuru API] Erro na Action '{action}': {e}")
            raise e
        
    def send_message(self, chat_number: str, text: str):
        return self._request("message_send", chat_number, {"text": text})
    
    def add_note(self, chat_number: str, note_text: str):
        return self._request("note_add", chat_number, {"note_text": note_text})
    
    def execute_dialog(self, chat_number: str, dialog_id: str):
        """Usa um 'Dialogo' do ChatGuru para transferir fila, mover de etapa, etc."""
        return self._request("dialog_execute", chat_number, {"dialog_id": dialog_id})
    
    def update_context(self, chat_number: str, contexts: dict):
        """
        Atualiza uma única variável de contexto (campo customizado) no ChatGuru.
        O Python adiciona o prefixo 'var__' automaticamente.

        Exemplo (1 item): 
            update_context(chat, {"URA": "vendas"})
        Exemplo (Vários itens): 
            update_context(chat, {"valor_aprovado": "1500", "parcela": "50"})
        """
        extra_data = {f"var__{key}": str(value) for key, value in contexts.items()}
        return self._request("chat_update_context", chat_number, extra_data)
    
    def update_custom_fields(self, chat_number: str, fields: dict):
        """
        Atualiza um ou múltiplos campos personalizados (CRM) no ChatGuru simultaneamente.
        O Python adiciona o prefixo 'field__' automaticamente para cada chave.
        
        Exemplo (1 item): 
            update_custom_fields(chat, {"cpf": "123.456.789-00"})
        Exemplo (Vários itens): 
            update_custom_fields(chat, {"valor_liberado": "1500", "parcela": "50"})
        """
        extra_data = {f"field__{key}": str(value) for key, value in fields.items()}
        
        return self._request("chat_update_custom_fields", chat_number, extra_data)