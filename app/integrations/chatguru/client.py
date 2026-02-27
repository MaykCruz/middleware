import os
import httpx
import logging
from app.utils.retry_transport import RetryTransport

logger = logging.getLogger(__name__)

class ChatGuruClient:
    def __init__(self):
        self.api_url = os.getenv("CHATGURU_API_URL")
        self.api_key = os.getenv("CHATGURU_API_KEY")
        self.account_id = os.getenv("CHATGURU_ACCOUNT_ID")
        self.phone_id = os.getenv("CHATGURU_PHONE_ID")

    def _get_http_client(self):
        """Reutiliza o nosso RetryTransport para blindar o ChatGuru contra erros 502/504"""
        transport = RetryTransport(max_retries=3, backoff_factor=1.0, retry_status_codes=[500, 502, 503, 504])
        return httpx.Client(timeout=30.0, transport=transport)
    
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
            with self._get_http_client() as client:
                response = client.post(self.api_url, data=payload)

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