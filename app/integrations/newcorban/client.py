import time
import httpx
import logging
import os
import re
from app.infrastructure.token_manager import TokenManager
from app.utils.retry_transport import RetryTransport
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

_global_newcorban_client = None

def get_newcorban_client():
    """Singleton do Cliente HTTP NewCorban."""
    global _global_newcorban_client
    if _global_newcorban_client is None or _global_newcorban_client.is_closed:
        transport = RetryTransport(
            max_retries=3,
            backoff_factor=1.5,
            retry_status_codes=[429, 500, 502, 503, 504]
        )
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=50, keepalive_expiry=10.0)
        _global_newcorban_client = httpx.Client(timeout=30.0, transport=transport, limits=limits)
    return _global_newcorban_client

class NewCorbanClient:
    """
    Responsável exclusivamente pela comunicação HTTP com a API NewCorban.
    Lida com autenticação, headers e execução das requisições.
    """
    def __init__(self):
        self.url_base_proposta = "https://api.newcorban.com.br/api"
        self.url_base_sistema = "https://server.newcorban.com.br"

        self.user = os.getenv('NEW_USER')
        self.password = os.getenv('NEW_PASSWORD')
        self.empresa = os.getenv('NEW_EMPRESA')

        self.server_user = os.getenv('NEW_SERVER_USER')
        self.server_pass = os.getenv('NEW_SERVER_PASSWORD')
        self.server_empresa = os.getenv('NEW_EMPRESA')

        self.token_manager = TokenManager()
        self.SCOPE = "NEWCORBAN_INTERNAL"

        self.headers_browser = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'origin': 'https://empreste.newcorban.com.br',
            'referer': 'https://empreste.newcorban.com.br/',
            'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
        }

        self.http_client = get_newcorban_client()
    
    def get_bank_account_history(self, cpf: str) -> Optional[List[Dict[str, Any]]]:
        """
        Busca o histórico de contas bancárias na API interna.
        Retorna a lista bruta de históricos ou None em caso de erro.
        """
        token = self._authenticate_internal()
        if not token:
            return None
        
        cpf_limpo = re.sub(r'\D', '', cpf)
        url = f"{self.url_base_sistema}/system/cliente.php"

        params = {
            "action": "getBankAccountHistory",
            "cpf": cpf_limpo
        }

        headers = self.headers_browser.copy()
        headers['Authorization'] = f"Bearer {token}"

        try:
            response = self.http_client.get(url, params=params, headers=headers)
            
            if response.status_code == 200:
                try:
                    return response.json()
                except Exception:
                    logger.error(f"❌ [NewCorban Client] Erro ao decodificar JSON de histórico.")
                    return None
            else:
                logger.warning(f"⚠️ [NewCorban Client] Erro HTTP {response.status_code} ao buscar histórico.")
                return None
        except Exception as e:
            logger.error(f"❌ [NewCorban Client] Erro de conexão (Histórico): {e}")
            return None
    
    def create_proposal(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Envia uma proposta para a API de parceiros.
        """
        url = f"{self.url_base_proposta}/propostas/"
        
        try:
            response = self.http_client.post(url, json=payload)
            
            # Retorna um dict padronizado com status e dados
            return {
                "status_code": response.status_code,
                "response_text": response.text,
                "success": response.status_code in [200, 201]
            }
        except Exception as e:
            logger.error(f"❌ [NewCorban Client] Erro crítico ao criar proposta: {e}")
            return {"success": False, "error": str(e)}

    def _authenticate_internal(self) -> Optional[str]:
        """
        Retorna um token Bearer válido para o NewCorban.
        Padronizado com a lógica de Cache e Lock da Facta.
        """
        max_tentativas = 3

        for tentativa in range(1, max_tentativas + 1):
            token = self.token_manager.get_token(self.SCOPE)
            if token:
                return token
            
            if self.token_manager.acquire_lock(self.SCOPE):
                try:
                    logger.info(f"🔑 [NewCorban] Tentativa {tentativa}/{max_tentativas}: Iniciando login...")
                    new_token = self._request_new_token()

                    if new_token:
                        self.token_manager.save_token(self.SCOPE, new_token, 72000)
                        return new_token
                    
                    self.token_manager.release_lock(self.SCOPE)
                    time.sleep(2)
                except Exception as e:
                    self.token_manager.release_lock(self.SCOPE)
                    logger.error(f"❌ [NewCorban] Falha na renovação: {e}")

                    if tentativa == max_tentativas:
                        raise e
                    time.sleep(2)
            
            else:
                logger.info(f"⏳ [NewCorban] Aguardando renovação por outro worker...")
                time.sleep(2)

        raise TimeoutError("Não foi possível obter token do NewCorban.")

    def _request_new_token(self) -> str:
        """Chamada HTTP real de login"""
        url = f"{self.url_base_sistema}/api/v2/login"

        payload = {
            "usuario": self.server_user,
            "empresa": self.server_empresa,
            "ip": "127.0.0.1",
            "senha": self.server_pass,
            "p": "facta"
        }

        headers = self.headers_browser.copy()
        headers['content-type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
        
        response = self.http_client.post(url, data=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data.get("token")
        else:
            logger.error(f"❌ [NewCorban Auth] Falha no login: {response.status_code}")
            return None