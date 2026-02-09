import logging
import httpx
import os
from typing import Optional, Dict, Any
from app.infrastructure.token_manager import TokenManager

logger = logging.getLogger(__name__)

class NewCorbanPrivateClient:
    """
    Cliente para acessar a API interna (Privada) do NewCorban.
    Baseando em engenharia reversa do portal web.
    """
    STATIC_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://empreste.newcorban.com.br",
        "Referer": "https://empreste.newcorban.com.br/",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest"
    }

    def __init__(self):
        self.base_url = "https://server.newcorban.com.br"
        self.user = os.getenv("NEW_USER")
        self.password = os.getenv("NEW_PASSWORD")
        self.empresa = os.getenv("NEW_EMPRESA")

        self.token_manager = TokenManager()
        self.scope = "NEWCORBAN_PRIVATE"

    def _get_token(self) -> str:
        token = self.token_manager.get_token(self.scope)
        if token:
            return token
        
        logger.info("🔑 Token expirado ou inexistente. Logando...")
        token = self._login()

        if token:
            self.token_manager.save_token(self.scope, token, ttl_seconds=82800)

        return token
    
    def _get_headers(self) -> dict:
        """
        Constrói os headers dinamicamente.
        Junta os estáticos (User-Agent) com o dinâmico (Token)
        """
        headers = self.STATIC_HEADERS.copy()

        token = self._get_token()

        if token:
            headers["Authorization"] = f"Bearer {token}"

        return headers
    
    def _login(self) -> str:
        url = f"{self.base_url}/api/v2/login"

        payload = {
            "usuario": self.user,
            "empresa": self.empresa,
            "ip": "127.0.0.1",
            "senha": self.password,
            "p": "facta"
        }

        try:
            resp = httpx.post(url, data=payload, headers=self.STATIC_HEADERS, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            return data.get("token")
        
        except Exception as e:
            logger.error(f"❌ Falha login NewCorban: {e}")
            raise e
    
    def buscar_historico_bancario(self, cpf: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/system/cliente.php"

        params = {"action": "getBankAccountHistory", "cpf": cpf}

        try:
            resp = httpx.get(url, params=params, headers=self._get_headers(), timeout=15)

            if resp.status_code == 401:
                logger.warning("🔄 Token expirado (401). Forçando renovação...")
                
                novo_token = self._login()

                if novo_token:
                    self.token_manager.save_token(self.scope, novo_token, ttl_seconds=82800)
                    
                resp = httpx.get(url, params=params, headers=self._get_headers(), timeout=15)

            return resp.json()
        
        except Exception as e:
            logger.error(f"❌ Erro NewCorban: {e}")
            return None

    