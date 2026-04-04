import os
import httpx
from app.utils.retry_transport import RetryTransport
import logging
import time
from app.infrastructure.token_manager import TokenManager

logger = logging.getLogger(__name__)

_global_v8_client = None

def create_v8_client(timeout: float = 60.0) -> httpx.Client:
    global _global_v8_client

    if _global_v8_client is None or _global_v8_client.is_closed:
        retry_transport = RetryTransport(
            max_retries=3,
            backoff_factor=2,
            retry_status_codes=[500, 520, 502, 503, 522, 524, 504, 429]
        )

        limits = httpx.Limits(
            max_keepalive_connections=50,
            max_connections=100,
            keepalive_expiry=10.0
        )

        _global_v8_client = httpx.Client(
            timeout=timeout,
            transport=retry_transport,
            limits=limits
        )

    return _global_v8_client

class V8Auth:
    def __init__(self):
        self.base_url = os.getenv("V8_API_URL")
        self.username = os.getenv("V8_USERNAME")
        self.password = os.getenv("V8_PASSWORD")
        self.audience = os.getenv("V8_AUDIENCE")
        self.client_id = os.getenv("V8_CLIENT_ID")

        self.token_manager = TokenManager()
        self.SCOPE = "V8_AUTH"
    
    def get_valid_token(self) -> str:
        max_tentativas = 5

        for tentativa in range(1, max_tentativas + 1):
            try:
                token = self.token_manager.get_token(self.SCOPE)
                if token:
                    return token
            except Exception as e:
                logger.warning(f"⚠️ [V8Auth] Erro ao ler cache (Tentativa {tentativa}): {e}")
            
            if self.token_manager.acquire_lock(self.SCOPE):
                try:
                    logger.info("🔑 [V8] Iniciando renovação de token na API...")
                    new_token, expires_in = self._request_api_token()
                    self.token_manager.save_token(self.SCOPE, new_token, expires_in)
                    return new_token
                except Exception as e:
                    self.token_manager.release_lock(self.SCOPE)
                    logger.exception(f"❌ [V8] Falha crítica na renovação: {str(e)}")
                    raise e
            
            else:
                if tentativa < max_tentativas:
                    logger.info(f"⏳ [V8] A aguardar renovação (Tentativa {tentativa}/{max_tentativas})...")
                    time.sleep(2)
                else:
                    logger.error("⏰ [V8] Timeout ao aguardar lock de autenticação.")
        
        raise TimeoutError("Falha crítica: Não foi possível obter token da V8 após múltiplas tentativas.")
    
    def _request_api_token(self) -> tuple[str, int]:
        url = f"{self.base_url}/oauth/token"

        if not all([self.username, self.password, self.audience, self.client_id]):
            raise ValueError("Credenciais V8 não configuradas no .env (V8_USERNAME, V8_PASSWORD, V8_AUDIENCE, V8_CLIENT_ID).")
        
        payload = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
            "audience": self.audience,
            "scope": "offline_access",
            "client_id": self.client_id
        }

        logger.info(
            f"🔍 [DEBUG V8 AUTH] Preparando request para {url}\n"
            f"Payload Raw (com repr para ver sujeiras): \n"
            f"Username: {repr(self.username)}\n"
            f"Password: {repr(self.password)}\n"
            f"ClientID: {repr(self.client_id)}"
        )

        try:
            client = create_v8_client()

            logger.info(f"🔍 [DEBUG V8 AUTH] Headers do Client: {client.headers}")

            response = client.post(url, data=payload)
            response.raise_for_status()

            data = response.json()
            token = data.get("access_token")
            expires_in = int(data.get("expires_in", 86400))

            if not token:
                raise ValueError("API retornou 200 mas sem campo 'access_token'.")
            
            logger.info("✅ [V8] Token gerado com sucesso.")
            return token, expires_in
        
        except httpx.HTTPStatusError as e:
            logger.error(
                f"❌ [DEBUG V8 AUTH] A API rejeitou a requisição!\n"
                f"Status: {e.response.status_code}\n"
                f"Headers da Resposta: {dict(e.response.headers)}\n"
                f"Corpo (Raw): {e.response.text}"
            )
            raise e
        except Exception as e:
            logger.error(f"❌ [V8] Erro de Conexão: {str(e)}")
            raise e
