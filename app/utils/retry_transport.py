import httpx
import time
import logging

logger = logging.getLogger(__name__)

class RetryTransport(httpx.HTTPTransport):
    """
    Transporte customizado para o HTTPX que realiza tentativas automáticas em caso de falhas de conexão ou timeouts.
    """
    def __init__(self, max_retries=3, backoff_factor=1.0, retry_status_codes=None, **kwargs):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_status_codes = retry_status_codes or []
        super().__init__(**kwargs)

    def handle_request(self, request):
        for attempt in range(1, self.max_retries + 1):
            try:
                response = super().handle_request(request)

                if response.status_code in self.retry_status_codes:
                    if attempt < self.max_retries:
                        wait_time = self.backoff_factor * (2 ** (attempt - 1))
                        logger.warning(
                            f"⚠️ [HTTP Retry] Status {response.status_code} recebido de {request.url}. "
                            f"Retentando em {wait_time}s... (Tentativa {attempt}/{self.max_retries})"
                        )
                        time.sleep(wait_time)
                        continue
                    
                return response
            
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout, httpx.NetworkError) as e:
                if attempt == self.max_retries:
                    logger.error(f"❌ [HTTP Retry] Todas as {self.max_retries} tentativas falharam para {request.url}. Erro final: {e}")
                    raise e
                
                wait_time = self.backoff_factor * (2 ** (attempt - 1))

                logger.warning(
                    f"⚠️ [HTTP Retry] Tentativa {attempt}/{self.max_retries} falhou. "
                    f"Retentando em {wait_time}s... Erro: {type(e).__name__}"
                )
                time.sleep(wait_time)