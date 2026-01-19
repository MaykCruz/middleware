import redis
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class TokenManager:
    _instance = None

    def __new__(cls):
        """
        Padrão Singleton: Garante que só exista uma conexão Redis aberta na memória da aplicação, economizando recursos.
        """
        if cls._instance is None:
            cls._instance = super(TokenManager, cls).__new__(cls)

            redis_url = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

            try:
                cls._instance.redis = redis.from_url(redis_url, decode_responses=True, socket_timeout=5.0, socket_connect_timeout=5.0)
                logger.info("🔐 [TokenManager] Conectado ao Redis com sucesso.")
            except Exception as e:
                logger.critical(f"❌ [TokenManager] Falha crítica ao conectar no Redis: {e}")
                raise e
            
        return cls._instance
    
    def _get_key(self, scope: str) -> str:
        """Gera a chave de armazenamento: auth:token:FACTA"""
        return f"auth:token:{scope.upper()}"
    
    def _get_lock_key(self, scope: str) -> str:
        """Gera a chave de bloqueio: lock:token:FACTA"""
        return f"lock:token:{scope.upper()}"
    
    def get_token(self, scope: str) -> Optional[str]:
        """
        Tenta recuperar um token válido para o escopo informado.
        Retorna None se não existir ou tiver expirado.
        """
        try:
            return self.redis.get(self._get_key(scope))
        except Exception as e:
            logger.error(f"⚠️ [TokenManager] Erro ao ler token ({scope}): {e}")
            return None
    
    def save_token(self, scope: str, token: str, ttl_seconds: int):
        """
        Salva o token com um tempo de vida (TTL) específico.

        Args:
            scope: Nome da API (ex 'FACTA')
            token: O hash do token
            ttl_seconds: Quanto tempo (em segundos) o token é válido na API.
        """
        safe_ttl = max(ttl_seconds - 60, 60)

        try:
            self.redis.set(self._get_key(scope), token, ex=safe_ttl)
            logger.info(f"💾 [TokenManager] Token {scope} salvo. Expira em {safe_ttl}s (Margem aplicada).")
        except Exception as e:
            logger.error(f"❌ [TokenManager] Erro ao salvar token ({scope}): {e}")
    
    def acquire_lock(self, scope: str, timeout: int = 10) -> bool:
        """
        Tenta ser o LÍDER da renovação (Mutex Distribuido).

        Returna:
            True: Você conseguiu o lock. DEVE renovar o token.
            False: Outro worker já está renovando. Espere e tenteler do cache.
        """
        lock_key = self._get_lock_key(scope)
        try:
            acquired = self.redis.set(lock_key, "LOCKED", ex=timeout, nx=True)
            return bool(acquired)
        except Exception as e:
            logger.error(f"⚠️ [TokenManager] Erro no lock ({scope}): {e}")
            return False
    
    def release_lock(self, scope: str):
        """Libera o bloqueio manualmente após renovar (ou falhar)."""
        try:
            self.redis.delete(self._get_lock_key(scope))
        except Exception:
            pass