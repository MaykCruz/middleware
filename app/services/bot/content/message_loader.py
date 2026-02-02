import json
import os
import logging
import httpx
import redis
from typing import Dict

logger = logging.getLogger(__name__)

class MessageLoader:
    _local_messages = {}
    _loaded = False

    REDIS_KEY = "bot:content:messages"
    TTL = 600

    @classmethod
    def _get_redis(cls):
        redis_url = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
        return redis.from_url(redis_url, decode_responses=True)

    @classmethod
    def load_local(cls):
        """Carrega do arquivo físico (Fallback de segurança ou Modo DEV)"""
        if cls._loaded:
            return cls._local_messages
        
        base_dir = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(base_dir, 'messages.json')

        try:
            with open(json_path, encoding='utf-8') as f:
                cls._local_messages = json.load(f)
                cls._loaded = True
                logger.info("📄 [MessageLoader] Mensagens carregadas com sucesso.")
        except Exception as e:
            logger.error(f"❌ [MessageLoader] Erro crítico ao carregar messages.json: {e}")
            cls._local_messages = {}
        
        return cls._local_messages
    
    @classmethod
    def fetch_remote(cls) -> Dict:
        """Busca o JSON atualizdo na URL externa (Gist)"""
        url = os.getenv("MESSAGES_URL")
        if not url: return {}

        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    return data
        except Exception as e:
            logger.warning(f"⚠️ [MessageLoader] Falha ao buscar mensagens remotas: {e}")
        
        return {}
    
    @classmethod
    def get(cls, key: str) -> dict:
        usar_gist = os.getenv("LOAD_MESSAGES_FROM_GIST", "true").lower() == "true"

        if not usar_gist:
            return cls.load_local().get(key, {})

        r = cls._get_redis()

        try:
            cached = r.get(cls.REDIS_KEY)
            if cached:
                messages = json.loads(cached)
                return messages.get(key, {})
        except Exception:
            pass

        logger.info("🔄 [MessageLoader] Cache expirado. Buscando atualizações...")
        remote_data = cls.fetch_remote()

        if remote_data:
            try:
                r.set(cls.REDIS_KEY, json.dumps(remote_data), ex=cls.TTL)
                logger.info("✅ [MessageLoader] Mensagens atualizadas e cacheadas.")
                return remote_data.get(key, {})
            except Exception as e:
                logger.error(f"❌ Erro ao salvar cache: {e}")
        
        local_data = cls.load_local()
        return local_data.get(key, {})