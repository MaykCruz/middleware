import redis
import os
import logging
import json
import time

logger = logging.getLogger(__name__)

class SessionManager:
    def __init__(self):
        redis_url = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
        self.redis_client = redis.from_url(redis_url)
        self.expire_time = 3600 * 24

    def _get_key_state(self, chat_id: str):
        return f"chat:{chat_id}:state"
    
    def _get_key_context(self, chat_id: str):
        return f"chat:{chat_id}:context"
    
    def _get_key_interaction(self, chat_id: str):
        return f"chat:{chat_id}:last_interaction"
    
    def touch(self, chat_id: int):
        """Atualiza o timestamp da última interação para AGORA"""
        key = self._get_key_interaction(chat_id)
        now = int(time.time())
        self.redis_client.set(key, now, ex=self.expire_time)
        return now
    
    def get_last_interaction(self, chat_id: int):
        """Retorna o timestamp da última interação"""
        key = self._get_key_interaction(chat_id)
        val = self.redis_client.get(key)
        return int(val) if val else 0
    
    def clear_session(self, chat_id: int):
        """
        Remove todo o contexto do chat do Redis.
        Usado quando recebemos 'closedChat'.
        """
        try:
            key_state = self._get_key_state(chat_id)
            key_context = self._get_key_context(chat_id)

            self.redis_client.delete(key_state)
            deleted_count = self.redis_client.delete(key_context)

            if deleted_count > 0:
                logger.info(f"🧹 [Session] Sessão limpa para o Chat ID: {chat_id}")
            else:
                logger.debug(f"💨 [Session] Nenhuma sessão ativa encontrada para Chat ID: {chat_id}. Ignorando.")
        except Exception as e:
            logger.error(f"❌ Erro ao limpar sessão do chat {chat_id}: {str(e)}")

    def get_state(self, chat_id: int):
        key = self._get_key_state(chat_id)
        val = self.redis_client.get(key)
        return val.decode("utf-8") if val else "START"
    
    def set_state(self, chat_id: int, state: str):
        key = self._get_key_state(chat_id)
        self.redis_client.set(key, state, ex=self.expire_time)
    
    def set_context(self, chat_id: int, data: dict):
        """Salva dados temporários (ex: CPF)"""
        key = self._get_key_context(chat_id)
        self.redis_client.set(key, json.dumps(data), ex=self.expire_time)
    
    def get_context(self, chat_id: int) -> dict:
        """Recupera dados salvos"""
        key = self._get_key_context(chat_id)
        val = self.redis_client.get(key)
        return json.loads(val) if val else {}
    
    def update_context(self, chat_id: int, new_data: dict):
        """
        Mescla novos dados no contexto existente sem apagar os anteriores.
        Útil para adicionar dados da oferta selecionada ao CPF/Nome que já existem.
        """
        try:
            current_context = self.get_context(chat_id)
            if not current_context:
                current_context = {}
            current_context.update(new_data)
            self.set_context(chat_id, current_context)
            logger.debug(f"💾Contexto atualizado para Chat {chat_id}: {new_data.keys()}")
        except Exception as e:
            logger.error(f"❌ Erro ao atualizar contexto do chat {chat_id}: {str(e)}")
    
    def _get_key_v8_context(self, consult_id: str):
        """Gera a chave única baseada no ID da consulta do V8"""
        return f"v8_context:{consult_id}"
    
    def save_v8_context(self, consult_id: str, data: dict):
        """
        Encerra a cápsula do tempo do V8.
        Usa o mesmo expire_time (24) da classe para slimpar o Redis automaticamente.
        """
        try:
            key = self._get_key_v8_context(consult_id)
            self.redis_client.set(key, json.dumps(data), ex=self.expire_time)
            logger.info(f"💾 [Memória V8] Contexto salvo com sucesso para o consult_id {consult_id}")
        except Exception as e:
            logger.error(f"❌ [Memória V8] Erro ao salvar contexto do {consult_id}: {str(e)}")
    
    def get_v8_context(self, consult_id: str) -> dict:
        """
        Desenterra a cápsula do tempo quando o Webhook chamar.
        """
        try:
            key = self._get_key_v8_context(consult_id)
            val = self.redis_client.get(key)
            if val:
                return json.loads(val)
            
            logger.warning(f"⚠️ [Memória V8] Nenhum contexto encontrado para {consult_id}. Expirado ou inexistente.")
            return {}
        except Exception as e:
            logger.error(f"❌ [Memória V8] Erro ao recuperar contexto do {consult_id}: {str(e)}")
            return {}
    
    def delete_v8_context(self, consult_id: str):
        """Apaga o contexto para evitar processamento duplicado de webhooks."""
        key = self._get_key_v8_context(consult_id)
        self.redis_client.delete(key)