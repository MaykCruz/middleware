import logging
import os
import sys
import contextvars
from logtail import LogtailHandler

chat_id_var = contextvars.ContextVar("chat_id", default=None)

class ChatIDFilter(logging.Filter):
    def filter(self, record):
        chat_id = chat_id_var.get()
        if chat_id and not str(record.msg).startswith(f"[ChatID: {chat_id}]"):
            record.msg = f"[ChatID: {chat_id}] {record.msg}"
        return True

def setup_logging():
    """
    Configura o logger raiz e aplica silenciadores nas bibliotecas de terceiros.
    """
    log_formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    root_logger.setLevel(log_level)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)

    console_handler.addFilter(ChatIDFilter())
    root_logger.addHandler(console_handler)

    token = os.getenv("BETTER_STACK_SOURCE_TOKEN")
    endpoint = os.getenv("BETTER_STACK_INGEST_URL")

    if token:
        try:
            if endpoint:
                handler = LogtailHandler(source_token=token, host=endpoint)
            else:
                handler = LogtailHandler(source_token=token)

            handler.setFormatter(log_formatter)
            
            handler.addFilter(ChatIDFilter())
            root_logger.addHandler(handler)
            logging.info("🚀 [System] Better Stack Logging conectado com sucesso.")
        except Exception as e:
            print(f"❌ Erro ao configurar Better Stack: {e}")
    else:
        print("⚠️ BETTER_STACK_SOURCE_TOKEN não encontrado. Logs apenas locais.")
    
    if log_level == "DEBUG":
        noisy_libraries = [
            "urllib3", "urllib3.connectionpool", "urllib3.util.retry",
            "uvicorn", "uvicorn.access", "uvicorn.error",
            "httpcore", "httpx", "hpack",
            "celery", "celery.worker", "celery.task", "celery.redirected",
            "kombu", "amqp", "vine",
            "redis", 
            "asyncio", "watchfiles"
        ]

        for lib in noisy_libraries:
            logging.getLogger(lib).setLevel(logging.WARNING)