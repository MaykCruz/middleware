import os
from celery import Celery
from celery.schedules import crontab
from celery.signals import setup_logging, worker_process_init
from dotenv import load_dotenv
from app.core.logger import setup_logging as configure_custom_logging

load_dotenv()

BROKER_URL = os.getenv("CELERY_BROKER_URL")
BACKEND_URL = os.getenv("CELERY_RESULT_BACKEND")

# --- CONEXÃO DO BETTER STACK ---
@setup_logging.connect
def config_loggers(*args, **kwargs):
    configure_custom_logging()
# -------------------------------

@worker_process_init.connect
def init_worker_logger(*args, **kwargs):
    configure_custom_logging()

celery_app = Celery(
    "worker",
    broker=BROKER_URL,
    backend=BACKEND_URL,
    include=[
        "app.tasks.api_processor"
    ],
)

# --- Configurações de Robustez ---
celery_app.conf.update(
    timezone='America/Sao_Paulo',
    enable_utc=False,
    task_track_started=True,
    result_expires=3600,          # Resultados expiram após 1 hora
    task_time_limit=180,      # Mata a task se demorar mais de 120s (evita zumbis)
    task_soft_time_limit=160,
    worker_prefetch_multiplier=1, # Garante que tasks longas não travem tasks rápidas
    task_default_queue="main-queue",

    broker_transport_options={
        'visibility_timeout': 300,  # 5 minutos (Default era 3600/1h)
        'fanout_prefix': True,
        'fanout_patterns': True
    },


    task_routes={
        "app.tasks.api_processor.*": {"queue": "main-queue"}
    }
)

celery_app.conf.beat_schedule = {
    'varredura-agendamentos-a-cada-minuto': {
        'task': 'app.tasks.api_processor.varredor_agendamentos',
        'schedule': crontab(minute='*')
    }
}