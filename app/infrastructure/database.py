import os
import logging
from supabase import create_client, Client

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase_client = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("✅ [Database] Supabase Client inicializado com sucesso.")
    except Exception as e:
        logger.error(f"❌ [Database] Erro ao inicializar Supabase: {e}")
else:
    logger.warning("⚠️ [Database] SUPABASE_URL ou SUPABASE_KEY não encontradas nas variáveis de ambiente.")