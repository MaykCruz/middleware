import os
import redis
from fastapi import FastAPI, Header, HTTPException, status, Depends
from dotenv import load_dotenv
from app.infrastructure.celery import celery_app
from app.routers import webhook_chatguru
from app.core.logger import setup_logging

load_dotenv()
setup_logging()

app = FastAPI(title="Middleware API")

app.include_router(webhook_chatguru.router)

def verify_admin_token(x_admin_token: str = Header(default=None)):
    """
    Verfica se o header 'x-admin-token' bate com a senha do .env.
    """
    expected_token = os.getenv("ADMIN_API_TOKEN")

    if not expected_token:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ADMIN_API_TOKEN não configurado no servidor."
        )
    
    if x_admin_token != expected_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de administração inválido ou ausente 🚫"
        )

@app.get("/")
async def root():
    return {"message": "Middleware is running 🚀"}

@app.get("/health/celery")
async def check_celery():
    """Endpoint para verificar conectividade com o Redis/Celery"""
    try:
        inspection = celery_app.control.inspect()
        active = inspection.active()
        return {"status": "ok", "workers_active": active}
    except Exception as e:
        return {"status": "error", "details": str(e)}

@app.post("/admin/refresh-messages", dependencies=[Depends(verify_admin_token)])
async def refresh_messages():
    """
    Limpa o cache de mensagens no Redis.
    Isso força o bot a baixar a versão mais recente do Gist na próxima interação.
    🔒 Protegido: Exige header 'x-admin-token'
    """
    try:
        redis_url = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

        r = redis.from_url(redis_url, decode_responses=True)

        r.delete("bot:content:messages")

        return {
            "status": "success",
            "message": "Cache limpo! 🧹 A próxima mensagem será carregada do Gist."
        }
    except Exception as e:
        return {"status": "error", "details": str(e)}