import logging
from fastapi import APIRouter, Header
from pydantic import BaseModel, Field
from app.infrastructure.celery import celery_app
from app.utils.validators import validate_cpf, clean_digits, formatar_telefone_br

router = APIRouter(prefix="/api/clt", tags=["API CLT"])
logger = logging.getLogger(__name__)

class SimulacaoCLTRequest(BaseModel):
    chat_id: str = Field(..., description="ID do chat")
    cpf: str = Field(..., description="CPF do cliente")
    nome: str = Field(..., description="Nome do cliente")
    celular: str = Field(..., description="Celular para envio do Termo (WhatsApp)")
    contact_id: str = Field(None, description="ID interno do contato na plataforma")

@router.post("/simular")
async def iniciar_simulacao_clt(
    request: SimulacaoCLTRequest,
    x_token: str = Header(None)
):
    """
    Inicia a simulação CLT de forma assíncrona.
    """
    cpf_limpo = clean_digits(request.cpf)

    if not validate_cpf(cpf_limpo):
        logger.info(f"🚫 [API] CPF Inválido recebido: {request.cpf}")

        return {
            "status": "erro",
            "code": "cpf_invalido",
            "message": "CPF informado é inválido."
        }
    
    telefone_formatado = formatar_telefone_br(request.celular)

    if not telefone_formatado:
        logger.info(f"🚫 [API] Telefone Inválido recebido: {request.celular}")
        return {
            "status": "erro",
            "code": "telefone_invalido",
            "message": "Telefone inválido. Informe DDD + Número."
        }
    
    request.cpf = cpf_limpo
    request.celular = telefone_formatado

    logger.info(f"🚀 [API CLT] Recebida solicitação para {request.cpf}")

    task = celery_app.send_task(
        "app.tasks.api_processor.executar_fluxo_clt",
        kwargs=request.model_dump()
    )

    return {
        "status": "PROCESSANDO",
        "code": "sucesso",
        "product": "CLT",
        "task_id": task.id,
        "message": "Solicitação CLT iniciada."
    }