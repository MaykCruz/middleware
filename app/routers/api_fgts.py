import logging
from fastapi import APIRouter, Header
from pydantic import BaseModel, Field
from app.infrastructure.celery import celery_app
from app.services.bot.memory.session import SessionManager
from app.utils.validators import validate_cpf, clean_digits, formatar_telefone_br

router = APIRouter(prefix="/api/fgts", tags=["API FGTS"])
logger = logging.getLogger(__name__)

class SimulacaoFGTSRequest(BaseModel):
    chat_id: str = Field(..., description="ID do chat")
    cpf: str = Field(..., description="CPF do cliente")
    nome: str = Field(None, description="Nome do cliente")
    celular: str = Field(..., description="Celular para contato/digitação")
    contact_id: str = Field(None, description="ID interno do contato na plataforma")

class ContratacaoFGTSRequest(BaseModel):
    chat_id: str = Field(..., description="ID do chat para recuperar contexto e efetivar")

@router.post("/simular")
async def iniciar_simulacao_fgts(
    request: SimulacaoFGTSRequest,
    x_token: str = Header(None)
):
    """
    Inicia a simulação FGTS de forma assíncrona.
    """
    cpf_limpo = clean_digits(request.cpf)

    if not validate_cpf(cpf_limpo):
        logger.info(
            f"🚫 [API] CPF Inválido recebido: {request.cpf} | "
            f"Chat: {request.chat_id} | "
            f"Nome: {request.nome} | "
            f"Contact ID: {request.contact_id}"
        )

        return {
            "status": "erro",
            "code": "cpf_invalido",
            "message": "CPF informado é inválido."
        }
    
    telefone_formatado = formatar_telefone_br(request.celular)

    if not telefone_formatado:
        logger.info(f"🚫 [API] Telefone Inválido recebido: {request.celular}| "
            f"Chat: {request.chat_id} | "
            f"Nome: {request.nome} | "
            f"Contact ID: {request.contact_id}"
        )
        
        return {
            "status": "erro",
            "code": "telefone_invalido",
            "message": "Telefone inválido. Informe DDD + Número."
        }
    
    request.cpf = cpf_limpo
    request.celular = telefone_formatado

    logger.info(f"🚀 [API FGTS] Recebida solicitação para {request.cpf}")

    session = SessionManager()

    contexto = {
        "cpf": request.cpf,
        "nome": request.nome,
        "celular": request.celular,
        "contact_id": request.contact_id
    }

    session.set_context(request.chat_id, contexto)

    task = celery_app.send_task(
        "app.tasks.api_processor.executar_fluxo_fgts",
        kwargs=request.model_dump()
    )

    return {
        "status": "PROCESSANDO",
        "code": "sucesso",
        "product": "FGTS",
        "task_id": task.id,
        "message": "Solicitação FGTS iniciada."
    }

@router.post("/contratar")
async def contratar_fgts(
    request: ContratacaoFGTSRequest,
    x_token: str = Header(None)
):
    """
    Dispara a esteira de contratação (Digitação Facta).
    Deve ser chamado quando o cliente confirma o aceite da proposta.
    """
    logger.info(f"✍️ [API FGTS] Recebida solicitação de CONTRATAÇÃO para Chat {request.chat_id}")

    task = celery_app.send_task(
        "app.tasks.api_processor.executar_digitacao_fgts",
        kwargs={"chat_id": request.chat_id}
    )

    return {
        "status": "PROCESSANDO_DIGITACAO",
        "code": "sucesso",
        "product": "FGTS",
        "task_id": task.id,
        "message": "Processo de contratação iniciado."
    }