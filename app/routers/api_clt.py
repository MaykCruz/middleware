import logging
from fastapi import APIRouter, Header
from pydantic import BaseModel, Field
from app.infrastructure.celery import celery_app
from app.utils.validators import validate_cpf, clean_digits, formatar_telefone_br
from app.utils.formatters import limpar_nome
from app.services.bot.memory.session import SessionManager
from app.integrations.huggy.service import HuggyService

router = APIRouter(prefix="/api/clt", tags=["API CLT"])
logger = logging.getLogger(__name__)

class SimulacaoCLTRequest(BaseModel):
    chat_id: str = Field(..., description="ID do chat")
    cpf: str = Field(..., description="CPF do cliente")
    nome: str = Field(..., description="Nome do cliente")
    celular: str = Field(..., description="Celular para envio do Termo (WhatsApp)")
    contact_id: str = Field(None, description="ID interno do contato na plataforma")

class VerificarAuthRequest(BaseModel):
    chat_id: str = Field(..., description="ID do chat para recuperar contexto")
    verificacao_manual: bool = Field(False, description="Se True, envia para humano após esgotar tentativas. Default: False." )

class ContratacaoCLTRequest(BaseModel):
    chat_id: str = Field(..., description="ID do chat para recuperar contexto e efetivar")

class AtualizarTelefoneRequest(BaseModel):
    chat_id: str = Field(..., description="ID do chat")
    novo_telefone: str = Field(..., description="Novo telefone informado pelo cliente")

@router.post("/simular")
async def iniciar_simulacao_clt(
    request: SimulacaoCLTRequest,
    x_token: str = Header(None)
):
    """
    Inicia a simulação CLT de forma assíncrona e salva o contexto no Redis.
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

        huggy = HuggyService()

        huggy.send_message(
            chat_id=request.chat_id,
            message_key="blank",
            variables={"blank": f"🚫 [API] Telefone Inválido recebido: {request.celular}"},
            force_internal=True
        )

        huggy.start_put_in_queue(request.chat_id)
        
        return {
            "status": "erro",
            "code": "telefone_invalido",
            "message": "Telefone inválido. Atendimento distribuido"
        }
    
    nome_limpo = limpar_nome(request.nome)
    
    request.cpf = cpf_limpo
    request.celular = telefone_formatado
    request.nome = nome_limpo

    logger.info(f"🚀 [API CLT] Recebida solicitação para {request.cpf}")

    session = SessionManager()

    contexto = {
        "cpf": request.cpf,
        "nome": request.nome,
        "celular": request.celular,
        "contact_id": request.contact_id
    }

    session.set_context(request.chat_id, contexto)

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

@router.post("/verificar-autorizacao")
async def verificar_autorizacao_clt(
    request: VerificarAuthRequest,
    x_token: str = Header(None)
):
    """
    Endpoint chamado pelo botão 'Já autorizei'.
    Recupera o CPF da memória (Redis) pelo Chat ID e reprocessa.
    """
    logger.info(f"🔄 [API CLT] Verificando autorização para Chat {request.chat_id}")

    session = SessionManager()
    contexto = session.get_context(request.chat_id)

    cpf = contexto.get("cpf") if contexto else None

    if not cpf:
        logger.warning(f"⚠️ [API CLT] Sessão expirada ou não encontrada para Chat {request.chat_id}")
        return {
            "status": "ERRO",
            "code": "sessao_expirada",
            "message": "Sessão não encontrada ou expirada. Por favor, reinicie o atendimento."
        }
    
    nome = contexto.get("nome", "")
    celular = contexto.get("celular", "")
    contact_id = contexto.get("contact_id")

    logger.info(f"✅ [API CLT] Contexto recuperado: CPF {cpf}")

    task = celery_app.send_task(
        "app.tasks.api_processor.executar_fluxo_clt",
        kwargs={
            "chat_id": request.chat_id,
            "cpf": cpf,
            "nome": nome,
            "celular": celular,
            "contact_id": contact_id,
            "enviar_link": False,
            "verificacao_manual": request.verificacao_manual
        }
    )

    return {
        "status": "PROCESSANDO",
        "code": "sucesso",
        "task_id": task.id,
        "message": "Reconsulta de autorização iniciada."
    }

@router.post("/atualizar-telefone")
async def atualizar_telefone_clt(
    request: AtualizarTelefoneRequest,
    x_token: str = Header(None)
):
    """
    Recebe o novo telefone do Flow de correção e reinicia a simulação.
    """
    logger.info(f"🔄 [API CLT] Atualizando telefone para Chat {request.chat_id}: {request.novo_telefone}")

    session = SessionManager()

    contexto = session.get_context(request.chat_id)

    if not contexto or not contexto.get("cpf"):
        logger.warning(f"⚠️ [API CLT] Contexto perdido durante troca de telefone. Chat {request.chat_id}")
        return {
            "status": "ERRO",
            "code": "sessao_expirada",
            "message": "Sessão expirada. Reinicie o atendimento informando o CPF."
        }
    
    novo_telefone_formatado = formatar_telefone_br(request.novo_telefone)

    if not novo_telefone_formatado:
        logger.warning(f"🚫 [API CLT] Novo telefone inválido: {request.novo_telefone}")

        huggy = HuggyService()

        huggy.send_message(
            chat_id=request.chat_id,
            message_key="blank",
            variables={"blank": f"🚫 [API] Telefone Inválido recebido: {request.novo_telefone}"},
            force_internal=True
        )

        huggy.start_put_in_queue(request.chat_id)

        return {
            "status": "erro",
            "code": "telefone_invalido",
            "message": "O número informado é inválido."
        }
    
    contexto["celular"] = novo_telefone_formatado
    session.set_context(request.chat_id, contexto)

    logger.info(f"✅ [API CLT] Contexto corrigido. Reiniciando simulação para {contexto['cpf']} no novo número.")

    task = celery_app.send_task(
        "app.tasks.api_processor.executar_fluxo_clt",
        kwargs={
            "chat_id": request.chat_id,
            "cpf": contexto["cpf"],
            "nome": contexto.get("nome", ""),
            "celular": novo_telefone_formatado, # <--- Número Novo
            "contact_id": contexto.get("contact_id"),
            "enviar_link": True # True = Envia SMS para o novo número
        }
    )
    
    return {
        "status": "PROCESSANDO",
        "code": "sucesso",
        "task_id": task.id,
        "message": "Telefone atualizado e simulação reiniciada."
    }

@router.post("/contratar")
async def contratar_clt(
    request: ContratacaoCLTRequest,
    x_token: str = Header(None)
):
    """
    Dispara a esteira de contratação (Digitação Facta).
    Deve ser chamado quando o cliente confirma o aceite da proposta.
    """
    logger.info(f"✍️ [API CLT] Recebida solicitação de CONTRATAÇÃO para Chat {request.chat_id}")

    task = celery_app.send_task(
        "app.tasks.api_processor.executar_digitacao_clt",
        kwargs={"chat_id": request.chat_id}
    )

    return {
        "status": "PROCESSANDO_DIGITACAO",
        "code": "sucesso",
        "product": "CLT",
        "task_id": task.id,
        "message": "Processo de contratação CLT iniciado."
    }