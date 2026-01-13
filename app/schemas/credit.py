from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field

class AnalysisStatus(str, Enum):
    """
    Lista Global de Status de Análise (FGTS + CLT + Genéricos)
    """
    # Genéricos
    APROVADO = "APROVADO"
    RETORNO_DESCONHECIDO = "RETORNO_DESCONHECIDO"
    ERRO_TECNICO = "ERRO_TECNICO"

    # Específicos FGTS
    SEM_AUTORIZACAO = "SEM_AUTORIZACAO"
    MUDANCAS_CADASTRAIS = "MUDANCAS_CADASTRAIS"
    SEM_SALDO = "SEM_SALDO"
    ANIVERSARIANTE = "ANIVERSARIANTE"
    SALDO_NAO_ENCONTRADO = "SALDO_NAO_ENCONTRADO"
    SEM_ADESAO = "SEM_ADESAO"
    LIMITE_EXCEDIDO_CONSULTAS_FGTS = "LIMITE_EXCEDIDO_CONSULTAS_FGTS"

    #Específicos CLT
    AGUARDANDO_AUTORIZACAO = "AGUARDANDO_AUTORIZACAO"
    TELEFONE_VINCULADO_OUTRO_CPF = "TELEFONE_VINCULADO_OUTRO_CPF"
    CPF_NAO_ENCONTRADO_NA_BASE = "CPF_NAO_ENCONTRADO_NA_BASE"
    IDADE_INSUFICIENTE = "IDADE_INSUFICIENTE"
    IDADE_INSUFICIENTE_FACTA = "IDADE_INSUFICIENTE_FACTA"
    SEM_MARGEM = "SEM_MARGEM"
    CATEGORIA_CNAE_INVALIDA = "CATEGORIA_CNAE_INVALIDA"
    SEM_OFERTA = "SEM_OFERTA"
    VIRADA_FOLHA = "VIRADA_FOLHA"
    REPROVADO_POLITICA = "REPROVADO_POLITICA"
    

class CreditOffer(BaseModel):
    """
    Substitui o dataclass.
    Representa o resultado padronizado de uma simulação de crédito.
    """
    status: AnalysisStatus
    message_key: str = Field(..., description="Chave da mensagem no messages.json")

    variables: Dict[str, str] = Field(default_factory=dict)
    is_internal: bool = False

    raw_details: Dict[str, Any] = Field(default_factory=dict)

    banco_origem: Optional[str] = None
    valor_liquido: Optional[float] = 0.0
