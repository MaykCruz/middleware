import os
from datetime import datetime
from pydantic import BaseModel, Field, field_validator
from typing import Optional, Literal

LOGIN_CERTIFICADO_DEFAULT = os.getenv("FACTA_LOGIN_CERTIFICADO")

class FactaBaseModel(BaseModel):
    """Base para adicionar validadores comuns a todos os schemas."""

    @field_validator('data_nascimento', 'data_expedicao', 'data_admissao', check_fields=False)
    @classmethod
    def validar_data_br(cls, v: str) -> str:
        """Garante que a data esteja no formato BR (DD/MM/AAAA)."""
        if not v:
            return v
        try:
            datetime.strptime(v, "%d/%m/%Y")
            return v
        except ValueError:
            raise ValueError('Data inválida. O formato deve ser DD/MM/AAAA')

class ProposalStep1Base(FactaBaseModel):
    """Campos comuns na Etapa 1 para qualquer produto."""
    cpf: str = Field(..., pattern=r'^\d{11}$', description="CPF apenas números", min_length=11, max_length=11)
    data_nascimento: str = Field(..., description="Formato DD/MM/AAAA")
    login_certificado: str = Field(default=LOGIN_CERTIFICADO_DEFAULT, description="Login do operador certificado")

class ProposalStep1CLT(ProposalStep1Base):
    """
    Payload específico para CLT
    """
    produto: str = "D"
    tipo_operacao: int = 13
    averbador: int = 10010
    convenio: int = 3

    codigo_tabela: int
    prazo: int
    valor_operacao: float
    valor_parcela: float
    coeficiente: float
    matricula: str

class ProposalStep1FGTS(ProposalStep1Base):
    """
    Payload específico para FGTS
    """
    produto: str = "D"
    tipo_operacao: int = 13
    averbador: int = 20095
    convenio: int = 3

    simulacao_fgts: int

class ProposalStep2Base(FactaBaseModel):
    """
    Dados Pessoais (Comuns a ambos)
    """
    id_simulador: int
    cpf: str = Field(..., pattern=r'^\d{11}$')
    nome: str = Field(..., min_length=3)
    sexo: Literal["M", "F"]
    data_nascimento: str

    rg: str
    estado_rg: str = Field(..., min_length=2, max_length=2)
    orgao_emissor: str
    data_expedicao: str

    cep: str = Field(..., pattern=r'^\d{8}$')
    endereco: str
    numero: int
    bairro: str
    cidade: int
    estado: str = Field(..., min_length=2, max_length=2)
    
    nome_mae: str
    estado_natural: str = Field(..., min_length=2, max_length=2)
    cidade_natural: int

    celular: str

    estado_civil: int = 4                             # Padrão: Solteiro
    nacionalidade: int = 1                            # Padrão: Brasileiro
    pais_origem: int = 26                             # Padrão: Brasil
    renda: float = 3000.00                            # Padrão: R$ 3.000
    complemento: str = ""                             # Padrão: Vazio
    nome_pai: str = "NAO INFORMOU"                    # Padrão
    valor_patrimonio: int = 1
    cliente_iletrado_impossibilitado: Literal["S", "N"] = "N"

    banco: Optional[str] = None
    agencia: Optional[str] = None
    conta: Optional[str] = None
    tipo_conta: Optional[Literal["C", "P"]] = None

    tipo_chave_pix: Optional[int] = None
    chave_pix: Optional[str] = None

    @field_validator('cep')
    @classmethod
    def validar_cep(cls, v: str) -> str:
        if not v.isdigit():
            raise ValueError("CEP deve conter apenas números.")
        return v

class ProposalStep2CLT(ProposalStep2Base):
    """Campos extras obrigatórios apenas para CLT"""
    matricula: str
    data_admissao: str
    cnpj_empregador: str

class ProposalStep3(BaseModel):
    codigo_cliente: int
    id_simulador: int
    tipo_formalizacao: str = "DIG"