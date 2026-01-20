import pytest
from app.services.products.fgts_service import FGTSService
from app.schemas.credit import AnalysisStatus

@pytest.fixture
def fgts_service_setup(mocker):
    # Mock da FactaFGTSService (Lógica de Integração)
    mock_facta = mocker.patch("app.services.products.fgts_service.FactaFGTSService")
    
    # Mock dos Dados Cadastrais (Busca de conta bancária)
    mock_dados = mocker.patch("app.services.products.fgts_service.FactaDadosCadastrais")
    
    # Mock da função de data para garantir que o teste não quebre dependendo do dia
    mocker.patch("app.services.products.fgts_service.calcular_segundo_dia_util_prox_mes", return_value="05 de fevereiro")

    service = FGTSService()
    
    return {
        "service": service,
        "facta": mock_facta.return_value,
        "dados": mock_dados.return_value
    }

def test_fgts_aprovado_sem_conta(fgts_service_setup):
    """Cenário: Cliente tem saldo, mas Facta não retornou conta bancária."""
    setup = fgts_service_setup
    
    setup["facta"].simular_antecipacao.return_value = {
        "aprovado": True,
        "motivo": "VALOR_DISPONÍVEL",
        "detalhes": {
            "valor_liquido": 500.00,
            "taxa": 1.80
        }
    }
    setup["dados"].buscar_conta_bancaria.return_value = None

    oferta = setup["service"].consultar_melhor_oportunidade("12345678900")

    assert oferta.status == AnalysisStatus.APROVADO
    assert oferta.message_key == "com_saldo" # Msg genérica
    assert oferta.valor_liquido == 500.00

def test_fgts_aprovado_com_conta(fgts_service_setup):
    """Cenário: Cliente tem saldo E conta bancária identificada."""
    setup = fgts_service_setup
    
    setup["facta"].simular_antecipacao.return_value = {
        "aprovado": True,
        "motivo": "VALOR_DISPONÍVEL",
        "detalhes": {
            "valor_liquido": 1200.00
        }
    }
    # Simula conta encontrada
    setup["dados"].buscar_conta_bancaria.return_value = {
        "texto_formatado": "Caixa Econômica, Ag 0001..."
    }

    oferta = setup["service"].consultar_melhor_oportunidade("12345678900")

    assert oferta.status == AnalysisStatus.APROVADO
    assert oferta.message_key == "com_saldo_conta" # Msg específica confirmando a conta
    assert "Caixa Econômica" in oferta.variables["dados_bancarios"]

def test_fgts_sem_autorizacao(fgts_service_setup):
    """Cenário: Cliente esqueceu de autorizar o banco."""
    setup = fgts_service_setup
    
    setup["facta"].simular_antecipacao.return_value = {
        "aprovado": False,
        "motivo": "SEM_AUT" # ou SEM_AUTORIZACAO
    }

    oferta = setup["service"].consultar_melhor_oportunidade("12345678900")

    assert oferta.status == AnalysisStatus.SEM_AUTORIZACAO
    assert oferta.message_key == "sem_autorizacao"

def test_fgts_aniversariante(fgts_service_setup):
    """Cenário: Bloqueio por mês de aniversário (deve retornar data futura)."""
    setup = fgts_service_setup
    
    setup["facta"].simular_antecipacao.return_value = {
        "aprovado": False,
        "motivo": "ANIVERSARIANTE"
    }

    oferta = setup["service"].consultar_melhor_oportunidade("12345678900")

    assert oferta.status == AnalysisStatus.ANIVERSARIANTE
    assert oferta.message_key == "aniversariante"
    # Verifica se usou o valor do nosso mock de data
    assert oferta.variables["data"] == "05 de fevereiro"

def test_fgts_limite_excedido(fgts_service_setup):
    """Cenário: Erro de limite de consultas da API."""
    setup = fgts_service_setup
    
    setup["facta"].simular_antecipacao.return_value = {
        "aprovado": False,
        "motivo": "LIMITE_EXCEDIDO_CONSULTAS_FGTS"
    }

    oferta = setup["service"].consultar_melhor_oportunidade("12345678900")

    assert oferta.status == AnalysisStatus.LIMITE_EXCEDIDO_CONSULTAS_FGTS
    assert oferta.is_internal is True # Deve ser forçado como interno para o bot não travar

def test_fgts_retorno_desconhecido(fgts_service_setup):
    """Cenário: Erro não mapeado."""
    setup = fgts_service_setup
    
    setup["facta"].simular_antecipacao.return_value = {
        "aprovado": False,
        "motivo": "ERRO_BIZARRO_DO_SISTEMA",
        "msg_tecnica": "Stacktrace java null pointer..."
    }

    oferta = setup["service"].consultar_melhor_oportunidade("12345678900")

    assert oferta.status == AnalysisStatus.RETORNO_DESCONHECIDO
    assert oferta.variables["erro"] == "Stacktrace java null pointer..."