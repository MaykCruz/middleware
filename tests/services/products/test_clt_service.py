import pytest
from app.services.products.clt_service import CLTService
from app.schemas.credit import AnalysisStatus

@pytest.fixture
def clt_service_setup(mocker):
    # Mock das dependências externas (Facta e Dados Cadastrais)
    mock_facta_service = mocker.patch("app.services.products.clt_service.FactaCLTService")
    mock_dados_cadastrais = mocker.patch("app.services.products.clt_service.FactaDadosCadastrais")
    
    service = CLTService()
    
    return {
        "service": service,
        "facta": mock_facta_service.return_value,
        "dados": mock_dados_cadastrais.return_value
    }

def test_clt_sucesso_com_conta(clt_service_setup):
    """Cenário Ideal: Aprovado e já temos a conta bancária do cliente"""
    setup = clt_service_setup
    
    # 1. Simula retorno positivo da Facta
    setup["facta"].simular_clt.return_value = {
        "aprovado": True,
        "motivo": "APROVADO",
        "oferta": {
            "valor_liquido": 1500.0,
            "parcela": 150.0,
            "prazo": 12
        }
    }
    
    # 2. Simula que achamos a conta no banco de dados
    setup["dados"].buscar_conta_bancaria.return_value = {
        "texto_formatado": "Banco Brasil, Ag 1234, CC 5678-9"
    }
    
    # 3. Executa
    oferta = setup["service"].consultar_oportunidade("12345678900", "Fulano", "11999999999")
    
    # 4. Verifica
    assert oferta.status == AnalysisStatus.APROVADO
    assert oferta.message_key == "clt_oferta_disponivel_conta" # Deve usar a msg que mostra a conta
    assert "Banco Brasil" in oferta.variables["dados_bancarios"]

def test_clt_sucesso_sem_conta(clt_service_setup):
    """Cenário Comum: Aprovado, mas cliente precisa informar Pix"""
    setup = clt_service_setup
    
    setup["facta"].simular_clt.return_value = {
        "aprovado": True,
        "motivo": "APROVADO",
        "oferta": {
            "valor_liquido": 1000.0,
            "parcela": 100.0,
            "prazo": 12
        }
    }
    
    # Não achou conta (None)
    setup["dados"].buscar_conta_bancaria.return_value = None
    
    oferta = setup["service"].consultar_oportunidade("123", "Fulano", "11999999999")
    
    assert oferta.status == AnalysisStatus.APROVADO
    assert oferta.message_key == "clt_oferta_disponivel" # Msg genérica pedindo Pix

def test_clt_aguardando_autorizacao(clt_service_setup):
    """Cenário de Termo: Cliente ainda não aceitou o link"""
    setup = clt_service_setup
    
    setup["facta"].simular_clt.return_value = {
        "aprovado": False,
        "motivo": "AGUARDANDO_AUTORIZACAO"
    }
    
    oferta = setup["service"].consultar_oportunidade("123", "Fulano", "11999999999")
    
    assert oferta.status == AnalysisStatus.AGUARDANDO_AUTORIZACAO
    assert oferta.message_key == "clt_termo_enviado"

def test_clt_idade_insuficiente_com_sugestao(clt_service_setup):
    """Cenário Complexo: Facta recusou por idade, mas sugerimos outros bancos"""
    setup = clt_service_setup
    
    # Idade 25 (Recusada na Facta mulher, mas aceita no C6, HUB, etc.)
    setup["facta"].simular_clt.return_value = {
        "aprovado": False,
        "motivo": "IDADE_INSUFICIENTE_FACTA",
        "idade": 25,
        "sexo": "M",
        "margem_disponivel": 200.00, # Margem boa
        "data_admissao": "01/01/2020"
    }
    
    oferta = setup["service"].consultar_oportunidade("123", "Fulano", "11999999999")
    
    assert oferta.status == AnalysisStatus.IDADE_INSUFICIENTE_FACTA
    
    # Verifica se a lógica montou a sugestão corretamente
    sugestao = oferta.raw_details["sugestao_bancos"]
    assert "C6" in sugestao
    assert "HUB" in sugestao

def test_clt_idade_insuficiente_sem_margem(clt_service_setup):
    """Cenário de Borda: Idade daria para outros bancos, mas sem margem para portar"""
    setup = clt_service_setup
    
    setup["facta"].simular_clt.return_value = {
        "aprovado": False,
        "motivo": "IDADE_INSUFICIENTE_FACTA",
        "idade": 25,
        "sexo": "M",
        "margem_disponivel": 10.00, # Margem muito baixa (< R$ 50)
        "data_admissao": "01/01/2020"
    }
    
    oferta = setup["service"].consultar_oportunidade("123", "Fulano", "11999999999")
    
    # O sistema deve priorizar o erro de SEM MARGEM ao invés de sugerir bancos impossíveis
    assert oferta.status == AnalysisStatus.SEM_MARGEM
    assert oferta.message_key == "sem_margem_cliente"

def test_clt_erro_tecnico(clt_service_setup):
    """Cenário de Falha: API Facta fora do ar"""
    setup = clt_service_setup
    
    setup["facta"].simular_clt.return_value = {
        "aprovado": False,
        "motivo": "ERRO_TECNICO",
        "msg_tecnica": "Timeout connecting to Facta"
    }
    
    oferta = setup["service"].consultar_oportunidade("123", "Fulano", "11999999999")
    
    assert oferta.status == AnalysisStatus.ERRO_TECNICO
    assert oferta.is_internal is True # Erros técnicos devem ser internos