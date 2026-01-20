import pytest
from app.integrations.facta.complementares.funcoes_complementares import FactaDadosCadastrais

@pytest.fixture
def dados_cadastrais_setup(mocker):
    # 1. Mockamos a Auth para não tentar conectar à API
    mock_auth = mocker.patch("app.integrations.facta.complementares.funcoes_complementares.FactaAuth")
    mock_auth.return_value.base_url = "http://mock-api"
    mock_auth.return_value.get_valid_token.return_value = "TOKEN_FAKE"

    # 2. Mockamos o DataManager (A sua nova dependência)
    mock_data_manager = mocker.patch("app.integrations.facta.complementares.funcoes_complementares.DataManager")
    
    # 3. Mockamos o create_client para simular a resposta da API Facta
    mock_client = mocker.patch("app.integrations.facta.complementares.funcoes_complementares.create_client")

    service = FactaDadosCadastrais()

    return {
        "service": service,
        "data_manager": mock_data_manager.return_value,
        "client_context": mock_client.return_value.__enter__.return_value
    }

def test_formatacao_banco_sucesso(dados_cadastrais_setup):
    """Verifica se o DataManager está sendo chamado corretamente para formatar o banco."""
    setup = dados_cadastrais_setup
    service = setup["service"]
    
    # Configura o Mock do DataManager
    setup["data_manager"].get_nome_banco.return_value = "Banco Teste S.A."
    
    # Configura o retorno da API Facta
    setup["client_context"].get.return_value.status_code = 200
    setup["client_context"].get.return_value.json.return_value = {
        "erro": False,
        "cliente": [{
            "BANCO": "999",
            "AGENCIA": "1234",
            "CONTA": "567891",
            "TIPO_CONTA": "C"
        }]
    }

    resultado = service.buscar_conta_bancaria("12345678900")

    # Verifica se o resultado usou o nome vindo do DataManager
    assert resultado is not None
    assert resultado["banco_nome"] == "Banco Teste S.A."
    assert "Banco Teste S.A." in resultado["texto_formatado"]
    
    # Garante que o método do DataManager foi chamado com o código certo
    setup["data_manager"].get_nome_banco.assert_called_with("999")

def test_banco_sem_dados(dados_cadastrais_setup):
    """Verifica comportamento quando a API não retorna conta."""
    setup = dados_cadastrais_setup
    service = setup["service"]

    setup["client_context"].get.return_value.status_code = 200
    setup["client_context"].get.return_value.json.return_value = {
        "cliente": [{"BANCO": None, "CONTA": None}] # Dados vazios
    }

    resultado = service.buscar_conta_bancaria("12345678900")
    
    assert resultado is None