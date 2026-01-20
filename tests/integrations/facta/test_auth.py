import pytest
from app.integrations.facta.auth import FactaAuth, create_client

def test_create_client_uses_proxy(mocker):
    """Verifica se a função fábrica injeta o proxy corretamente"""
    # Espiona a classe httpx.Client para ver como ela é chamada
    mock_httpx = mocker.patch("app.integrations.facta.auth.httpx.Client")
    
    create_client()
    
    # Verifica se foi chamado com o proxy definido no conftest.py
    mock_httpx.assert_called_with(
        timeout=60.0, 
        proxy="http://user:pass@127.0.0.1:8080"
    )

def test_auth_request_token_success(mocker, mock_token_manager):
    """Simula uma renovação de token com sucesso"""
    # Mock da resposta HTTP
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"token": "NOVO_TOKEN_123"}
    
    # Mock do cliente retornado pelo create_client
    mock_client_instance = mocker.MagicMock()
    mock_client_instance.__enter__.return_value.get.return_value = mock_response
    
    mocker.patch("app.integrations.facta.auth.create_client", return_value=mock_client_instance)

    auth = FactaAuth()
    token = auth.get_valid_token()

    assert token == "NOVO_TOKEN_123"
    mock_token_manager.save_token.assert_called()