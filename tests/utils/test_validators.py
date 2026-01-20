import pytest
from app.utils.validators import (
    validate_cpf, 
    clean_digits, 
    formatar_telefone_br,
    formatar_data_br
)
from datetime import datetime

# --- Testes de Lógica Pura ---

def test_validate_cpf():
    assert validate_cpf("52998224725") is True  # Válido
    assert validate_cpf("11111111111") is False # Inválido (dígitos iguais)
    assert validate_cpf("123456") is False      # Tamanho inválido

def test_clean_digits():
    assert clean_digits("123.456-78") == "12345678"

def test_formatar_telefone_br():
    assert formatar_telefone_br("11999999999") == "5511999999999"
    assert formatar_telefone_br("5511999999999") == "5511999999999"
    assert formatar_telefone_br("123") is None

# --- TESTE DA INTEGRAÇÃO COM DATA MANAGER ---

def test_formatar_data_br(mocker):
    """
    Verifica se a função chama o DataManager para pegar o nome do mês.
    """
    # 1. Mock do DataManager onde ele é importado (em validators)
    mock_dm = mocker.patch("app.utils.validators.DataManager")
    
    # Configura para retornar "janeiro" quando chamado
    mock_dm.return_value.get_nome_mes.return_value = "janeiro"
    
    data_exemplo = datetime(2023, 1, 15)
    
    resultado = formatar_data_br(data_exemplo)
    
    assert resultado == "15 de janeiro"
    
    # Garante que passou o número do mês (1) para o DataManager
    mock_dm.return_value.get_nome_mes.assert_called_with(1)