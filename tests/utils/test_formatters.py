import pytest
from datetime import datetime
from app.utils.formatters import (
    parse_valor_monetario, 
    formatar_moeda, 
    limpar_nome,
    formatar_display_tempo,
    obter_mes_inicio_desconto
)

# --- Testes Simples (Sem Mock) ---

def test_parse_valor_monetario():
    assert parse_valor_monetario("R$ 1.200,50") == 1200.50
    assert parse_valor_monetario(1200.50) == 1200.50
    assert parse_valor_monetario(None) == 0.0

def test_formatar_moeda():
    assert formatar_moeda(1200.50) == "1.200,50"
    assert formatar_moeda(None) == None

def test_limpar_nome():
    assert limpar_nome("João da Silva!") == "João da Silva"

def test_formatar_display_tempo():
    # Teste básico para garantir que não quebra
    assert formatar_display_tempo(None) == "Data n/d"
    assert formatar_display_tempo("Texto Invalido") == "Texto Invalido"

# --- TESTES COM MOCK DE DATA E DATA MANAGER ---

def test_obter_mes_inicio_desconto_antes_dia_20(mocker):
    """
    Cenário: Hoje é dia 15/01. 
    Regra: Soma 2 meses -> Mês 3 (Março).
    """
    # 1. Mock do DataManager para retornar o nome do mês
    mock_dm = mocker.patch("app.utils.formatters.DataManager")
    mock_dm.return_value.get_nome_mes.return_value = "março"

    # 2. Mock do datetime APENAS neste módulo
    # Isso substitui a classe 'datetime' importada dentro de formatters.py
    mock_dt = mocker.patch("app.utils.formatters.datetime")
    
    # 3. Configuramos o .now() para retornar uma DATA REAL fixa
    mock_dt.now.return_value = datetime(2023, 1, 15)

    # Executa a função
    resultado = obter_mes_inicio_desconto()
    
    assert resultado == "março"
    
    # Verifica se chamou o DataManager pedindo o mês 3 (Janeiro + 2)
    mock_dm.return_value.get_nome_mes.assert_called_with(3)

def test_obter_mes_inicio_desconto_depois_dia_20(mocker):
    """
    Cenário: Hoje é dia 21/01.
    Regra: Soma 3 meses -> Mês 4 (Abril).
    """
    mock_dm = mocker.patch("app.utils.formatters.DataManager")
    mock_dm.return_value.get_nome_mes.return_value = "abril"

    mock_dt = mocker.patch("app.utils.formatters.datetime")
    # Simulamos dia 21
    mock_dt.now.return_value = datetime(2023, 1, 21)

    resultado = obter_mes_inicio_desconto()
    
    assert resultado == "abril"
    
    # Verifica se chamou o DataManager pedindo o mês 4 (Janeiro + 3)
    mock_dm.return_value.get_nome_mes.assert_called_with(4)