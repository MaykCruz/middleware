import pytest
from app.services.bot.engine import BotEngine

# Fixture para instanciar a Engine com tudo mockado
@pytest.fixture
def engine_setup(mocker):
    # Mock das dependências internas da Engine
    mock_session = mocker.patch("app.services.bot.engine.SessionManager")
    mock_huggy = mocker.patch("app.services.bot.engine.HuggyService")
    mock_fgts = mocker.patch("app.services.bot.engine.FGTSService")
    mock_clt = mocker.patch("app.services.bot.engine.CLTService")
    
    # Mock do Task do Celery para não tentar conectar no Redis real
    mocker.patch("app.services.bot.engine.check_inactivity")

    engine = BotEngine()
    
    # Retorna um dicionário para acessarmos os mocks dentro dos testes
    return {
        "engine": engine,
        "session": mock_session.return_value,
        "huggy": mock_huggy.return_value,
        "clt_service": mock_clt.return_value,
        "fgts_service": mock_fgts.return_value
    }

def test_flow_start_to_menu(engine_setup):
    """Testa: Início -> Menu de Apresentação"""
    setup = engine_setup
    engine = setup["engine"]
    
    # 1. Configura o cenário (Usuário novo)
    setup["session"].get_state.return_value = "START"

    # 2. Executa a ação
    engine.process(chat_id=123, message_text="Oi")

    # 3. Asserts (Verifica se o comportamento esperado ocorreu)
    setup["huggy"].send_message.assert_called_with(123, "menu_bem_vindo")
    setup["session"].set_state.assert_called_with(123, "MENU_APRESENTACAO")

def test_flow_menu_to_clt(engine_setup):
    """Testa: Menu (Opção 1) -> CLT Aguardando CPF"""
    setup = engine_setup
    engine = setup["engine"]
    
    # Cenário: Usuário está no Menu
    setup["session"].get_state.return_value = "MENU_APRESENTACAO"

    # Ação: Digita '1'
    engine.process(chat_id=123, message_text="1")

    # Verifica:
    setup["huggy"].send_message.assert_called_with(123, "pedir_cpf") # Mandou msg certa?
    setup["session"].set_state.assert_called_with(123, "CLT_AGUARDANDO_CPF") # Mudou estado?

def test_flow_menu_to_fgts(engine_setup):
    """Testa: Menu (Opção 2) -> FGTS Aguardando CPF"""
    setup = engine_setup
    engine = setup["engine"]
    
    setup["session"].get_state.return_value = "MENU_APRESENTACAO"

    # Ação: Digita '2'
    engine.process(chat_id=123, message_text="2")

    setup["huggy"].send_message.assert_called_with(123, "pedir_cpf")
    setup["session"].set_state.assert_called_with(123, "FGTS_AGUARDANDO_CPF")

def test_flow_menu_invalid_option(engine_setup):
    """Testa: Menu (Opção Inválida) -> Handoff Humano"""
    setup = engine_setup
    engine = setup["engine"]
    
    setup["session"].get_state.return_value = "MENU_APRESENTACAO"

    # Ação: Digita algo nada a ver
    engine.process(chat_id=123, message_text="batata")

    # Verifica se chamou o fallback
    setup["huggy"].send_message.assert_called_with(123, "atendente_fallback")
    setup["huggy"].start_auto_distribution.assert_called_with(123)
    setup["session"].set_state.assert_called_with(123, "FINISHED")

def test_clt_cpf_valido(engine_setup, mocker):
    """Testa: CLT (CPF Válido) -> Pergunta Tempo de Registro"""
    setup = engine_setup
    engine = setup["engine"]
    
    setup["session"].get_state.return_value = "CLT_AGUARDANDO_CPF"
    
    # Mockamos o validador de CPF para retornar True
    mocker.patch("app.services.bot.engine.validate_cpf", return_value=True)
    mocker.patch("app.services.bot.engine.clean_digits", return_value="12345678900")

    engine.process(chat_id=123, message_text="123.456.789-00")

    setup["huggy"].send_message.assert_called_with(123, "tempo_de_registro")
    setup["session"].set_state.assert_called_with(123, "CLT_AGUARDANDO_TEMPO_REGISTRO")

def test_clt_cpf_invalido_primeira_vez(engine_setup, mocker):
    """Testa: CLT (CPF Inválido) -> Estado de Erro"""
    setup = engine_setup
    engine = setup["engine"]
    
    setup["session"].get_state.return_value = "CLT_AGUARDANDO_CPF"
    mocker.patch("app.services.bot.engine.validate_cpf", return_value=False)

    engine.process(chat_id=123, message_text="00000")

    setup["huggy"].send_message.assert_called_with(123, "cpf_invalido")
    setup["session"].set_state.assert_called_with(123, "CLT_CPF_INVALIDO")

def test_clt_cpf_invalido_segunda_vez(engine_setup, mocker):
    """Testa: CLT (CPF Inválido 2x) -> Transbordo"""
    setup = engine_setup
    engine = setup["engine"]
    
    # Já estava no estado de erro
    setup["session"].get_state.return_value = "CLT_CPF_INVALIDO" 
    mocker.patch("app.services.bot.engine.validate_cpf", return_value=False)

    engine.process(chat_id=123, message_text="00000")

    # Deve desistir e mandar para humano
    setup["huggy"].send_message.assert_called_with(123, "cpf_invalido_fallback", force_internal=True)
    setup["huggy"].start_auto_distribution.assert_called_with(123)
    setup["session"].set_state.assert_called_with(123, "FINISHED")