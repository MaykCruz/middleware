import pytest
import os
import json
import logging
from dotenv import load_dotenv

# Carrega variáveis de ambiente (User/Senha)
load_dotenv()

from app.integrations.newcorban.service import NewCorbanService

# Configura Logger para ver os prints
logger = logging.getLogger(__name__)

# CPF que sabemos que tem dados (do seu teste anterior)
CPF_TESTE = "044.507.199-05" 

@pytest.mark.skipif(not os.getenv('NEW_USER'), reason="Sem credenciais")
def test_fluxo_completo_newcorban_service(caplog):
    caplog.set_level(logging.INFO)
    
    print(f"\n\n🧪 [TESTE SERVICE] Iniciando consulta normalizada para: {CPF_TESTE}")
    print("=" * 60)

    # 1. Instancia o Service
    try:
        service = NewCorbanService()
        print("✅ Service instanciado com sucesso.")
    except Exception as e:
        pytest.fail(f"❌ Falha ao instanciar NewCorbanService: {e}")

    # 2. Chama o método principal (que será usado pelo BankAccountService)
    print("\n🔄 Chamando consultar_conta_fallback()...")
    resultado = service.consultar_conta_fallback(CPF_TESTE)

    # 3. Análise do Resultado
    if resultado:
        print("\n✅ RETORNO DO SERVICE (Padronizado):")
        print("-" * 30)
        print(json.dumps(resultado, indent=2, ensure_ascii=False))
        print("-" * 30)

        raw = resultado.get("raw", {})
        texto = resultado.get("texto_formatado", "")

        # --- Validações Críticas (O que não pode falhar) ---
        
        # A) Tem que ter a flag de origem e tipo
        assert raw.get("origem") == "newcorban"
        assert raw.get("tipo_dado") in ["CONTA", "PIX"]
        
        # B) Se for CONTA, tem que ter BANCO preenchido (não pode ser None)
        if raw.get("tipo_dado") == "CONTA":
            banco = raw.get("BANCO")
            print(f"\n🧐 Verificação de Conta Bancária:")
            print(f"   -> Banco Código: {banco}")
            print(f"   -> Agência: {raw.get('AGENCIA')}")
            print(f"   -> Conta Full: {raw.get('CONTA')}")
            
            if banco is None:
                print("\n⚠️  ALERTA: O campo 'BANCO' veio None!")
                print("   Possível causa: A chave no _normalizar_dados está 'banco_averba' mas a API manda 'banco_averbacao'.")
                pytest.fail("Falha na normalização: Código do Banco não foi capturado.")
            else:
                print("   ✅ Código do banco capturado corretamente.")

        # C) Se for PIX, tem que ter chave
        elif raw.get("tipo_dado") == "PIX":
            print(f"\n🧐 Verificação de PIX:")
            print(f"   -> Chave: {raw.get('chave_pix')}")
            assert raw.get("chave_pix") is not None

        # D) Texto formatado deve existir
        print(f"\n💬 Texto Formatado (Para o Usuário):")
        print(f'"{texto}"')
        assert len(texto) > 5, "Texto formatado está muito curto ou vazio."

    else:
        print("\n❌ O Service retornou None. Verifique se o CPF possui dados no NewCorban.")
        pytest.fail("Service retornou None para um CPF que deveria ter dados.")