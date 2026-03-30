import os
import logging
import httpx
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(message)s")
load_dotenv()

# O nosso truque para o Redis no Windows
os.environ["CELERY_RESULT_BACKEND"] = "redis://127.0.0.1:6379/0"

from app.integrations.v8.auth import V8Auth
from app.integrations.v8.clt.client import V8CLTAdapter

def testar_autorizacao_termo():
    print("="*50)
    print("🚀 A INICIAR TESTE DO CLIENT V8: autorizar_termo")
    print("="*50)
    
    try:
        print("\n🔑 1. A autenticar e a buscar o Token...")
        auth = V8Auth()
        token = auth.get_valid_token()
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        with httpx.Client(headers=headers, timeout=30.0) as http_client:
            v8_adapter = V8CLTAdapter(http_client)
            
            # O ID exato que acabaste de gerar!
            consult_id = "2a561a45-a3aa-41f4-a4e0-60835e445616"
            
            print(f"\n👍 2. A enviar o Auto-Aceite para o Consult ID: {consult_id}...")
            sucesso = v8_adapter.autorizar_termo(consult_id)
            
            print("\n" + "="*50)
            if sucesso:
                print("✅ SUCESSO! TERMO AUTORIZADO!")
                print("👉 A V8 já engatilhou a consulta no Dataprev.")
                print("👉 O próximo passo será receber a notificação no Webhook.")
            else:
                print("❌ Falha na autorização. Verifica os logs acima.")
            print("="*50)

    except Exception as e:
        print("\n" + "="*50)
        print(f"❌ OCORREU UM ERRO: {str(e)}")
        print("="*50)

if __name__ == "__main__":
    testar_autorizacao_termo()