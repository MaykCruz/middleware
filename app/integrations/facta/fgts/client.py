import logging
import httpx
from app.integrations.facta.auth import FactaAuth
from app.utils.formatters import parse_valor_monetario

logger = logging.getLogger(__name__)

class FactaFGTSAdapter:
    def __init__(self, http_client: httpx.Client):
        self.auth = FactaAuth()
        self.base_url = self.auth.base_url
        self.http_client = http_client

    @property
    def _get_headers(self):
        token = self.auth.get_valid_token()
        return {"Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
    
    def consultar_saldo(self, cpf: str) -> dict:
        url = f"{self.base_url}/fgts/saldo"
        params = {"cpf": cpf, "banco": "facta"}

        try:
            logger.info(f"💰 [Facta] Consultando saldo para CPF {cpf}...")
            response = self.http_client.get(url, headers=self._get_headers, params=params)
            data = response.json()

            status = self._interpretar_retorno(data)
    
            return {
                "status": status,
                "dados": data.get("retorno", {}),
                "msg_original": data.get("mensagem", "")
            }
        
        except httpx.TimeoutException as e:
            logger.error(f"⏳ [Facta CLT] Timeout na API (Demorou muito para responder): {e}")
            raise e
        
        except Exception as e:
            logger.error(f"Erro Saldo: {e}")
            return {"status": "ERRO_TECNICO", "msg_original": str(e)}

    def simular_calculo(self, cpf: str, dados_saldo: dict) -> dict:
        parcelas = self._organizar_parcelas(dados_saldo)

        saldo_bruto = parse_valor_monetario(dados_saldo.get("saldo_total", 0))

        info_tabela = self._selecionar_melhor_tabela(saldo_bruto)

        logger.info(f"🧮 [Facta] Simulando tabela '{info_tabela['nome']}' (Cód {info_tabela['codigo']}) para saldo {saldo_bruto}")

        url = f"{self.base_url}/fgts/calculo"

        body = {
            "cpf": cpf.replace(".", "").replace("-", ""),
            "taxa": info_tabela["taxa"],
            "tabela": info_tabela["codigo"],
            "parcelas": parcelas
        }

        try:
            resp = self.http_client.post(url, headers=self._get_headers, json=body)
            data = resp.json()
            
            # Verifica se aprovou
            if data.get("permitido", "").upper() == "SIM":
                return {
                    "status": "APROVADO",
                    "valor_liquido": parse_valor_monetario(data.get("valor_liquido")),
                    "raw": data
                }
            else:
                return {
                    "status": "REPROVADO", 
                    "msg_original": data.get("msg")
                }
        except httpx.TimeoutException as e:
            logger.error(f"⏳ [Facta CLT] Timeout na API (Demorou muito para responder): {e}")
            raise e
        
        except Exception as e:
            return {"status": "ERRO_TECNICO", "msg_original": str(e)}
        
    def _interpretar_retorno(self, data: dict) -> str:
        """
        Traduz os códigos de erros da Facta para nossos status internos.
        """
        if not data.get("erro"):
            return "SUCESSO"
    
        code = data.get("codigo")
        msg = data.get("mensagem", "").lower()

        termos_retry = ["volte em"]

        if any(termo in msg for termo in termos_retry):
            return "PROCESSAMENTO_PENDENTE"

        # Sem autorização
        if code == 7 or "instituição fiduciária não possui autorização do trabalhador" in msg:
            return "SEM_AUTORIZACAO"

        # Sem adesão
        if code == 9 or "trabalhador não possui adesão ao saque aniversário vigente" in msg: 
            return "SEM_ADESAO"

        # Mudanças cadastrais
        if code == 35 or "mudanças cadastrais na conta do fgts foram realizadas, que impedem a contratação" in msg: 
            return "MUDANCAS_CADASTRAIS"

        # Aniversariante
        termos_aniversariante = [
            "existe uma operação fiduciária em andamento",
            "operação não permitida antes de"
        ]

        if code in [5, 10] or any(termo in msg for termo in termos_aniversariante): 
            return "ANIVERSARIANTE"
        
        # Saldo não encontrado
        if "saldo não encontrado." in msg:
            return "SALDO_NAO_ENCONTRADO"

        # Sem saldo
        if "cliente não possui saldo fgts" in msg:
            return "SEM_SALDO"
        
        if "limite mensal de consultas fgts excedido" in msg:
            return "LIMITE_EXCEDIDO_CONSULTAS_FGTS"
            
        return "RETORNO_DESCONHECIDO"
    
    def _organizar_parcelas(self, retorno_saldo: dict) -> list:
        parcelas = []
        encontrou_valida = False
        zerar = False

        for i in range(1, 6):
            data = retorno_saldo.get(f'dataRepasse_{i}')
            val_bruto = retorno_saldo.get(f'valor_{i}')

            if not data: break

            valor = parse_valor_monetario(val_bruto)

            if not encontrou_valida:
                if valor >= 100: encontrou_valida = True
            else:
                if valor < 100: zerar = True

            if zerar and encontrou_valida: valor = 0.0

            parcelas.append({f"dataRepasse_{i}": data, f"valor_{i}": valor})
            
        return parcelas
    
    def _selecionar_melhor_tabela(self, saldo_total: float) -> dict:
        """
        Define a melhor tabela e taxa com base no saldo do cliente.
        Retorna um dicionário com parâmetros para a API.
        """

        return {
            "codigo": 62170,
            "taxa": 1.80,
            "nome": "Gold Preference"
        }