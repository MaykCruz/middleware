import logging
from typing import Optional, Dict, Any
from app.integrations.facta.auth import FactaAuth, create_client
from app.services.data_manager import DataManager

logger = logging.getLogger(__name__)

class FactaDadosCadastrais:
    def __init__(self):
        self.auth = FactaAuth()
        self.base_url = self.auth.base_url
        self.data_manager = DataManager()
    
    @property
    def _get_headers(self):
        token = self.auth.get_valid_token()
        return {"Authorization": f"Bearer {token}"}
    
    def consultar_dados_completos(self, cpf: str) -> Optional[Dict[str, Any]]:
        """
        Realiza a requisição na API Facta e retorna o JSON bruto do cliente.
        Este método serve de base para todas as outras extrações.
        """
        url = f"{self.base_url}/proposta/consulta-cliente"
        params = {"cpf": cpf}

        try:
            with create_client() as client:
                logger.info(f"🔎 [Facta] Consultando dados cadastrais completos para CPF {cpf}...")
                response = client.get(url, headers=self._get_headers, params=params)

                if response.status_code != 200:
                    logger.warning(f"⚠️ [Facta] Erro API Consulta: {response.status_code}")
                    return None
                
                data = response.json()

                if data.get("erro") is True:
                    return None
                
                cliente_lista = data.get("cliente", [])
                if not cliente_lista:
                    return None
                
                return cliente_lista[0]
        
        except Exception as e:
            logger.error(f"❌ [Facta] Falha ao consultar dados cadastrais: {e}")
            return None
    
    def buscar_conta_bancaria(self, cpf: str) -> Optional[Dict[Dict, str]]:
        """
        Usa a consulta completa para extrair e formatar apenas dados bancários.
        """
        dados = self.consultar_dados_completos(cpf)

        if not dados:
            return None
        
        banco = dados.get("BANCO")
        agencia = dados.get("AGENCIA") 
        conta = dados.get("CONTA")
        tipo = dados.get("TIPO_CONTA", "")

        if not banco or not conta:
            logger.info(f"ℹ️ [Facta] Cliente encontrado, mas sem dados bancários completos.")
            return None
        
        texto_completo = self._formatar_dados_bancarios(banco, agencia, conta, tipo)
        
        return {
            "raw": {
                "origem": "facta",
                "tipo_dado": "CONTA",
                
                "BANCO": banco,
                "AGENCIA": agencia,
                "CONTA": conta,
                "TIPO_CONTA": tipo,

                "chave_pix": None,
                "tipo_chave_pix": None,
                "codigo_tipo_chave_pix": 0
            },
            "texto_formatado": texto_completo,
            "origem": "facta"
        }

    def _formatar_dados_bancarios(self, banco: str, agencia: str, conta: str, tipo: str) -> str:
        """
        Formata os dados brutos para exibição amigável.
        """
        if not banco or not conta:
            return ""
        
        banco_formatado = self.data_manager.get_nome_banco(str(banco))
        if not banco_formatado:
            banco_formatado = f"Banco {banco}"

        agencia_formatada = str(agencia).zfill(4) if agencia else "Sem Agência"

        conta_limpa = str(conta).strip()
        if len(conta_limpa) >= 2 and "-" not in conta_limpa:
            conta_sem_dv = conta_limpa[:-1]
            digito = conta_limpa[-1]
            conta_formatada = f"{conta_sem_dv}-{digito}"
        else:
            conta_formatada = conta_limpa

        tipo_upper = str(tipo).upper()
        tipo_desc = "poupança" if "P" in tipo_upper else "corrente"
        
        texto = f"{banco_formatado}\nAgência: {agencia_formatada}\nConta {tipo_desc}: {conta_formatada}"
        return texto
