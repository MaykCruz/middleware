import logging
import httpx
from typing import Dict, Any
from app.integrations.facta.auth import FactaAuth

logger = logging.getLogger(__name__)

class FactaContratoAndamentoError(Exception):
    """Levantada quando a API recusa por contrato já em andamento."""
    pass

class FactaProposalClient:
    """
    Cliente especializado na esteira de digitação.
    Responsável apenas pelo transporte HTTP (POST/GET) para os endpoints de proposta.
    """
    def __init__(self, http_client: httpx.Client):
        self.auth = FactaAuth()
        self.base_url = self.auth.base_url
        self.http_client = http_client
    
    @property
    def _get_headers(self):
        token = self.auth.get_valid_token()
        return {"Authorization": f"Bearer {token}",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
    
    def _post_request(self, endpoint: str, payload: Dict[str, Any], use_json: bool = False) -> Dict[str, Any]:
        """
        Wrapper centralizado para requisições POST com tratamento de erro padrão.
        """
        url = f"{self.base_url}{endpoint}"

        try:
            logger.info(f"📝 [Proposal Client] POST {endpoint} | Payload keys: {list(payload.keys())}")

            if use_json:
                resp = self.http_client.post(url, headers=self._get_headers, json=payload)
            else:
                clean_payload = {k: v for k, v in payload.items() if v is not None}
                resp = self.http_client.post(url, headers=self._get_headers, data=clean_payload)
            
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, dict) and data.get("erro") is True:
                msg = str(data.get("mensagem") or data.get("msg") or "Erro desconhecido").lower()

                if "contrato em andamento" in msg:
                    logger.warning(f"⚠️ [Proposal Client] Bloqueio de Negócio: {msg}")
                    raise FactaContratoAndamentoError(msg)
                
                logger.error(f"❌ [Proposal Client] Erro de Negócio em {endpoint}: {msg}")
                raise ValueError(f"Facta Error: {msg}")
            
            return data
        
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ [Proposal Client] Erro HTTP {e.response.status_code} em {endpoint}: {e.response.text}")
            raise e
        except Exception as e:
            logger.error(f"❌ [Proposal Client] Falha técnica em {endpoint}: {str(e)}")
            raise e
    
    def registrar_etapa_1_simulacao(self, payload: Dict[str, Any]) -> int:
        """
        Endpoint: /proposta/etapa1-simulador
        Retorna: id_simulador (int)
        """
        resp = self._post_request("/proposta/etapa1-simulador", payload, use_json=False)
        return int(resp.get("id_simulador"))

    def registrar_etapa_2_dados_pessoais(self, payload: Dict[str, Any]) -> int:
        """
        Endpoint: /proposta/etapa2-dados-pessoais
        Retorna: codigo_cliente (int)
        """
        resp = self._post_request("/proposta/etapa2-dados-pessoais", payload, use_json=False)
        return int(resp.get("codigo_cliente"))
    
    def registrar_etapa_3_efetivacao(self, codigo_cliente: int, id_simulador: int) -> str:
        """
        Endpoint: /proposta/etapa3-proposta-cadastro
        Vincula cliente ao simulador e gera a proposta.
        Retorna: codigo_af (ID da proposta final)
        """
        payload = {
            "codigo_cliente": codigo_cliente,
            "id_simulador": id_simulador,
            "tipo_formalizacao": "DIG"
        }
        resp = self._post_request("/proposta/etapa3-proposta-cadastro", payload, use_json=False)
        return {
            "codigo": str(resp.get("codigo")),
            "mensagem": resp.get("mensagem"),
            "url_formalizacao": resp.get("url_formalizacao")
        }