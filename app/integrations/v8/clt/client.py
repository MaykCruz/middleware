import os
import logging
import httpx
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class V8CLTAdapter:
    def __init__(self, http_client: httpx.Client):
        self.client = http_client
        self.base_url = os.getenv("V8_BFF_URL")
    
    def buscar_consulta_existente(self, cpf: str) -> Optional[Dict[str, Any]]:
        logger.info(f"🔍 [V8 CLT] Buscando histórico de consultas para o CPF {cpf}...")
        endpoint = f"{self.base_url}/private-consignment/consult"

        params = {
            "search": cpf,
            "limit": 50,
            "page": 1,
            "provider": "QI"
        }

        try:
            response = self.client.get(endpoint, params=params)
            response.raise_for_status()
            dados = response.json()

            logger.info(f"🐛 [DEBUG V8] Retorno bruto da busca: {dados}")

            registros = dados.get("data", [])
            if not registros:
                logger.info(f"⚪ [V8 CLT] Nenhuma consulta prévia encontrada para o CPF {cpf}.")
                return None
            
            ultima_consulta = registros[0]
            logger.info(f"📋 [V8 CLT] Consulta anterior encontrada! ID: {ultima_consulta.get('id')} | Status: {ultima_consulta.get('status')}")
            return ultima_consulta
        
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ [V8 CLT] Erro HTTP ao buscar consultas ({e.response.status_code}): {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"❌ [V8 CLT] Erro inesperado ao buscar consultas para {cpf}: {str(e)}")
            return None
    
    def criar_termo_consulta(self, cpf: str) -> Optional[str]:
        logger.info(f"📝 [V8 CLT] Gerando novo termo de consentimento para {cpf} (com dados genéricos)...")
        endpoint = f"{self.base_url}/private-consignment/consult"

        payload = {
            "borrowerDocumentNumber": cpf,
            "gender": "male", 
            "birthDate": "1990-01-01", # Data genérica válida
            "signerName": "Cliente Consulta", # Nome genérico
            "signerEmail": "cliente@sememail.com",
            "signerPhone": {
                "countryCode": "55",
                "areaCode": "11",
                "phoneNumber": "999999999"
            },
            "provider": "QI"
        }

        try:
            response = self.client.post(endpoint, json=payload)
            response.raise_for_status()
            consult_id = response.json().get("id")

            logger.info(f"✅ [V8 CLT] Termo gerado com sucesso! Consult ID: {consult_id}")
            return consult_id

        except httpx.HTTPStatusError as e:
            logger.error(f"❌ [V8 CLT] Erro HTTP ao gerar termo ({e.response.status_code}): {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"❌ [V8 CLT] Erro de rede ao gerar termo para {cpf}: {str(e)}")
            return None
    
    def autorizar_termo(self, consult_id: str) -> bool:
        logger.info(f"👍 [V8 CLT] Enviando auto-aceite para o termo {consult_id}...")
        endpoint = f"{self.base_url}/private-consignment/consult/{consult_id}/authorize"

        try:
            response = self.client.post(endpoint, json={}, timeout=120.0)
            response.raise_for_status()
            logger.info(f"🚀 [V8 CLT] Termo {consult_id} autorizado! Aguardando webhook do Dataprev...")
            return True
        
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 400:
                try:
                    erro_dados = e.response.json()
                    if erro_dados.get("type") == "consult_already_approved":
                        logger.info(f"✅ [V8 CLT] Termo {consult_id} já constava como autorizado na V8.")
                        return True
                except Exception:
                    pass
    
            logger.error(f"❌ [V8 CLT] Falha ao autorizar termo ({e.response.status_code}): {e.response.text}")
            return False
    
    def buscar_tabelas(self, consult_id: str) -> Optional[list]:
        logger.info(f"📊 [V8 CLT] Buscando tabelas disponíveis para a consulta {consult_id}...")
        endpoint = f"{self.base_url}/private-consignment/simulation/configs"
        params = {"consultId": consult_id}

        try:
            response = self.client.get(endpoint, params=params)
            response.raise_for_status()
            dados = response.json()
            tabelas = dados.get("configs", [])
            
            if not tabelas:
                logger.warning(f"⚠️ [V8 CLT] Nenhuma tabela retornada para a consulta {consult_id}.")
                return None
                
            return tabelas

        except httpx.HTTPStatusError as e:
            logger.error(f"❌ [V8 CLT] Erro HTTP ao buscar tabelas ({e.response.status_code}): {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"❌ [V8 CLT] Erro ao buscar tabelas: {str(e)}")
            return None
        
    def simular_operacao(self, consult_id: str, table_id: str, valor_parcela: float, quantidade_maxima_parcelas: int) -> Optional[Dict[str, Any]]:
        logger.info(f"🧮 [V8 CLT] Gerando simulação (Tabela: {table_id} | Parcela: {valor_parcela})...")
        endpoint = f"{self.base_url}/private-consignment/simulation"
        payload = {
            "consult_id": consult_id,
            "config_id": table_id,
            "installment_face_value": valor_parcela,
            "number_of_installments": quantidade_maxima_parcelas,
            "provider": "QI"
        }

        try:
            response = self.client.post(endpoint, json=payload)
            response.raise_for_status()
            simulacao = response.json()

            logger.info(f"✅ [V8 CLT] Simulação gerada com sucesso! ID Simulação: {simulacao.get('id_simulation')}")
            return simulacao

        except httpx.HTTPStatusError as e:
            logger.error(f"❌ [V8 CLT] Erro HTTP ao simular operação ({e.response.status_code}): {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"❌ [V8 CLT] Erro de rede ao simular operação: {str(e)}")
            return None
    
    def buscar_detalhes_consulta(self, consult_id: str) -> Optional[Dict[str, Any]]:
        logger.info(f"🔍 [V8 CLT] Buscando detalhes completos da consulta ID {consult_id}...")
        endpoint = f"{self.base_url}/private-consignment/consult/{consult_id}"

        try:
            response = self.client.get(endpoint)
            response.raise_for_status()
            return response.json()
        
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ [V8 CLT] Erro HTTP ao buscar detalhes ({e.response.status_code}): {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"❌ [V8 CLT] Erro inesperado ao buscar detalhes para {consult_id}: {str(e)}")
            return None
