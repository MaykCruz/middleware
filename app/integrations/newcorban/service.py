import httpx
import logging
import os
import re
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class NewCorbanService:
    def __init__(self):
        self.url = "https://api.newcorban.com.br/api/propostas/"
        self.user = os.getenv('NEW_USER')
        self.password = os.getenv('NEW_PASSWORD')
        self.empresa = os.getenv('NEW_EMPRESA')
        
        # IDs Fixos (Coloquei como padrão os do seu código, mas ideal é ir pro .env)
        self.promotora_id = os.getenv('NEW_PROMOTORA_ID', "001")
        self.origem_id = os.getenv('NEW_ORIGEM_ID', "3586")
        self.login_digitacao = os.getenv('NEW_LOGIN_DIGITACAO', "96918")
        self.tabela_id = os.getenv('NEW_TABELA_ID', "53244")
        self.vendedor = os.getenv('NEW_VENDEDOR', "12995")
        self.banco_id = 935 # Facta

    def cadastrar_proposta(self, dados_facta: Dict[str, Any], codigo_af: str) -> bool:
        """
        Orquestra a transformação e o envio.
        """
        logger.info(f"🔌 [NewCorban] Preparando envio da AF {codigo_af}...")

        try:
            # 1. Transforma os dados
            payload = self._transformar_dados(dados_facta, codigo_af)
            
            # 2. Envia (Síncrono)
            with httpx.Client(timeout=30.0) as client:
                response = client.post(self.url, json=payload)

                logger.info(f"📥 [NewCorban] HTTP Status: {response.status_code}")
                logger.info(f"📥 [NewCorban] Resposta Body: {response.text}")
                
                if response.status_code in [200, 201]:
                    try:
                        resp_json = response.json()
                        if resp_json.get("error") or resp_json.get("erro"):
                            logger.error(f"❌ [NewCorban] Erro Lógico: {resp_json}")
                            return False
                    except:
                        pass # Não é JSON, segue o jogo

                    return True
                else:
                    logger.error(f"❌ [NewCorban] Erro API: {response.status_code} - {response.text}")
                    return False

        except Exception as e:
            logger.error(f"❌ [NewCorban] Erro Crítico ao integrar: {e}")
            return False

    # --- Helpers de Transformação (Privados) ---

    def _transformar_dados(self, dados_facta: dict, codigo_af: str) -> dict:
        ddd, telefone_sem_ddd = self._extrair_ddd_telefone(dados_facta.get("CELULAR", ""))
        conta, conta_digito = self._separar_conta_digito(dados_facta.get("CONTA", ""))
        
        # Mapeamento seguro de valores nulos
        valor_liquido = dados_facta.get("VALOR_LIQUIDO") or dados_facta.get("valor_liquido") or 0.0

        payload = {
            "auth": {
                "username": self.user,
                "password": self.password,
                "empresa": self.empresa
            },
            "requestType": "createProposta",
            "content": {
                "cliente": {
                    "pessoais": {
                        "cpf": dados_facta.get("CPF"),
                        "nome": dados_facta.get("DESCRICAO") or dados_facta.get("nome"),
                        "nascimento": dados_facta.get("DATANASCIMENTO"),
                        "sexo": self._mapear_sexo(dados_facta.get("SEXO", "")),
                        "estado_civil": dados_facta.get("ESTADOCIVIL", "SOLTEIRO"),
                        "nacionalidade": "BRASILEIRO",
                        "mae": dados_facta.get("NOMEMAE"),
                        "pai": dados_facta.get("NOMEPAI", "NAO INFORMADO"),
                        "renda": 1412,
                        "email": dados_facta.get("EMAIL") or "naoinformado@email.com",
                        "falecido": False,
                        "nao_perturbe": False,
                        "analfabeto": False
                    },
                    "documentos": {
                        "RG": { # Chave fixa conforme seu código, mas cuidado se usar CNH
                            "numero": dados_facta.get("RG"),
                            "tipo": "RG",
                            "data_emissao": dados_facta.get("EMISSAORG"),
                            "uf": dados_facta.get("ESTADORG")
                        }
                    },
                    "enderecos": {
                        "PRINCIPAL": { # Usei uma chave fixa para facilitar
                            "cep": dados_facta.get("CEP"),
                            "logradouro": dados_facta.get("ENDERECO"),
                            "numero": dados_facta.get("NUMERO") or "S/N",
                            "bairro": dados_facta.get("BAIRRO"),
                            "cidade": dados_facta.get("CIDADE"),
                            "estado": self._mapear_uf_para_estado(dados_facta.get("ESTADO", "")),
                            "uf": dados_facta.get("ESTADO"),
                            "complemento": dados_facta.get("COMPLEMENTO", "")
                        }
                    },
                    "telefones": {
                        "PRINCIPAL": {
                            "ddd": ddd,
                            "numero": telefone_sem_ddd
                        }
                    }
                },
                "proposta": {
                    "documento_id": dados_facta.get("RG"),
                    "endereco_id": dados_facta.get("CEP"), 
                    "telefone_id": telefone_sem_ddd,
                    "banco_averbacao": dados_facta.get("BANCO"),
                    "agencia": dados_facta.get("AGENCIA"),
                    "conta": conta,
                    "conta_digito": conta_digito,
                    "tipo_liberacao": self._mapear_tipo_conta(dados_facta.get("TIPO_CONTA", "")),
                    "proposta_id_banco": codigo_af, # AQUI ENTRA O AF
                    "promotora_id": self.promotora_id,
                    "origem_id": self.origem_id,
                    "login_digitacao": self.login_digitacao,
                    "tabela_id": self.tabela_id,
                    "vendedor": self.vendedor,
                    "valor_financiado": valor_liquido, # Ajuste se precisar do Bruto aqui
                    "valor_liberado": valor_liquido,
                    "prazo": 5, # Seu código estava fixo em 5
                    "taxa": "1.80",
                    "banco_id": self.banco_id,
                    "convenio_id": "100000",
                    "produto_id": "7",
                    "status": 0,
                    "tipo_cadastro": "API",
                    "proposta_id": False,
                    "valor_parcela": 0,
                    "link_formalizacao": dados_facta.get("link_formalizacao", "")
                }
            }
        }
        return payload

    def _mapear_sexo(self, sigla: str) -> str:
        if str(sigla).upper() == 'F': return 'FEMININO'
        if str(sigla).upper() == 'M': return 'MASCULINO'
        return 'MASCULINO' # Default seguro

    def _mapear_tipo_conta(self, sigla: str) -> str:
        if str(sigla).upper() == 'P': return 'CONTA_POUPANCA'
        return 'CONTA_CORRENTE'

    def _extrair_ddd_telefone(self, telefone_completo: str) -> tuple:
        if not telefone_completo: return "", ""
        numeros = re.sub(r'\D', '', str(telefone_completo))
        # Tratamento do seu código original
        if len(numeros) == 12 and numeros.startswith('0'):
            return numeros[1:3], numeros[3:]
        if len(numeros) >= 10:
            return numeros[0:2], numeros[2:]
        return "", numeros

    def _separar_conta_digito(self, conta_completa: str) -> tuple:
        if not conta_completa or len(str(conta_completa)) < 2:
            return str(conta_completa), ""
        c = str(conta_completa)
        return c[:-1], c[-1]

    def _mapear_uf_para_estado(self, uf: str) -> str:
        estados = {
            'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas',
            'BA': 'Bahia', 'CE': 'Ceará', 'DF': 'Distrito Federal', 'ES': 'Espírito Santo',
            'GO': 'Goiás', 'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul',
            'MG': 'Minas Gerais', 'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná',
            'PE': 'Pernambuco', 'PI': 'Piauí', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte',
            'RS': 'Rio Grande do Sul', 'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina',
            'SP': 'São Paulo', 'SE': 'Sergipe', 'TO': 'Tocantins'
        }
        return estados.get(str(uf).upper(), '')