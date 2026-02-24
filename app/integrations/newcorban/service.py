import httpx
import logging
import os
import re
from typing import Optional, Dict, Any, List
from app.integrations.newcorban.client import NewCorbanClient
from app.services.data_manager import DataManager
from app.utils.formatters import formatar_cpf, formatar_telefone

logger = logging.getLogger(__name__)

class NewCorbanService:
    """
    Service Global do NewCorban.
    Orquestra as chamadas ao Client e aplica regras de negócio (transformação de dados).
    """
    def __init__(self):
        self.client = NewCorbanClient()
        self.data_manager = DataManager()

        self.promotora_id = os.getenv('NEW_PROMOTORA_ID', "001")
        self.origem_id = os.getenv('NEW_ORIGEM_ID', "3586")
        self.login_digitacao = os.getenv('NEW_LOGIN_DIGITACAO', "96918")
        self.tabela_id = os.getenv('NEW_TABELA_ID', "53244")
        self.vendedor = os.getenv('NEW_VENDEDOR', "12995")
        self.banco_id = 935 # Facta
        
    def consultar_conta_fallback(self, cpf: str) -> Optional[Dict[str, Any]]:
        """
        Busca histórico bancário e retorna o registro mais recente (Conta ou PIX).
        Retorna None se nada for encontrado.
        """
        logger.info(f"🔎 [NewCorban Service] Iniciando fallback de conta para {cpf}")

        historico_bruto = self.client.get_bank_account_history(cpf)

        if not historico_bruto or not isinstance(historico_bruto, list):
            return None
        
        melhor_registro = self._filtrar_mais_recente(historico_bruto)

        if melhor_registro:
            dados_raw = self._normalizar_dados(melhor_registro, cpf_cliente=cpf)
            
            texto = self._formatar_saida_usuario(dados_raw)

            return {
                "raw": dados_raw,
                "texto_formatado": texto,
                "origem": "newcorban"
            }

        return None

    def cadastrar_proposta(self, dados_facta: Dict[str, Any], codigo_af: str) -> bool:
        """
        Transforma os dados da Facta no payload do NewCorban e envia.
        """
        logger.info(f"🔌 [NewCorban] Preparando envio da AF {codigo_af}...")

        payload = self._transformar_dados(dados_facta, codigo_af)
        resultado = self.client.create_proposal(payload)

        if resultado["success"]:
            logger.info(f"✅ [NewCorban] Proposta enviada com sucesso.")
            return True
           
        else:
            logger.error(f"❌ [NewCorban] Falha no envio: {resultado.get('response_text')}")
            return False

    # --- Helpers de Transformação (Privados) ---

    def _normalizar_dados(self, dados_new: Dict[str, Any], cpf_cliente: str = "") -> Dict[str, Any]:
        """
        Transforma o JSON do NewCorban num dicionário 'raw' igual ao da Facta.
        """
        tipo_liberacao = str(dados_new.get("tipo_liberacao", "")).upper()

        if tipo_liberacao == "PIX":
            chave_pix = dados_new.get("pix")

            tipo_chave_detectado = self._identificar_tipo_chave_pix(chave_pix, cpf_cliente)

            chave_limpa = self._sanitizar_valor_pix(chave_pix, tipo_chave_detectado)

            mapa_codigos_facta = {
                "CPF": 1,
                "TELEFONE": 2,
                "EMAIL": 3,
                "ALEATORIA": 4
            }

            codigo_pix = mapa_codigos_facta.get(tipo_chave_detectado, 0)

            return {
                "tipo_dado": "PIX",
                "origem": "newcorban",
                "chave_pix": chave_limpa,
                "tipo_chave_pix": tipo_chave_detectado,
                "codigo_tipo_chave_pix": codigo_pix,

                "BANCO": None,
                "AGENCIA": None,
                "CONTA": None,
                "TIPO_CONTA": None
            }
        
        conta_sem_digito = str(dados_new.get("conta") or "")
        digito = str(dados_new.get("conta_digito") or "")
        conta_full = f"{conta_sem_digito}{digito}" if conta_sem_digito else None

        tipo_conta_sigla = "P" if "POUPANCA" in tipo_liberacao else "C"

        return {
            "tipo_dado": "CONTA",
            "origem": "newcorban",
            "chave_pix": None,
            "BANCO": dados_new.get("banco_averbacao"),
            "AGENCIA": dados_new.get("agencia"),
            "CONTA": conta_full,
            "TIPO_CONTA": tipo_conta_sigla
        }
    
    def _sanitizar_valor_pix(self, chave: str, tipo: str) -> str:
        """
        Método especialista: Remove sujeira (+55, pontos, traços) baseado no tipo.
        """
        if not chave: return ""

        chave_str = str(chave).strip()

        if tipo in ["CPF"]:
            return re.sub(r'\D', '', chave_str)
        
        if tipo == "TELEFONE":
            # Remove não-dígitos
            nums = re.sub(r'\D', '', chave_str)
            # Regra do DDI (+55) se sobrar (Ex: 55119... -> 119...)
            if nums.startswith('55') and len(nums) >= 12:
                return nums[2:]
            return nums
        
        # Email e Aleatória não devem ser tocados
        return chave_str
    
    def _identificar_tipo_chave_pix(self, chave: str, cpf_cliente: str) -> str:
        """
        Analisa a string da chave PIX para determinar seu tipo.
        Retorna: Retorna: 'EMAIL', 'ALEATORIA', 'CPF', 'CNPJ', 'TELEFONE
        """
        if not chave:
            return "DESCONHECIDO"
        
        chave_limpa = str(chave).strip()

        if re.match(r"[^@]+@[^@]+\.[^@]+", chave_limpa):
            return "EMAIL"
        
        if re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", chave_limpa, re.IGNORECASE):
            return "ALEATORIA"
        
        apenas_numeros = re.sub(r'\D', '', chave_limpa)
        cpf_cliente_limpo = re.sub(r'\D', '', str(cpf_cliente))

        if apenas_numeros == cpf_cliente_limpo:
            return "CPF"
        
        if len(apenas_numeros) in [10, 11]:
            return "TELEFONE"
        
        if len(apenas_numeros) in [12, 13] and apenas_numeros.startswith("55"):
            return "TELEFONE"
        
        return "DESCONHECIDO"
    
    def _formatar_saida_usuario(self, dados_raw: Dict[str, Any]) -> str:
        """
        Gera o texto amigável para exibir no Chat.
        Retorna: texto_formatado
        """
        if dados_raw.get("tipo_dado") == "PIX":
            chave = dados_raw.get("chave_pix")
            tipo_chave = dados_raw.get("tipo_chave_pix")

            chave_formatada = chave
            
            if tipo_chave == "CPF":
                chave_formatada = formatar_cpf(chave)
                print(f"   -> Tentou formatar CPF. Resultado: '{chave_formatada}'")

            elif tipo_chave == "TELEFONE":
                chave_formatada = formatar_telefone(chave)
                print(f"   -> Tentou formatar TELEFONE. Resultado: '{chave_formatada}'")

            return f"Chave PIX: {chave_formatada}"
        
        banco = dados_raw.get("BANCO")
        agencia = dados_raw.get("AGENCIA")
        conta = dados_raw.get("CONTA")
        tipo = dados_raw.get("TIPO_CONTA")

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
        
        tipo_desc = "poupança" if str(tipo).upper() == "P" else "corrente"

        texto = f"{banco_formatado}\nAgência: {agencia_formatada}\nConta {tipo_desc}: {conta_formatada}"
        
        return texto

    def _filtrar_mais_recente(self, historico: List[Dict[str, Any]]) -> Dict[str, Any]:
        def get_date(item):
            return item.get("data_cadastro") or "0000-00-00 00:00:00"
        
        historico_ordenado = sorted(historico, key=get_date, reverse=True)
        return historico_ordenado[0]

    def _transformar_dados(self, dados_facta: dict, codigo_af: str) -> dict:
        ddd, telefone_sem_ddd = self._extrair_ddd_telefone(dados_facta.get("CELULAR", ""))
        
        # Mapeamento seguro de valores nulos
        valor_liquido = dados_facta.get("VALOR_LIQUIDO") or dados_facta.get("valor_liquido") or 0.0

        tipo_dado = dados_facta.get("tipo_dado")

        if tipo_dado == "PIX":
            dados_pagamento = {
                "tipo_liberacao": "PIX",
                "pix": dados_facta.get("chave_pix")
            }
        
        else:
            conta, conta_digito = self._separar_conta_digito(dados_facta.get("CONTA", ""))
            tipo_conta_raw = dados_facta.get("TIPO_CONTA", "")

            dados_pagamento = {
                "tipo_liberacao": self._mapear_tipo_conta(tipo_conta_raw),
                "banco_averbacao": dados_facta.get("BANCO"),
                "agencia": dados_facta.get("AGENCIA"),
                "conta": conta,
                "conta_digito": conta_digito
            }

        payload = {
            "auth": {
                "username": self.client.user,
                "password": self.client.password,
                "empresa": self.client.empresa
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

                    **dados_pagamento, # Insere os dados de pagamento mapeados (PIX ou CONTA)

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