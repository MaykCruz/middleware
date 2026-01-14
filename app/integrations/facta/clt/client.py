import logging
from app.integrations.facta.auth import FactaAuth, create_client

logger = logging.getLogger(__name__)

class FactaCLTAdapter:
    AVERBADOR = 10010
    PRODUTO = "D"
    TIPO_OPERACAO = 13
    CONVENIO = 3
    RENDA_PADRAO = 3000
    CNA_PADRAO = 10

    def __init__(self):
        self.auth = FactaAuth()
        self.base_url = self.auth.base_url

    @property
    def _get_headers(self):
        token = self.auth.get_valid_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
    
    def solicitar_termo(self, cpf: str, nome: str, celular: str) -> dict:
        """
        Envia o termo de autorização via WhatsApp (Facta)
        OBS: A API da Facta NÃO aceita DDI (55), apenas DDD + Número.
        """
        url = f"{self.base_url}/solicita-autorizacao-consulta"
        headers = self._get_headers
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        celular_api = celular
        if celular and celular.startswith("55") and len(celular) == 13:
            celular_api = celular[2:]

        data = {
            "averbador": self.AVERBADOR,
            "nome": nome,
            "cpf": cpf,
            "celular": celular_api,
            "tipo_envio": "WHATSAPP"
        }

        try:
            with create_client() as client:
                logger.info(f"📜 [Facta CLT] Solicitando termo para {cpf}...")
                resp = client.post(url, headers=headers, data=data)
                data = resp.json()

                status = self._interpretar_retorno_termo(data)

                return {
                    "status": status,
                    "msg_original": data.get("mensagem", ""),
                    "dados": data
                }
            
        except Exception as e:
            logger.error(f"❌ [Facta CLT] Erro ao solicitar termo: {e}")
            return {"status": "ERRO_TECNICO", "msg_original": str(e)}
        
    def consultar_dados_trabalhador(self, cpf: str) -> dict:
        """
        Verifica se o trabalhador existe na base e se é elegível.
        """
        url = f"{self.base_url}/consignado-trabalhador/autoriza-consulta"
        params = {"cpf": cpf}

        try:
            with create_client() as client:
                resp = client.get(url, headers=self._get_headers, params=params)
                data = resp.json()

                msg = data.get("mensagem", "").lower()

                if data.get("erro") and "fila de autorização" in msg:
                    return {
                        "status": "PROCESSAMENTO_PENDENTE",
                        "dados": [],
                        "msg_original": data.get("mensagem", "")
                    }

                status = self._interpretar_retorno_dados_trabalhador(data)

                return {
                    "status": status,
                    "dados": data.get("dados_trabalhador", {}).get("dados", []),
                    "msg_original": data.get("mensagem", "")
                }
            
        except Exception as e:
            logger.error(f"❌ [Facta CLT] Erro ao consultar dados trabalhador: {e}")
            return {"status": "ERRO_TECNICO", "msg_original": str(e)}
    
    def validar_politica_credito(self, cpf: str, matricula: str, nascimento: str, admissao: str) -> dict:
        """
        [ATUALIZADO] Consulta limites de prazo e valor pré-aprovado.
        Novo Endpoint: /analise-politica-credito
        """
        url = f"{self.base_url}/consignado-trabalhador/analise-politica-credito"
        params = {
            "cpf": cpf,
            "matricula": matricula,
            "dataNascimento": nascimento,
            "dataAdmissao": admissao,
            "prazo": 48,
            "valorEmprestimo": 26000
        }

        try:
            with create_client() as client:
                resp = client.get(url, headers=self._get_headers, params=params)
                data = resp.json()

                def tem_valor_disponivel(dado):
                    val = dado.get("valor_maximo_disponivel")
                    try:
                        return val is not None and float(val) > 0
                    except (ValueError, TypeError):
                        return False

                if data.get("erro") is True:
                    status = "ERRO_TECNICO"

                elif str(data.get("aprovado")) == "1":
                    status = "SUCESSO"
                
                elif str(data.get("aprovado")) == "0" and tem_valor_disponivel(data):
                    status = "SUCESSO"
                
                elif str(data.get("aprovado")) == "0":
                    status = "REPROVADO_POLITICA_FACTA"

                else:
                    logger.warning(f"⚠️ [Facta] Retorno desconhecido na política: {data}")
                    status = "ERRO_TECNICO"
                
                return {"status": status, "dados": data, "msg_original": data.get("mensagem", "")}

        except Exception as e:
            return {"status": "ERRO_TECNICO", "msg_original": str(e)}
    
    def buscar_operacoes(self, cpf: str, nascimento: str, valor_parcela: float = None, valor_solicitado = None) -> dict:
        """
        Simula as tabelas disponívels (Opção 1: Por Valor, Opção 2: Por Parcela)
        """
        url = f"{self.base_url}/proposta/operacoes-disponiveis"

        params = {
            "produto": self.PRODUTO,
            "tipo_operacao": self.TIPO_OPERACAO,
            "averbador": self.AVERBADOR,
            "convenio": self.CONVENIO,
            "cpf": cpf,
            "data_nascimento": nascimento,
            "valor_renda": self.RENDA_PADRAO
        }

        if valor_solicitado:
            params["opcao_valor"] = 1
            params["valor"] = valor_solicitado
        
        else:
            params["opcao_valor"] = 2
            params["valor_parcela"] = valor_parcela or 0
        
        try:
            with create_client() as client:
                logger.info(f"🧮 [Facta CLT] Buscando operações para {cpf}...")
                resp = client.get(url, headers=self._get_headers, params=params)
                data = resp.json()

                status = "ERRO_OPERACOES" if data.get("erro") else "SUCESSO"
                return {"status": status, "dados": data, "msg_original": data.get("mensagem", "")}

        except Exception as e:
            logger.error(f"❌ [Facta CLT] Erro ao buscar operações: {e}")
            return {"status": "ERRO_TECNICO", "msg_original": str(e)}
    
    def _interpretar_retorno_termo(self, data: dict) -> str:
        """
        Traduz os códigos de erro do termo de autorização, para nossos status internos.
        """
        msg = data.get("mensagem", "").lower()

        if data.get("erro"):
            return "ERRO_TECNICO"

        if "solicitação enviada com sucesso!" in msg:
            return "TERMO_ENVIADO"
        
        if "token válido" in msg:
            return "TERMO_JA_AUTORIZADO"
        
        if "telefone já informado para outro cpf!" in msg:
            return "TELEFONE_VINCULADO_OUTRO_CPF"
        
        return "RETORNO_DESCONHECIDO"
    
    def _interpretar_retorno_dados_trabalhador(self, data: dict) -> str:
        """
        Traduz os códigos de erro da consulta de dados do trabalhador, para nossos status internos.
        """
        if not data.get("erro"):
            return "SUCESSO"
        
        msg = data.get("mensagem", "").lower()

        if "token expirado, necessário utilizar o endpoint" in msg:
            return "TERMO_EXPIRADO"
        
        if "consulta de dados indisponível devido a virada de folha" in msg:
            return "VIRADA_FOLHA_CLT"
        
        if "cpf não encontrado na base" in msg:
            return "CPF_NAO_ENCONTRADO_NA_BASE"
        
        return "RETORNO_DESCONHECIDO"
        
        

