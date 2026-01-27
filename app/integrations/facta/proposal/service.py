import logging
import re
from typing import Dict, Any
from datetime import datetime

from app.integrations.facta.proposal.client import FactaProposalClient
from app.integrations.facta.proposal.schemas import ProposalStep1FGTS, ProposalStep2Base, ProposalStep1CLT, ProposalStep2CLT
from app.integrations.facta.complementares.funcoes_complementares import FactaDadosCadastrais
from app.services.data_manager import DataManager

logger = logging.getLogger(__name__)

class FactaProposalService:
    """
    Serviço especialista em digitação Facta.
    Responsável por validar os dados (Schemas) e orquestrar as chamadas ao Client.
    """
    def __init__(self):
        self.client = FactaProposalClient()
        self.consulta_dados = FactaDadosCadastrais()
        self.data_manager = DataManager()
    
    def _limpar_numeros(self, texto: str) -> str:
        """Remove tudo que não é dígito."""
        if not texto: return ""
        return re.sub(r'\D', '', str(texto))
    
    def _extrair_id_hibrido(self, valor: str) -> int:
        """
        Converte '442 - SAO PAULO' para 442.
        Se falhar ou vier vazio, retorna 442 (São Paulo) como fallback seguro.
        """
        if not valor or " - " not in str(valor):
            return 442
        try:
            return int(str(valor).split(" - ")[0])
        except:
            return 442
        
    def _converter_data(self, data_iso: str) -> str:
        """
        Converte '1990-01-31' para '31/01/1990'.
        Essencial pois a Facta retorna ISO na consulta mas exige BR no cadastro.
        """
        if not data_iso: return ""
        try:
            from datetime import datetime
            iso_clean = data_iso.split(" ")[0]
            d = datetime.strptime(iso_clean, "%Y-%m-%d")
            return d.strftime("%d/%m/%Y")
        except:
            return data_iso
    
    def _formatar_celular(self, celular_raw: str) -> str:
        """
        Formata para (0XX) XXXXX-XXXX.
        Espera entrada como: 5511999999999 ou 11999999999.
        """
        limpo = self._limpar_numeros(celular_raw)

        if len(limpo) == 13 and limpo.startswith("55"):
            limpo = limpo[2:]
            
        if len(limpo) == 11:
            ddd = limpo[:2]
            parte1 = limpo[2:7]
            parte2 = limpo[7:]

            return f"(0{ddd}) {parte1}-{parte2}"
        
        return celular_raw
    
    def _mapear_dados_api_para_schema(self, cpf: str, dados_api: dict, id_simulador: int, dados_contexto: dict) -> dict:
        """
        Traduz o JSON 'sujo' da Consulta Facta para o formato limpo do Schema Step 2.
        """
        naturalidade_id = self._extrair_id_hibrido(dados_api.get("CIDADENATURAL"))

        estado_natural = self.data_manager.get_uf_por_id(naturalidade_id)
        if not estado_natural:
            estado_natural = dados_api.get("ESTADORG")
        
        cidade_id = self._extrair_id_hibrido(dados_api.get("CIDADE"))

        numero = dados_api.get("NUMERO")
        if not numero or str(numero).strip() == "":
            numero = "01"
        
        celular_contexto = self._formatar_celular(dados_contexto.get("celular"))
        
        oferta_ctx = dados_contexto.get("oferta_selecionada", {})
        detalhes_ctx = oferta_ctx.get("detalhes", {})
        dados_bancarios = detalhes_ctx.get("dados_bancarios") or {}

        return {
            "id_simulador": id_simulador,
            "cpf": cpf,
            "nome": dados_api.get("DESCRICAO"),
            "sexo": dados_api.get("SEXO"),
            "data_nascimento": self._converter_data(dados_api.get("DATANASCIMENTO")),
            "rg": dados_api.get("RG"),
            "estado_rg": dados_api.get("ESTADORG"),
            "orgao_emissor": dados_api.get("ORGAOEMISSOR"),
            "data_expedicao": self._converter_data(dados_api.get("EMISSAORG")),
            "cidade_natural": naturalidade_id,
            "estado_natural": estado_natural,
            "celular": celular_contexto,
            "cep": dados_api.get("CEP"),
            "endereco": dados_api.get("ENDERECO"),
            "numero": str(numero),
            "bairro": dados_api.get("BAIRRO"),
            "cidade": cidade_id,
            "estado": dados_api.get("ESTADO"),
            "nome_mae": dados_api.get("NOMEMAE"),
            "banco": dados_bancarios.get("banco"),
            "agencia": dados_bancarios.get("agencia"),
            "conta": dados_bancarios.get("conta"),
            "tipo_conta": dados_bancarios.get("tipo_conta")
        }
    
    def _step1_simulacao_fgts(self, dados: Dict[str, Any]) -> int:
        payload = ProposalStep1FGTS(**dados)
        return self.client.registrar_etapa_1_simulacao(payload.model_dump())
    
    def _step1_simulacao_clt(self, dados: Dict[str, Any]) -> int:
        payload = ProposalStep1CLT(**dados)
        return self.client.registrar_etapa_1_simulacao(payload.model_dump())
    
    def _step2_dados_pessoais_clt(self, dados: Dict[str, Any]) -> int:
        payload = ProposalStep2CLT(**dados)
        return self.client.registrar_etapa_2_dados_pessoais(payload.model_dump())
    
    def _step2_dados_pessoais(self, dados: Dict[str, Any]) -> int:
        payload = ProposalStep2Base(**dados)
        return self.client.registrar_etapa_2_dados_pessoais(payload.model_dump())
    
    def _step3_finalizacao(self, codigo_cliente: int, id_simulador: int) -> Dict[str, Any]:
        return self.client.registrar_etapa_3_efetivacao(codigo_cliente, id_simulador)
    
    def processar_digitacao_fgts(self, cpf: str, simulacao_id_calculo: int, dados_contexto: Dict[str, Any]) -> Dict[str, Any]:
        """
        Orquestra a Digitação FGTS.
        """
        logger.info(f"🤖 [Facta] Iniciando esteira FGTS para {cpf}")

        dados_api = self.consulta_dados.consultar_dados_completos(cpf)
        if not dados_api:
            raise ValueError(f"Cliente não encontrado na Facta: {cpf}")
        
        nasc_br = self._converter_data(dados_api.get("DATANASCIMENTO"))
        dados_step1 = {
            "cpf": cpf,
            "data_nascimento": nasc_br,
            "simulacao_fgts": simulacao_id_calculo
        }
        id_simulador = self._step1_simulacao_fgts(dados_step1)
        logger.info(f"✅ Step 1 OK. ID: {id_simulador}")

        payload_step2 = self._mapear_dados_api_para_schema(cpf, dados_api, id_simulador, dados_contexto)
        codigo_cliente = self._step2_dados_pessoais(payload_step2)

        logger.info(f"✅ Step 2 OK. Cli: {codigo_cliente}")

        resultado = self._step3_finalizacao(codigo_cliente, id_simulador)

        return resultado
    
    def processar_digitacao_clt(self, cpf: str, dados_oferta: Dict[str, Any], dados_contexto: Dict[str, Any]) -> Dict[str, Any]:
        """
        Orquestra a Digitação CLT.
        Exige: codigo_tabela, prazo, valor, dados empregatícios no contexto.
        """
        logger.info(f"🤖 [Facta] Iniciando esteira CLT para {cpf}")

        dados_api = self.consulta_dados.consultar_dados_completos(cpf)
        if not dados_api:
            raise ValueError(f"Cliente não encontrado na Facta: {cpf}")
        
        nasc_br = self._converter_data(dados_api.get("DATANASCIMENTO"))

        dados_step1 = {
            "cpf": cpf,
            "data_nascimento": nasc_br,
            "codigo_tabela": int(dados_oferta.get("codigo_tabela")),
            "prazo": int(dados_oferta.get("prazo")),
            "valor_operacao": float(dados_oferta.get("valor_operacao")),
            "valor_parcela": float(dados_oferta.get("valor_parcela")),
            "coeficiente": float(dados_oferta.get("coeficiente") or 0)
        }
        id_simulador = self._step1_simulacao_clt(dados_step1)
        logger.info(f"✅ Step 1 (CLT) OK. ID: {id_simulador}")

        payload_base = self._mapear_dados_api_para_schema(cpf, dados_api, id_simulador, dados_contexto)

        payload_base["matricula"] = str(dados_contexto.get("matricula", ""))
        payload_base["data_admissao"] = self._converter_data(str(dados_contexto.get("data_admissao", "")))
        payload_base["cnpj_empregador"] = str(dados_contexto.get("cnpj_empregador", ""))

        codigo_cliente = self._step2_dados_pessoais_clt(payload_base)
        logger.info(f"✅ Step 2 (CLT) OK. Cli: {codigo_cliente}")

        resultado = self._step3_finalizacao(codigo_cliente, id_simulador)
        return resultado