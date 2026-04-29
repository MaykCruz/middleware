import logging
import re
import httpx
from typing import Dict, Any
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
    def __init__(self, http_client: httpx.Client):
        self.client = FactaProposalClient(http_client)
        self.consulta_dados = FactaDadosCadastrais(http_client)
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
        Usa 'tipo_dado' (PIX ou CONTA) para decidir o preenchimento.
        """
        dados_api = dados_api or {}

        dados_basicos = dados_contexto.get("dados_basicos_cliente", {})
        nc = dados_contexto.get("dados_newcorban", {})

        sexo_desc = str(dados_basicos.get("sexo_descricao", "")).upper()
        sexo_sigla = "F" if "FEMININO" in sexo_desc else "M"

        nome = dados_api.get("DESCRICAO") or dados_basicos.get("nome") or nc.get("nome")
        nome_mae = dados_api.get("NOMEMAE") or dados_basicos.get("nome_mae") or nc.get("nome_mae") or "NAO INFORMADO"

        data_nascimento_raw = dados_api.get("DATANASCIMENTO") or dados_basicos.get("data_nascimento") or nc.get("data_nascimento")
        data_nascimento = self._converter_data(data_nascimento_raw)

        sexo = dados_api.get("SEXO") or nc.get("sexo") or sexo_sigla

        rg = dados_api.get("RG") or nc.get("rg") or cpf
        orgao_emissor = dados_api.get("ORGAOEMISSOR") or "SSP"
        estado_rg = dados_api.get("ESTADORG") or nc.get("uf") or "SP"

        data_expedicao_raw = dados_api.get("EMISSAORG")
        data_expedicao = self._converter_data(data_expedicao_raw) if data_expedicao_raw else "01/01/2010"
        
        cep_raw = dados_api.get("CEP") or nc.get("cep")
        cep = self._limpar_numeros(cep_raw) or "01001000"
        endereco = dados_api.get("ENDERECO") or nc.get("logradouro") or "RUA NAO INFORMADA"

        numero = dados_api.get("NUMERO") or nc.get("numero")
        numero = str(numero) if numero and str(numero).strip() != "" else "01"
        bairro = dados_api.get("BAIRRO") or nc.get("bairro") or "CENTRO"

        estado_raw = str(dados_api.get("ESTADO") or nc.get("uf") or "SP").upper().strip()

        cidade_raw = str(dados_api.get("CIDADE") or nc.get("cidade") or "SAO PAULO").upper().strip()

        if " - " in cidade_raw:
            cidade_id = self._extrair_id_hibrido(cidade_raw)
            estado = estado_raw
        else:
            id_busca = self.data_manager.get_cidade_id(cidade_raw, estado_raw)
            if id_busca:
                cidade_id = id_busca
                estado = estado_raw
            else:
                logger.warning(f"⚠️ [Mapeador] Cidade '{cidade_raw}/{estado_raw}' não encontrada. Aplicando Fallback SP/SP.")
                cidade_id = 442
                estado = "SP"

        naturalidade_raw = str(dados_api.get("CIDADENATURAL") or "SAO PAULO").upper().strip()
        
        if " - " in naturalidade_raw:
            naturalidade_id = self._extrair_id_hibrido(naturalidade_raw)
            estado_natural = self.data_manager.get_uf_por_id(naturalidade_id) or estado_rg
        else:
            id_natural_busca = self.data_manager.get_cidade_id(naturalidade_raw, estado_raw)
            if id_natural_busca:
                naturalidade_id = id_natural_busca
                estado_natural = estado_raw
            else:
                naturalidade_id = 442
                estado_natural = "SP"
        
        celular_contexto = self._formatar_celular(dados_contexto.get("celular"))

        payload = {
            "id_simulador": id_simulador,
            "cpf": cpf,
            "nome": nome,
            "sexo": sexo,
            "data_nascimento": data_nascimento,

            "rg": rg,
            "estado_rg": estado_rg,
            "orgao_emissor": orgao_emissor,
            "data_expedicao": data_expedicao,

            "cidade_natural": naturalidade_id,
            "estado_natural": estado_natural,

            "celular": celular_contexto,

            "cep": cep,
            "endereco": endereco,
            "numero": str(numero),
            "bairro": bairro,
            "cidade": cidade_id,
            "estado": estado,

            "nome_mae": nome_mae
        }
        
        oferta_ctx = dados_contexto.get("oferta_selecionada", {})
        detalhes_ctx = oferta_ctx.get("detalhes", {})
        dados_bancarios = detalhes_ctx.get("dados_bancarios") or {}

        tipo_pagamento = dados_bancarios.get("tipo_dado")

        if tipo_pagamento == "PIX":
            logger.info(f"💸 [Facta Service] Configurando pagamento via PIX para {cpf}")

            payload.update({
                "chave_pix": dados_bancarios.get("chave_pix"),
                "tipo_chave_pix": dados_bancarios.get("codigo_tipo_chave_pix")
            })
        
        else:
            logger.info(f"🏦 [Facta Service] Configurando pagamento via CONTA para {cpf}")

            payload.update({
                "banco": dados_bancarios.get("BANCO"),
                "agencia": dados_bancarios.get("AGENCIA"),
                "conta": dados_bancarios.get("CONTA"),
                "tipo_conta": dados_bancarios.get("TIPO_CONTA")
            })

        return payload
    
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

        dados_api = self.consulta_dados.consultar_dados_completos(cpf) or {}
        dados_nc = dados_contexto.get("dados_newcorban", {})
        dados_basicos = dados_contexto.get("dados_basicos_cliente", {})

        if not dados_api and not dados_nc and not dados_basicos:
            raise ValueError(f"Cliente não possui dados cadastrais (Facta/NewCorban) para digitação: {cpf}")
        
        nasc_br_raw = dados_api.get("DATANASCIMENTO") or dados_nc.get("data_nascimento") or dados_basicos.get("data_nascimento")
        nasc_br = self._converter_data(nasc_br_raw)

        oferta = dados_contexto.get("oferta_selecionada", {}) 
        detalhes = oferta.get("detalhes", {})
        matricula = str(detalhes.get("matricula", ""))

        dados_step1 = {
            "cpf": cpf,
            "data_nascimento": nasc_br,
            "codigo_tabela": int(dados_oferta.get("codigo_tabela")),
            "prazo": int(dados_oferta.get("prazo")),
            "valor_operacao": float(dados_oferta.get("valor_operacao")),
            "valor_parcela": float(dados_oferta.get("valor_parcela")),
            "coeficiente": float(dados_oferta.get("coeficiente") or 0),
            "matricula": matricula
        }
        id_simulador = self._step1_simulacao_clt(dados_step1)
        logger.info(f"✅ Step 1 (CLT) OK. ID: {id_simulador}")

        payload_base = self._mapear_dados_api_para_schema(cpf, dados_api, id_simulador, dados_contexto)


        payload_base["matricula"] = matricula
        payload_base["data_admissao"] = self._converter_data(str(detalhes.get("data_admissao", "")))
        payload_base["cnpj_empregador"] = str(detalhes.get("cnpj_empregador", ""))

        codigo_cliente = self._step2_dados_pessoais_clt(payload_base)
        logger.info(f"✅ Step 2 (CLT) OK. Cli: {codigo_cliente}")

        resultado = self._step3_finalizacao(codigo_cliente, id_simulador)
        return resultado