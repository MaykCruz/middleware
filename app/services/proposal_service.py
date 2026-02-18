import logging
from typing import Dict, Any
from app.integrations.facta.proposal.service import FactaProposalService
from app.services.bot.memory.session import SessionManager
from app.integrations.newcorban.service import NewCorbanService
from app.integrations.facta.complementares.funcoes_complementares import FactaDadosCadastrais


logger = logging.getLogger(__name__)

class ProposalService:
    """
    Serviço de Aplicação (Global).
    Responsável por gerenciar a Sessão do Usuário (Redis) e acionar a integração correta.
    """
    def __init__(self):
        self.facta_service = FactaProposalService()
        self.session_manager = SessionManager()

        self.newcorban_service = NewCorbanService()
        self.facta_dados = FactaDadosCadastrais()
    
    def executar_digitacao_fgts(self, chat_id: str) -> Dict[str, Any]:
        """
        Recupera o contexto do usuário no Redis e dispara a esteira de FGTS.
        """
        try:
            logger.info(f"🤖 [Proposal Global] Iniciando fluxo FGTS para Chat {chat_id}")

            context = self.session_manager.get_context(chat_id)
            if not context:
                raise ValueError("Sessão expirada ou não encontrada.")
            
            if context.get("cep"):
                context["cep"] = str(context["cep"]).replace("-", "").replace(".", "").strip()
            
            cpf = context.get("cpf")
            if not cpf:
                raise ValueError("CPF não encontrado na sessão.")
            
            oferta = context.get("oferta_selecionada", {})
            detalhes = oferta.get("detalhes", {})

            simulacao_id = detalhes.get("simulacao_fgts")
            if not simulacao_id:
                raise ValueError("ID da simulação FGTS não encontrado no contexto. O cliente fez a simulação?")
            
            resultado = self.facta_service.processar_digitacao_fgts(
                cpf=cpf,
                simulacao_id_calculo=int(simulacao_id),
                dados_contexto=context
            )

            self._tentar_cadastro_newcorban(cpf, resultado, detalhes.get("valor_liquido"), context=context)

            logger.info(f"🎉 [Proposal Global] Sucesso Chat {chat_id}! Link: {resultado.get('url_formalizacao')}")
            return resultado

        except Exception as e:
            logger.error(f"❌ [Proposal Global] Falha na digitação automática FGTS: {e}")
            raise e
        
    def executar_digitacao_clt(self, chat_id: str) -> Dict[str, Any]:
        """
        Recupera o contexto do usuário no Redis e dispara a esteira de CLT.
        """
        try:
            logger.info(f"🤖 [Proposal Global] Iniciando fluxo CLT para Chat {chat_id}")

            context = self.session_manager.get_context(chat_id)
            if not context:
                raise ValueError("Sessão expirada ou não encontrada.")
            
            if context.get("cep"):
                context["cep"] = str(context["cep"]).replace("-", "").replace(".", "").strip()
            
            cpf = context.get("cpf")
            if not cpf:
                raise ValueError("CPF não encontrado na sessão.")
            
            oferta_wrapper = context.get("oferta_selecionada", {})
            oferta_dados = oferta_wrapper.get("detalhes") or oferta_wrapper

            if not oferta_dados.get("codigo_tabela"):
                raise ValueError("Dados da oferta CLT (tabela/prazo) não encontrados no contexto.")
            
            resultado = self.facta_service.processar_digitacao_clt(
                cpf=cpf,
                dados_oferta=oferta_dados,
                dados_contexto=context
            )

            self._tentar_cadastro_newcorban(cpf, resultado, oferta_dados.get("valor_liquido"), context=context)

            logger.info(f"🎉 [Proposal Global] Sucesso CLT Chat {chat_id}! Link: {resultado.get('url_formalizacao')}")
            return resultado

        except Exception as e:
            logger.error(f"❌ [Proposal Global] Falha na digitação automática CLT: {e}")
            raise e

    def _tentar_cadastro_newcorban(self, cpf: str, resultado_facta: dict, valor_liquido: float, context: dict):
        """Helper para isolar a lógica do NewCorban e evitar duplicação"""
        codigo_af = resultado_facta.get("codigo")
        link_formalizacao = resultado_facta.get("url_formalizacao")

        if codigo_af:
            try:
                logger.info(f"🔌 [Proposal Global] Iniciando cadastro no NewCorban para AF {codigo_af}...")
                dados_completos = self.facta_dados.consultar_dados_completos(cpf)
                if dados_completos:
                    oferta = context.get("oferta_selecionada", {})
                    detalhes = oferta.get("detalhes", {})
                    dados_bancarios_redis = detalhes.get("dados_bancarios") or {}
                    
                    dados_completos.update(dados_bancarios_redis)

                    dados_completos["link_formalizacao"] = link_formalizacao
                    dados_completos["VALOR_LIQUIDO"] = valor_liquido
                    self.newcorban_service.cadastrar_proposta(dados_completos, codigo_af)
                else:
                    logger.warning("⚠️ [Proposal Global] Falha ao obter dados completos na Facta. CRM pulado.")
            except Exception as e_crm:
                logger.error(f"⚠️ [Proposal Global] Erro ao integrar com NewCorban (Não crítico): {e_crm}")
