import logging
from typing import Dict, Any, Optional
from app.integrations.v8.auth import V8Auth, create_v8_client
from app.integrations.v8.clt.client import V8CLTAdapter

logger = logging.getLogger(__name__)

class V8CLTService:
    def __init__(self):
        self.auth = V8Auth()

    def _get_adapter(self) -> V8CLTAdapter:
        token = self.auth.get_valid_token()
        http_client = create_v8_client()
        http_client.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        })

        return V8CLTAdapter(http_client)
    
    def processar_nova_consulta(self, cpf: str) -> Dict[str, Any]:
        adapter = self._get_adapter()
        consulta_existente = adapter.buscar_consulta_existente(cpf)

        if consulta_existente:
            status_v8 = consulta_existente.get("status")
            logger.info(f"🔄 [V8 Service] Reaproveitando consulta existente ({status_v8}) para {cpf}.")
            return {
                "acao": "REAPROVEITADO",
                "dados": consulta_existente
            }
        
        logger.info(f"🆕 [V8 Service] Nenhuma consulta válida encontrada. Gerando termo para {cpf}.")
        consult_id = adapter.criar_termo_consulta(cpf)

        if not consult_id:
            logger.error(f"❌ [V8 Service] Fluxo interrompido: Falha ao gerar o termo.")
            return {
                "acao": "ERRO_CRIACAO",
                "dados": None
            }
        
        sucesso_autorizacao = adapter.autorizar_termo(consult_id)

        if sucesso_autorizacao:
            logger.info(f"⏳ [V8 Service] Fluxo inicial concluído! ID {consult_id} aguardando resposta assíncrona do Dataprev.")
            return {
                "acao": "NOVO_AGUARDANDO_WEBHOOK",
                "consult_id": consult_id
            }
        else:
            return {
                "acao": "ERRO_AUTORIZACAO",
                "consult_id": consult_id
            }
    
    def obter_melhor_tabela(self, consult_id: str) -> Optional[str]:
        adapter = self._get_adapter()
        tabelas = adapter.buscar_tabelas(consult_id)

        if not tabelas:
            return None
        
        com_seguro = next((t for t in tabelas if t.get("is_insured")), None)
        if com_seguro:
            logger.info(f"🛡️ [V8 Service] Selecionando tabela com seguro: {com_seguro.get('slug')}")
            return com_seguro.get("id")
        
        logger.info(f"ℹ️ [V8 Service] Selecionando tabela padrão: {tabelas[0].get('slug')}")
        return tabelas[0].get("id")
    
    def gerar_simulacao_final(self, consult_id: str, valor_parcela: float) -> Dict[str, Any]:
        adapter = self._get_adapter()
        table_id = self.obter_melhor_tabela(consult_id)

        if not table_id:
            logger.error(f"❌ [V8 Service] Nenhuma tabela encontrada para simular a consulta {consult_id}.")
            return {"acao": "ERRO_TABELAS", "dados": None}
        
        simulacao = adapter.simular_operacao(consult_id, table_id, valor_parcela)

        if not simulacao:
            return {"acao": "ERRO_SIMULACAO", "dados": None}
            
        logger.info(f"🎉 [V8 Service] Simulação finalizada com sucesso para {consult_id}!")
        return {
            "acao": "SIMULACAO_CONCLUIDA",
            "dados": simulacao
        }
