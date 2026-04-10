import logging
from typing import Dict, Any, Optional
from app.integrations.v8.auth import V8Auth, create_v8_client
from app.integrations.v8.clt.client import V8CLTAdapter
from app.schemas.credit import AnalysisStatus

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
            consult_id = consulta_existente.get("id")
            logger.info(f"🔄 [V8 Service] Reaproveitando consulta existente ({status_v8}) para {cpf}.")

            if status_v8 == "SUCCESS":
                detalhes = adapter.buscar_detalhes_consulta(consult_id)
                if detalhes:
                    margem_raw = detalhes.get("marginBaseValue")
                    margem = float(margem_raw)
                    limites = detalhes.get("simulationLimit", {})
                    max_parcelas = limites.get("installmentsMax")

                    logger.info(f"✅ [V8 Service] Detalhes recuperados: Margem R$ {margem} | {max_parcelas}x")

                    return {
                        "acao": AnalysisStatus.APROVADO,
                        "consult_id": consult_id,
                        "margem": margem,
                        "max_parcelas": max_parcelas
                    }
                
                else:
                    logger.error(f"❌ [V8 Service] Falha ao recuperar os detalhes da consulta {consult_id}.")
                    return {"acao": AnalysisStatus.ERRO_TECNICO}
                
            elif status_v8 == "REJECTED":
                return {
                    "acao": AnalysisStatus.REPROVADO_POLITICA_V8,
                    "motivo": consulta_existente.get("description"),
                }
            elif status_v8 in ["WAITING_CREDIT_ANALYSIS", "WAITING_CONSENT", "PROCESSING"]:
                return {
                    "acao": AnalysisStatus.AGUARDANDO_AUTORIZACAO,
                    "consult_id": consulta_existente.get("id")
                }
        
        logger.info(f"🆕 [V8 Service] Nenhuma consulta válida encontrada. Gerando termo para {cpf}.")
        consult_id = adapter.criar_termo_consulta(cpf)

        if not consult_id:
            logger.error(f"❌ [V8 Service] Fluxo interrompido: Falha ao gerar o termo.")
            return {
                "acao": AnalysisStatus.ERRO_TECNICO,
                "dados": None
            }
        
        sucesso_autorizacao = adapter.autorizar_termo(consult_id)

        if sucesso_autorizacao:
            logger.info(f"⏳ [V8 Service] Fluxo inicial concluído! ID {consult_id} aguardando resposta assíncrona do Dataprev.")
            return {
                "acao": AnalysisStatus.AGUARDANDO_WEBHOOK,
                "consult_id": consult_id
            }
        else:
            return {
                "acao": AnalysisStatus.ERRO_TECNICO,
                "consult_id": consult_id
            }
    
    def _organizar_prioridade_tabelas(self, tabelas: list) -> list:
        tabela_com_seguro = [t for t in tabelas if t.get("is_insured")]
        tabela_sem_seguro = [t for t in tabelas if not t.get("is_insured")]

        fila = tabela_com_seguro + tabela_sem_seguro
        return fila if fila else tabelas
    
    def gerar_simulacao_final(self, consult_id: str, valor_parcela: float, parcelas: int) -> Dict[str, Any]:
        adapter = self._get_adapter()
        tabelas = adapter.buscar_tabelas(consult_id)

        if not tabelas:
            logger.error(f"❌ [V8 Service] Nenhuma tabela encontrada para simular a consulta {consult_id}.")
            return {"acao": "ERRO_TABELAS", "dados": None}
        
        fila_tabelas = self._organizar_prioridade_tabelas(tabelas)
        simulacao = None

        ERROS_TENTAR_PROXIMA = [
            "provider_does_not_have_insurance_active"
        ]

        ERROS_ABORTAR_FLUXO = [
            "simulation_consult_operation_ongoing"
        ]

        for tabela in fila_tabelas:
            table_id = tabela.get("id")
            nome_tabela = tabela.get("slug", table_id)
            prazos_aceitos = tabela.get("number_of_installments", [])

            parcelas_tentativa = parcelas
            prazos_int = sorted([int(p) for p in prazos_aceitos])

            if prazos_int:
                max_permitido = max(prazos_int)
                if parcelas_tentativa > max_permitido:
                    parcelas_tentativa = max_permitido
                elif str(parcelas_tentativa) not in [str(p) for p in prazos_aceitos]:
                    prazos_menores = [p for p in prazos_int if p <= parcelas_tentativa]
                    parcelas_tentativa = max(prazos_menores) if prazos_menores else max_permitido
            
            logger.info(f"🔄 [V8 Service] Tentando simulação. Tabela: {nome_tabela} | Prazo: {parcelas_tentativa}x")

            resultado = adapter.simular_operacao(consult_id, table_id, valor_parcela, parcelas_tentativa)

            if isinstance(resultado, dict) and resultado.get("is_error"):
                payload = resultado.get("payload", {})
                tipo_erro = payload.get("type")
                detalhe = payload.get("detail", "Erro desconhecido na API")

                if tipo_erro in ERROS_ABORTAR_FLUXO:
                    logger.warning(f"🚫 [V8 Service] Simulação abortada: {detalhe}")
                    return {
                        "acao": "SIMULACAO_BLOQUEADA",
                        "sub_tipo": tipo_erro,
                        "mensagem": detalhe
                    }
                
                if tipo_erro in ERROS_TENTAR_PROXIMA:
                    logger.warning(f"⚠️ [V8 Service] Tabela {nome_tabela} indisponível ({tipo_erro}). Indo para próxima...")
                    continue

                logger.error(f"❌ [V8 Service] Erro inesperado: {tipo_erro} - {detalhe}")
                break

            elif not resultado:
                logger.warning(f"⚠️ [V8 Service] Sem resposta da API para tabela {nome_tabela}. Tentando próxima...")
                continue

            else:
                logger.info(f"🎉 [V8 Service] Simulação bem sucedida na tabela {nome_tabela}!")
                simulacao = resultado
                break

        if not simulacao:
            logger.error(f"❌ [V8 Service] Todas as tentativas de tabela falharam para {consult_id}.")
            return {"acao": "ERRO_SIMULACAO", "mensagem": "Não foi possível encontrar uma tabela compatível para este valor."}
        
        logger.info(f"🎉 [V8 Service] Simulação finalizada com sucesso para {consult_id}!")
        return {
            "acao": "SIMULACAO_CONCLUIDA",
            "dados": simulacao
        }
