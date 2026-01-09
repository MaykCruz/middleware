import logging
from app.services.bot.memory.session import SessionManager
from app.integrations.huggy.service import HuggyService
from app.services.products.fgts_service import FGTSService
from app.services.products.clt_service import CLTService
from app.schemas.credit import AnalysisStatus
from app.tasks.monitor import check_inactivity
from app.core.timeouts import TIMEOUT_POLICES
from app.utils.validators import validate_cpf, clean_digits


logger = logging.getLogger(__name__)

class BotEngine:
    """
    Máquina de Estados que decide o fluxo da conversa.
    """
    def __init__(self):
        self.session = SessionManager()
        self.huggy = HuggyService()
        self.fgts_service = FGTSService()
        self.clt_service = CLTService()

    def _schedule_timeout(self, chat_id: int, state: str, interaction_time: int):
        """
        Verifica se existe uma regra de timeout para o estado 'state' e agenda a task no Celery.
        """
        rule = TIMEOUT_POLICES.get(state)

        if rule:
            check_inactivity.apply_async(
                args=[chat_id, state, interaction_time],
                countdown=rule['delay']
            )

    def process(self, chat_id: int, message_text: str):
        interaction_time = self.session.touch(chat_id)
        current_state = self.session.get_state(chat_id)
        context = self.session.get_context(chat_id)

        logger.info(f"🤖 [Engine] Chat: {chat_id} | Estado: {current_state} | Input: '{message_text}'")

        next_state = current_state

        # --- MÁQUINA DE ESTADOS ---

        # 0. Início
        if current_state == "START":
            self.huggy.send_message(chat_id, "menu_bem_vindo")
            next_state = "MENU_APRESENTACAO"
            self._schedule_timeout(chat_id, next_state, interaction_time)

        # 1. Menu Inicial
        elif current_state == "MENU_APRESENTACAO":
            opt = message_text.strip()

            if opt == "1": # CLT
                self.huggy.send_message(chat_id, "pedir_cpf")
                next_state = "CLT_AGUARDANDO_CPF"
                self._schedule_timeout(chat_id, next_state, interaction_time)
            
            elif opt == "2": # FGTS
                self.huggy.send_message(chat_id, "pedir_cpf")
                next_state = "FGTS_AGUARDANDO_CPF"
                self._schedule_timeout(chat_id, next_state, interaction_time)
            
            else:
                self._handoff_human(chat_id)
                next_state = "FINISHED"
        
        elif current_state == "MENU_TIMEOUT_1":
            opt = message_text.strip()

            if opt == "1": # CLT
                self.huggy.send_message(chat_id, "pedir_cpf")
                next_state = "CLT_AGUARDANDO_CPF"
                self._schedule_timeout(chat_id, next_state, interaction_time)

            elif opt == "2": # FGTS
                self.huggy.send_message(chat_id, "pedir_cpf")
                next_state = "FGTS_AGUARDANDO_CPF"
                self._schedule_timeout(chat_id, next_state, interaction_time)

            else:
                self._handoff_human(chat_id)
                next_state = "FINISHED"

        elif current_state == "MENU_TIMEOUT_2":
            opt = message_text.strip()

            if opt == "1": # CLT
                self.huggy.send_message(chat_id, "pedir_cpf")
                next_state = "CLT_AGUARDANDO_CPF"
                self._schedule_timeout(chat_id, next_state, interaction_time)

            elif opt == "2": # FGTS
                self.huggy.send_message(chat_id, "pedir_cpf")
                next_state = "FGTS_AGUARDANDO_CPF"
                self._schedule_timeout(chat_id, next_state, interaction_time)

            else:
                self._handoff_human(chat_id)
                next_state = "FINISHED"

        elif current_state == "CPF_TIMEOUT":
            opt = message_text.strip()

            if opt == "1": # Sim
                self.huggy.start_auto_distribution(chat_id)
                next_state = "FINISHED"
            
            elif opt == "2": # Não
                self.huggy.send_message(chat_id, "sem_interesse")
                self.huggy.finish_attendance(chat_id, tabulation_id=self.huggy.tabulations.get("SEM_INTERESSE"))
                next_state = "FINISHED"

        # ---------------------------------------------------------
        # FLUXO 1: CLT (CPF -> Tempo Registro -> Fim)
        # ---------------------------------------------------------
        elif current_state == "CLT_AGUARDANDO_CPF" or current_state == "CLT_CPF_INVALIDO":
            cpf_limpo = clean_digits(message_text)
            
            if validate_cpf(cpf_limpo):
                # CPF VÁLIDO
                context["cpf"] = cpf_limpo
                self.session.set_context(chat_id, context)

                self.huggy.send_message(chat_id, "tempo_de_registro")
                next_state = "CLT_AGUARDANDO_TEMPO_REGISTRO"
            
            else:
                # CPF INVÁLIDO
                if current_state == "CLT_AGUARDANDO_CPF":
                    self.huggy.send_message(chat_id, "cpf_invalido")
                    next_state = "CLT_CPF_INVALIDO"
                else:
                    self.huggy.send_message(chat_id, "cpf_invalido_fallback", force_internal=True)
                    self.huggy.start_auto_distribution(chat_id)
                    next_state = "FINISHED"

        # Lógica de CLT_AGUARDANDO_TEMPO_REGISTRO
        elif current_state == "CLT_AGUARDANDO_TEMPO_REGISTRO":
            opt = message_text.strip()

            if opt == "1": # Possui o mínimo de 6 meses.
                self.huggy.send_message(chat_id, "iniciando_simulacao")

                oferta = self.clt_service.consultar_oportunidade(cpf_limpo)

                self.huggy.send_message(
                    chat_id,
                    oferta.message_key,
                    variables=oferta.variables,
                    force_internal=oferta.is_internal
                )

                if oferta.status == AnalysisStatus.APROVADO:
                    self.huggy.move_to_aprovado(chat_id)
                    self.huggy.start_auto_distribution(chat_id)
                    next_state = "FINISHED"

                self.huggy.start_auto_distribution(chat_id)
                next_state = "FINISHED"
            
            elif opt == "2": # Não possui o mínimo de 6 meses.
                self.huggy.send_message(chat_id, "requirements_fail")
                self.huggy.finish_attendance(chat_id, tabulation_id=self.huggy.tabulations.get("MENOS_SEIS_MESES"))
                next_state = "FINISHED"

        # ---------------------------------------------------------
        # FLUXO 2: FGTS (CPF -> Simulação Imediata)
        # ---------------------------------------------------------
        elif current_state == "FGTS_AGUARDANDO_CPF" or current_state == "FGTS_CPF_INVALIDO":
            cpf_limpo = clean_digits(message_text)
            
            if validate_cpf(cpf_limpo):
                # 1. Salvar Contexto
                context["cpf"] = cpf_limpo
                self.session.set_context(chat_id, context)

                # 2. Feedback de "Simulando..."
                # Não mudamos para um estado intermediário, executamos já.
                self.huggy.send_message(chat_id, "iniciando_simulacao")
                
                # Chama o Service Global
                oferta = self.fgts_service.consultar_melhor_oportunidade(cpf_limpo)

                self.huggy.send_message(
                    chat_id,
                    oferta.message_key,
                    variables=oferta.variables,
                    force_internal=oferta.is_internal
                )

                if oferta.status == AnalysisStatus.APROVADO:
                    self.huggy.move_to_aprovado(chat_id)
                    self.huggy.start_auto_distribution(chat_id)
                    next_state = "FINISHED"

                if oferta.status == AnalysisStatus.SEM_AUTORIZACAO:
                    self.huggy.start_flow_authorization(chat_id)
                    next_state = "FINISHED"

                if oferta.status == AnalysisStatus.SEM_ADESAO:
                    self.huggy.start_auto_distribution(chat_id)
                    next_state = "FINISHED"

                if oferta.status == AnalysisStatus.MUDANCAS_CADASTRAIS:
                    self.huggy.finish_attendance(chat_id, tabulation_id=self.huggy.tabulations.get("MUDANCAS_CADASTRAIS"))
                    next_state = "FINISHED"
                
                if oferta.status == AnalysisStatus.ANIVERSARIANTE:
                    self.huggy.finish_attendance(chat_id, tabulation_id=self.huggy.tabulations.get("ANIVERSARIANTE"))
                    next_state = "FINISHED"
                
                if oferta.status == AnalysisStatus.SALDO_NAO_ENCONTRADO:
                    self.huggy.finish_attendance(chat_id, tabulation_id=self.huggy.tabulations.get("SALDO_NAO_ENCONTRADO"))
                    next_state = "FINISHED"

                if oferta.status == AnalysisStatus.SEM_SALDO:
                    self.huggy.finish_attendance(chat_id, tabulation_id=self.huggy.tabulations.get("SEM_SALDO"))
                    next_state = "FINISHED"
                
                if oferta.status == AnalysisStatus.LIMITE_EXCEDIDO_CONSULTAS_FGTS:
                    self.huggy.start_auto_distribution(chat_id)
                    next_state = "FINISHED"
                
                if oferta.status == AnalysisStatus.RETORNO_DESCONHECIDO:
                    self.huggy.start_auto_distribution(chat_id)
                    next_state = "FINISHED"
            
            else:
                # CPF INVÁLIDO (Lógica de retry)
                if current_state == "FGTS_AGUARDANDO_CPF":
                    self.huggy.send_message(chat_id, "cpf_invalid")
                    next_state = "FGTS_CPF_INVALIDO"
                else:
                    self.huggy.send_message(chat_id, "cpf_invalido_fallback", force_internal=True)
                    self.huggy.start_auto_distribution(chat_id)
                    next_state = "FINISHED"

        # 4. Estado Final
        elif current_state == "FINISHED":
            logger.info(f"Chat {chat_id} ignorado (Fluxo finalizado).")
            return

        # 5. Persistência
        if next_state != current_state:
            self.session.set_state(chat_id, next_state)
    
    def _handoff_human(self, chat_id: int):
        """Helper para transferir em caso de erro/imcompreensão"""
        self.huggy.send_message(chat_id, "atendente_fallback")
        self.huggy.start_auto_distribution(chat_id)