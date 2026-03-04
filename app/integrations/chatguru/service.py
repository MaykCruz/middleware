import logging
import os
from typing import Dict, Any, Optional
from app.services.bot.content.message_loader import MessageLoader
from app.integrations.chatguru.client import ChatGuruClient
from app.services.bot.memory.session import SessionManager

logger = logging.getLogger(__name__)

class ChatGuruService:
    def __init__(self, chat_id: str):
        self.client = ChatGuruClient()
        self.session = SessionManager()
        self.chat_id = chat_id

        contexto = self.session.get_context(chat_id)
        self.chat_number = contexto.get("celular") if contexto else None

        if not self.chat_number:
            logger.error(f"❌ [ChatGuruService] Falha crítica: Celular não encontrado no Redis para {chat_id}")

    def send_message(self, chat_id: str, message_key: str, variables: Optional[Dict[str, Any]] = None, force_internal: bool = False):
        """Envia mensagem para o cliente OU adiciona nota interna"""
        texto = ""
        if message_key == "blank":
            texto = variables.get("blank", "") if variables else ""
        else:
            msg_data = MessageLoader.get(message_key)
            template = ""
            if isinstance(msg_data, dict):
                template = msg_data.get("text", msg_data.get("texto", str(msg_data)))
            elif isinstance(msg_data, str):
                template = msg_data

            # Substitui as variáveis (ex: {nome}) se elas existirem
            try:
                texto = template.format(**(variables or {})) if variables else template
            except Exception as e:
                logger.warning(f"⚠️ Erro ao formatar variáveis na msg '{message_key}': {e}")
                texto = template
        
        if not texto:
            return False
    
        if force_internal:
            logger.info(f"📤 [ChatGuru API] Adicionando Nota Interna para {self.chat_number}")
            self.client.add_note(self.chat_number, texto)
        else:
            logger.info(f"📤 [ChatGuru API] Enviando Mensagem para {self.chat_number}")
            self.client.send_message(self.chat_number, texto)
        
        return True

    def start_put_in_queue(self, chat_number: str):
        """
        Delega o atendimento para o departamento, colocando o cliente em uma fila de atendimento.
        """
        DIALOG_ID_FILA = os.getenv("CHATGURU_DIALOG_ID_FILA")
        logger.info(f"👨‍💻 [ChatGuru API] Transferindo o Chat {chat_number} para a Fila de Atendimento Humano.")
        if self.chat_number:
            return self.client.execute_dialog(self.chat_number, DIALOG_ID_FILA)
        return False

    def start_auto_distribution(self, chat_number: str):
        """
        Delega o atendimento para algum atendente disponível, sem passar por fila.
        """
        DIALOG_ID_DISTRIBUICAO = os.getenv("CHATGURU_DIALOG_ID_DISTRIBUICAO")
        logger.info(f"✅ [ChatGuru API] Cliente Aprovado! Distribuindo Chat {chat_number} para a equipe de vendas.")
        if self.chat_number:
            return self.client.execute_dialog(self.chat_number, DIALOG_ID_DISTRIBUICAO)
        
    def finish_attendance(self, chat_number: str):
        """
        Finaliza o atendimento no ChatGuru chamando o diálogo de encerramento.
        """
        DIALOG_ID_ENCERRAMENTO = os.getenv("CHATGURU_DIALOG_ID_ENCERRAMENTO")
        logger.info(f"🏁 [ChatGuru API] Executando Diálogo de Encerramento ({DIALOG_ID_ENCERRAMENTO}) para o Chat {chat_number}")
        if self.chat_number:
            return self.client.execute_dialog(self.chat_number, DIALOG_ID_ENCERRAMENTO)
        return False
    
    def start_saldo_nao_encontrado(self, chat_number: str):
        """
        Executa o diálogo de Saldo não encontrado.
        """
        DIALOG_ID_SALDO_NAO_ENCONTRADO = os.getenv("CHATGURU_DIALOG_SALDO_NAO_ENCONTRADO")
        logger.info(f"📵 [ChatGuru API] Saldo não encontrado! Executando o diálogo no Chat {chat_number}.")
        if self.chat_number:
            return self.client.execute_dialog(self.chat_number, DIALOG_ID_SALDO_NAO_ENCONTRADO)

    def start_flow_digitacao_fgts(self, chat_number: str):
        """
        Executa o diálogo de Digitação FGTS.
        """
        DIALOG_ID_DIGITACAO_FGTS = os.getenv("CHATGURU_DIALOG_DIGITACAO_FGTS")
        logger.info(f"📵 [ChatGuru API] Digitação FGTS! Executando o diálogo no Chat {chat_number}.")
        if self.chat_number:
            return self.client.execute_dialog(self.chat_number, DIALOG_ID_DIGITACAO_FGTS)
    
    def start_flow_com_saldo_sem_conta(self, chat_number: str):
        """
        Executa o diálogo de com saldo sem conta.
        """
        DIALOG_ID_COM_SALDO_SEM_CONTA = os.getenv("CHATGURU_DIALOG_COM_SALDO_SEM_CONTA")
        logger.info(f"📵 [ChatGuru API] Com saldo sem conta FGTS! Executando o diálogo no Chat {chat_number}.")
        if self.chat_number:
            return self.client.execute_dialog(self.chat_number, DIALOG_ID_COM_SALDO_SEM_CONTA)
    
    def start_flow_authorization(self, chat_number: str):
        """
        Executa o diálogo de Sem autorização.
        """
        DIALOG_ID_SEM_AUTORIZACAO = os.getenv("CHATGURU_DIALOG_SEM_AUTORIZACAO")
        logger.info(f"📵 [ChatGuru API] Sem autorização! Executando o diálogo no Chat {chat_number}.")
        if self.chat_number:
            return self.client.execute_dialog(self.chat_number, DIALOG_ID_SEM_AUTORIZACAO)
    
    def start_flow_sem_adesao(self, chat_number: str):
        """
        Executa o diálogo de Sem adesão.
        """
        DIALOG_ID_SEM_ADESAO = os.getenv("CHATGURU_DIALOG_SEM_ADESAO")
        logger.info(f"📵 [ChatGuru API] Sem adesão! Executando o diálogo no Chat {chat_number}.")
        if self.chat_number:
            return self.client.execute_dialog(self.chat_number, DIALOG_ID_SEM_ADESAO)
    
    def start_flow_telefone_vinculado(self, chat_number: str):
        """
        Executa o diálogo de Telefone Vinculado Outro CPF.
        """
        DIALOG_ID_TELEFONE_VINCULADO_OUTRO_CPF = os.getenv("CHATGURU_DIALOG_TELEFONE_VINCULADO_OUTRO_CPF")
        logger.info(f"📵 [ChatGuru API] Iniciando Fluxo de Telefone Vinculado para o Chat {chat_number}")
        if self.chat_number:
            return self.client.execute_dialog(self.chat_number, DIALOG_ID_TELEFONE_VINCULADO_OUTRO_CPF)
    
    def preparar_mensagem_dialogo(self, message_key: str, variables: Optional[Dict[str, Any]] = None):
        """
        Formata a mensagem (com emojis, variáveis e quebras de linha) e salva 
        no campo personalizado 'mensagem_bot' do ChatGuru para ser usada em um Diálogo.
        """
        logger.info(f"📝 [ChatGuru] Preparando mensagem estática no campo 'mensagem_bot' (Chat {self.chat_number})")

        texto = ""
        if message_key == "blank":
            texto = variables.get("blank", "") if variables else ""
        else:
            msg_data = MessageLoader.get(message_key)
            template = ""
            if isinstance(msg_data, dict):
                template = msg_data.get("text", msg_data.get("texto", str(msg_data)))
            elif isinstance(msg_data, str):
                template = msg_data

            try:
                texto = template.format(**(variables or {})) if variables else template
            except Exception as e:
                logger.warning(f"⚠️ Erro ao formatar variáveis na msg '{message_key}': {e}")
                texto = template
        
        if not texto or not self.chat_number:
            return False
        
        return self.client.update_custom_fields(self.chat_number, {
            "Mensagem_Bot": texto
        })
    
    # MOCKS
    def start_flow_wait_term2(self, chat_number: str):
        logger.info(f"⏳ [ChatGuru API] Adicionando tag 'Aguardando Termo' ao Chat {chat_number}.")
    
    def start_flow_digitacao_clt(self, chat_number: str):
        logger.info(f"📝 [ChatGuru API] Iniciando Fluxo de Digitação CLT para o Chat {chat_number}")

    def start_flow_wait_term(self, chat_number: str):
        logger.info(f"⏳ [ChatGuru API] Iniciando Fluxo de Aguardando Termo (Loop 1) para o Chat {chat_number}")


    def move_to_simular_outros_bancos(self, chat_number: str):
        logger.info(f"🏦 [ChatGuru API] Movendo Chat {chat_number} para simulação em outros bancos.")
    
    def move_to_aprovado(self, chat_number: str):
        logger.info(f"🏦 [ChatGuru API] Movendo Chat {chat_number} para funil de aprovação.")

    def move_to_digitacao(self, chat_number: str):
        logger.info(f"🏦 [ChatGuru API] Movendo Chat {chat_number} para funil de digitação.")

    def transfer_maria_luiza(self, chat_number: str):
        logger.info(f"🏦 [ChatGuru API] Adicionando Maria Luiza no chat {chat_number} para suporte.")