import calendar
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging

logger = logging.getLogger(__name__)

def agendar_para_data_fixa(chat_id: str, phone_id: str, data_str: str, motivo: str) -> bool:
    """
    Insere um agendamento no Supabase para uma data exata já calculada.
    data_str deve estar no formato DD/MM/YYYY.
    """
    if not data_str:
        return False
    
    try:
        data_dt = datetime.strptime(data_str, "%d/%m/%Y")

        fuso_br = ZoneInfo("America/Sao_Paulo")
        agora = datetime.now(fuso_br)

        data_agendamento = data_dt.replace(hour=8, minute=30, second=0, tzinfo=None)

        if data_agendamento < agora.replace(tzinfo=None):
            logger.warning(f"⚠️ [Agendamento Auto] Data fixa calculada já passou: {data_agendamento}")
            return False
        
        from app.infrastructure.database import supabase_client

        if not supabase_client:
            logger.error("❌ [Agendamento Auto] Supabase client não encontrado.")
            return False
        
        supabase_client.table("agendamentos").insert({
            "chat_id": chat_id,
            "phone_id": phone_id,
            "atendente": "agendamento_bot",
            "motivo": motivo,
            "data_agendada": data_agendamento.isoformat(),
            "status": "PENDENTE"
        }).execute()

        from app.integrations.chatguru.service import ChatGuruService
        chatguru = ChatGuruService(chat_id=chat_id, phone_id=phone_id)
        
        data_exibicao = data_agendamento.strftime('%d/%m/%Y às %H:%M')
        msg_interna = (
            f"🤖 *[BOT] Agendamento de Data Fixa*\n\n"
            f"📅 *Data:* {data_exibicao}\n"
            f"📝 *Motivo:* {motivo}\n"
            f"💡 _O bot reabrirá este chat automaticamente nesta data._"
        )
        
        chatguru.send_message(
            chat_id=chat_id, 
            message_key="blank", 
            variables={"blank": msg_interna}, 
            force_internal=True,
            delay=10
        )

        logger.info(f"✅ [Agendamento Auto] Chat {chat_id} agendado para data fixa: {data_agendamento.strftime('%d/%m/%Y às %H:%M')}")
        return True
        
    except Exception as e:
        logger.error(f"❌ [Agendamento Auto] Falha ao criar agendamento de data fixa: {e}")
        return False


def _adicionar_meses_precisao(data_base: datetime, meses: int) -> datetime:
    """
    Adiciona meses exatos a uma data lidando com calendários reais.
    Evita o erro clássico de somar meses em dias 31 para meses que vão até dia 30.
    """
    mes_novo = data_base.month - 1 + meses
    ano_novo = data_base.year + mes_novo // 12
    mes_novo = mes_novo % 12 + 1
    dia_novo = min(data_base.day, calendar.monthrange(ano_novo, mes_novo)[1])

    return data_base.replace(year=ano_novo, month=mes_novo, day=dia_novo)

def agendar_retentativa_automatica(chat_id: str, phone_id: str, data_admissao_str: str, meses_alvo: int = 3) -> bool:
    """
    Calcula "X meses exatos + 1 dia" e insere um agendamento no Supabase silenciosamente.
    data_admissao_str deve estar no formato DD/MM/YYYY.
    """
    if not data_admissao_str:
        return False
    
    try:
        admissao_dt = datetime.strptime(data_admissao_str, "%d/%m/%Y")
        data_futura = _adicionar_meses_precisao(admissao_dt, meses_alvo)

        fuso_br = ZoneInfo("America/Sao_Paulo")
        agora = datetime.now(fuso_br)

        data_agendamento = data_futura.replace(hour=8, minute=30, second=0, tzinfo=None)

        if data_agendamento < agora.replace(tzinfo=None):
            logger.warning(f"⚠️ [Agendamento Auto] Data calculada já passou: {data_agendamento}")
            return False
        
        from app.infrastructure.database import supabase_client

        if not supabase_client:
            logger.error("❌ [Agendamento Auto] Supabase client não encontrado.")
            return False
        
        supabase_client.table("agendamentos").insert({
            "chat_id": chat_id,
            "phone_id": phone_id,
            "atendente": "agendamento_bot",
            "motivo": f"Rentativa Auto: Cliente completou {meses_alvo} meses de empresa.",
            "data_agendada": data_agendamento.isoformat(),
            "status": "PENDENTE"
        }).execute()

        from app.integrations.chatguru.service import ChatGuruService
        chatguru = ChatGuruService(chat_id=chat_id, phone_id=phone_id)

        data_exibicao = data_agendamento.strftime('%d/%m/%Y às %H:%M')
        msg_interna = (
            f"🤖 *[BOT] Agendamento Automático Realizado*\n\n"
            f"📅 *Data:* {data_exibicao}\n"
            f"📝 *Motivo:* Cliente completará {meses_alvo} meses de registro.\n"
            f"💡 _O bot reabrirá este chat automaticamente nesta data._"
        )
        
        chatguru.send_message(
            chat_id=chat_id, 
            message_key="blank", 
            variables={"blank": msg_interna}, 
            force_internal=True,
            delay=10
        )

        logger.info(f"✅ [Agendamento Auto] Chat {chat_id} agendado para {data_agendamento.strftime('%d/%m/%Y às %H:%M')}")
        return True
        
    except Exception as e:
        logger.error(f"❌ [Agendamento Auto] Falha ao criar agendamento passivo: {e}")
        return False