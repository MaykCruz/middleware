import re
from datetime import datetime, timedelta
import holidays
from app.services.data_manager import DataManager

def validate_cpf(cpf: str) -> bool:
    """
    Valida se um CPF é válido (algoritmo de módulo 11).
    Retorna True se válido, False se inválido.
    """
    # Remove caracteres não numéricos
    cpf = clean_digits(cpf)

    if len(cpf) != 11:
        return False

    # Verifica se todos os dígitos são iguais (ex: 111.111.111-11 é inválido mas passa no cálculo)
    if cpf == cpf[0] * len(cpf):
        return False

    # Cálculo do primeiro dígito verificador
    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    resto = (soma * 10) % 11
    if resto == 10: resto = 0
    if resto != int(cpf[9]):
        return False

    # Cálculo do segundo dígito verificador
    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    resto = (soma * 10) % 11
    if resto == 10: resto = 0
    if resto != int(cpf[10]):
        return False

    return True

def clean_digits(text: str) -> str:
    """Retorna apenas os números da string"""
    return re.sub(r'[^0-9]', '', text)

def formatar_data_br(data: datetime) -> str:
    """Formata a data como 'DD de mês_extenso'."""
    nome_mes = DataManager().get_nome_mes(data.month)
    return f"{data.day} de {nome_mes}"

def calcular_segundo_dia_util_mes(mes, ano):
    """Calcula o 2º dia útil de um mês específico"""
    # Garante que feriados estaduais não quebrem (usando apenas BR federal por padrão)
    feriados = holidays.Brazil(years=[ano]) 
    data = datetime(ano, mes, 1)
    dias_uteis = 0
    
    while True:
        weekday = data.weekday() # 0 = Seg, 6 = Dom
        dia_do_mes = data.day
        eh_feriado = data in feriados
        
        # Regra de dia útil (Seg-Sex e não feriado)
        e_dia_util_comum = (weekday < 5 and not eh_feriado)
        
        # Sua regra específica: Sábado (5) conta se for dia 02?
        # Mantive sua lógica original aqui:
        e_sabado_especifico = (weekday == 5 and dia_do_mes == 2 and not eh_feriado)

        if e_dia_util_comum or e_sabado_especifico:
            dias_uteis += 1
            
        if dias_uteis == 2:
            return data
            
        data += timedelta(days=1)

def calcular_segundo_dia_util_prox_mes():
    hoje = datetime.now()
    
    # Calcular o 2º dia útil do mês atual
    segundo_dia_util_mes_atual = calcular_segundo_dia_util_mes(hoje.month, hoje.year)
    
    # Lógica de decisão
    if hoje.date() <= segundo_dia_util_mes_atual.date(): # Comparar apenas datas (.date()) é mais seguro
        data_resultado = segundo_dia_util_mes_atual
    else:
        # Pula para próximo mês
        mes_proximo = hoje.month + 1
        ano_proximo = hoje.year
        
        if mes_proximo > 12: # Correção simples para virada de ano
            mes_proximo = 1
            ano_proximo += 1
            
        data_resultado = calcular_segundo_dia_util_mes(mes_proximo, ano_proximo)

        data_amigavel = formatar_data_br(data_resultado)
        data_banco_dados = data_resultado.strftime("%d/%m/%Y")

        return data_amigavel, data_banco_dados
    
    # Retorna formatado sem depender do locale do Windows/Linux
    return formatar_data_br(data_resultado)

def formatar_telefone_br(telefone: str) -> str | None:
    """
    Padroniza telefones brasileiros para o formato 55 + DDD + 9 + 8 dígitos (13 digitos).
    Retorna o número limpo e formatado ou None se for inválido.
    """
    if not telefone:
        return None
    
    tel = clean_digits(telefone)

    if len(tel) == 13:
        if tel.startswith("55"):
            return tel
        return None
    
    if len(tel) == 12 and tel.startswith("55"):
        return f"{tel[:4]}9{tel[4:]}"
    
    if len(tel) == 11:
        return f"55{tel}"
    
    if len(tel) == 10:
        return f"55{tel[:2]}9{tel[2:]}"
    
    return None
