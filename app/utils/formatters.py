from datetime import datetime
from dateutil.relativedelta import relativedelta

def parse_valor_monetario(valor) -> float:
    if valor is None: return 0.0
    if isinstance(valor, (float, int)): return float(valor)
    if isinstance(valor, str):
        limpo = valor.replace("R$", "").strip()
        if "," in limpo:
            limpo = limpo.replace(".", "").replace(",", ".")
        try:
            return float(limpo)
        except ValueError:
            return 0.0
    return 0.0

def formatar_moeda(valor) -> str:
    """
    Converte float (1234.50) para formato BRL (R$ 1.234,50).
    Necessário pois o Service retorna float puro.
    """
    try:
        val = float(valor)
        us_fmt = f"{val:,.2f}"
        br_fmt = us_fmt.replace(',', 'X').replace('.', ',').replace('X', '.')
        return br_fmt
    except (ValueError, TypeError):
        return valor

def obter_mes_inicio_desconto() -> str:
    """
    Calcula o mês em que o cliente PERCEBE o desconto (Pagamento do Salário).
    Regra:
    - Até dia 20: Entra na folha do próximo mês -> Recebe/Desconta no mês seguinte (+2 meses do atual).
    - Após dia 21: Pula folha do próximo mês -> Recebe/Desconta 2 meses depois (+3 meses do atual).

    Exemplo (hoje = Janeiro):
    - Dia 20/01 -> Folha Fev -> Paga em MARÇO.
    - Dia 21/01 -> Folha Mar -> Paga em ABRIL.
    """
    agora = datetime.now()
    DIA_CORTE = 20

    meses_para_somar = 2 if agora.day <= DIA_CORTE else 3

    data_futura = agora + relativedelta(months=meses_para_somar)

    meses_pt = {
        1: "janeiro", 2: "fevereiro", 3: "março", 4: "abril",
        5: "maio", 6: "junho", 7: "julho", 8: "agosto",
        9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro"
    }
    
    return meses_pt[data_futura.month]