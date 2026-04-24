import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
from app.utils.validators import validate_cpf
from app.services.data_manager import DataManager

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
    
    return DataManager().get_nome_mes(data_futura.month)

def limpar_nome(nome: str) -> str:
    """
    Remove emojis, símbolos e caracteres especiais, mantendo apenas letras e espaços.
    """
    if not nome:
        return "Cliente (Nome não informado)"
    
    limpo = re.sub(r'[^a-zA-ZÀ-ÿ\s]', '', str(nome)).strip()

    if not limpo:
        return "Cliente (Nome não informado)"

    return limpo

def formatar_display_tempo(data_str: str) -> str:
    """
    Retorna string formatada ex: Retorna string formatada ex: "16/01/2023 (3 anos)" ou "08/08/2025 (5 meses)"
    """
    if not data_str: return "Data n/d"
    try:
        dt_adm = datetime.strptime(data_str, "%d/%m/%Y")
        dt_hoje = datetime.now()
        diff = relativedelta(dt_hoje, dt_adm)

        partes = []

        if diff.years > 0:
            s_ano = "anos" if diff.years > 1 else "ano"
            partes.append(f"{diff.years} {s_ano}")

        if diff.months > 0:
            s_mes = "meses" if diff.months > 1 else "mês"
            partes.append(f"{diff.months} {s_mes}")
        
        if not partes:
            texto_tempo = "menos de 1 mês"
        
        else:
            texto_tempo = " e ".join(partes)
        
        return f"{data_str} ({texto_tempo})"
    
    except Exception:
        return data_str

def calcular_meses(data_str):
    if not data_str: return 0
    try:
        data_admissao = datetime.strptime(data_str, "%d/%m/%Y")
        data_atual = datetime.now()
        diferenca = relativedelta(data_atual, data_admissao)
        meses_completos = diferenca.years * 12 + diferenca.months
        return max(0, meses_completos)
    except Exception:
        return 0
    
def formatar_cpf(cpf: str) -> str:
    """
    Formata CPF string pura (12345678900) para 123.456.789-00
    """
    if not cpf: return ""
    c = re.sub(r'\D', '', str(cpf))
    
    if len(c) != 11: 
        return cpf # Retorna original se não tiver 11 dígitos
        
    return f"{c[:3]}.{c[3:6]}.{c[6:9]}-{c[9:]}"

def formatar_telefone(telefone: str) -> str:
    if not telefone: return ""
    t = re.sub(r'\D', '', str(telefone))

    if t.startswith('55') and len(t) >= 12:
        t = t[2:] # remove os 2 primeiros caracteres
    
    # Se tem 10 (ex: 6992558501), vira 11 (69992558501)
    if len(t) == 10:
        t = f"{t[:2]}9{t[2:]}"
    
    # Se tem 11 (seja original ou transformado), aplica máscara
    if len(t) == 11:
        return f"({t[:2]}) {t[2:7]}-{t[7:]}"
    
    return telefone

def identificar_tipo_chave_pix(chave: str, cpf_cliente: str = "") -> str:
    """
    Analisa a string da chave PIX para determinar seu tipo.
    Retorna: 'EMAIL', 'ALEATORIA', 'CPF', 'TELEFONE' ou 'DESCONHECIDO'
    """
    if not chave:
        return "DESCONHECIDO"
    
    chave_limpa = str(chave).strip()

    if re.match(r"[^@]+@[^@]+\.[^@]+", chave_limpa):
        return "EMAIL"
    
    if re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", chave_limpa, re.IGNORECASE):
        return "ALEATORIA"
    
    apenas_numeros = re.sub(r'\D', '', chave_limpa)

    if len(apenas_numeros) == 11 and validate_cpf(apenas_numeros):
        return "CPF"
    
    if len(apenas_numeros) in [10, 11]:
        return "TELEFONE"
    
    if len(apenas_numeros) in [12, 13] and apenas_numeros.startswith("55"):
        return "TELEFONE"
    
    return "DESCONHECIDO"

def sanitizar_valor_pix(chave: str, tipo: str) -> str:
    """
    Remove sujeira (+55, pontos, traços) baseado no tipo da chave.
    """
    if not chave: return ""
    chave_str = str(chave).strip()

    if tipo == "CPF":
        return re.sub(r'\D', '', chave_str)
    
    if tipo == "TELEFONE":
        nums = re.sub(r'\D', '', chave_str)
        if nums.startswith('55') and len(nums) >= 12:
            return nums[2:]
        return nums
    
    return chave_str

def obter_codigo_tipo_chave_pix_facta(tipo: str) -> int:
    """Retorna o ID exigido pela API da Facta para cada tipo de chave."""
    mapa_codigo = {
        "CPF": 1,
        "TELEFONE": 2,
        "EMAIL": 3,
        "ALEATORIA": 4
    }
    return mapa_codigo.get(tipo, 0)