import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from app.integrations.facta.clt.client import FactaCLTAdapter
from app.utils.formatters import parse_valor_monetario

logger = logging.getLogger(__name__)

class FactaCLTService:
    MARGEM_COMPROMETIDA_FACTA = 0.90

    def __init__(self):
        self.client = FactaCLTAdapter()
    
    def simular_clt(self, cpf: str, nome: str, celular: str) -> dict:
        """
        Fluxo Otimista:
        1. Tenta Consultar Dados.
        2. Se der TOKEN_EXPIRADO -> Solicita Termo.
        3. Se não precisar de termo -> Segue fluxo normal.
        """
        resp_dados = self.client.consultar_dados_trabalhador(cpf)
        status_dados = resp_dados["status"]

        if status_dados == "TOKEN_EXPIRADO":
            logger.info(f"🔐 [CLT] Token expirado para {cpf}. Solicitando novo termo...")

            resp_termo = self.client.solicitar_termo(cpf, nome, celular)
            status_termo = resp_termo["status"]

            if status_termo == "TERMO_ENVIADO":
                return {
                    "aprovado": False,
                    "motivo": "AGUARDANDO_AUTORIZACAO",
                    "msg_tecnica": "Termo de autorização enviado via WhatsApp. Aguardando autorização do cliente."
                }
            
            if status_termo == "TELEFONE_VINCULADO_OUTRO_CPF":
                return {
                    "aprovado": False,
                    "motivo": "TELEFONE_VINCULADO_OUTRO_CPF",
                    "msg_tecnica": resp_termo.get("msg_original")
                }
            
            if status_termo == "TERMO_JA_AUTORIZADO":
                logger.info(f"🔄 [CLT] Token renovado/válido. Retentando consulta de dados...")
                resp_dados = self.client.consultar_dados_trabalhador(cpf)
                status_dados = resp_dados["status"]
            else:
                return {
                    "aprovado": False,
                    "motivo": "ERRO_TERMO",
                    "msg_tecnica": resp_termo.get("msg_original")
                }
        
        if status_dados != "SUCESSO":
            return {
                "aprovado": False,
                "motivo": status_dados,
                "msg_tecnica": resp_dados.get("msg_original")
            }

        lista_dados = resp_dados["dados"]
        if not lista_dados:
            return {"aprovado": False, "motivo": "SEM_DADOS", "msg_tecnica": "Lista de dados vazia."}
        
        trabalhador = lista_dados[0]

        validacao = self._validar_regras_basicas(trabalhador)
        if not validacao["ok"]:
            retorno = {
                "aprovado": False,
                "motivo": validacao["motivo"], 
                "msg_tecnica": validacao["msg"], 
                "dados": trabalhador
            }

            for chave, valor in validacao.items():
                if chave not in ["ok", "motivo", "msg"]:
                    retorno[chave] = valor
                    
            return retorno
        
        meses_servico = self._calcular_meses(trabalhador.get("dataAdmissao"))
        resp_politica = self.client.validar_politica_credito(
            cpf,
            meses=meses_servico,
            nascimento=trabalhador.get("dataNascimento"),
            sexo="M" if trabalhador.get("sexo_codigo") == "1" else "F"
        )

        if resp_politica["status"] != "SUCESSO":
            return {
                "aprovado": False,
                "motivo": "REPROVADO_POLITICA",
                "msg_tecnica": resp_politica.get("msg_original")
            }
        
        politica = resp_politica["dados"]
        margem = parse_valor_monetario(trabalhador.get("valorMargemDisponivel"))
        parcela_maxima = round(margem * self.MARGEM_COMPROMETIDA_FACTA, 2)

        return self._encontrar_melhor_tabela(cpf, trabalhador, politica, parcela_maxima)
        
    def _validar_regras_basicas(self, dados: dict) -> dict:

        categoria = dados.get("codigoCategoriaTrabalhador")
        if categoria not in ["101", "102"]:
            return {"ok": False, "motivo": "CATEGORIA_CNAE_INVALIDA", "msg": "Categoria do trabalhador inválida.", "categoria": categoria}
        
        idade = self._calcular_idade(dados.get("dataNascimento"))
        if idade < 21:
            return {"ok": False, "motivo": "IDADE_INSUFICIENTE_FACTA", "msg": f"Idade {idade} anos (Mínimo Facta: 21)", "idade": idade}
        
        margem = parse_valor_monetario(dados.get("valorMargemDisponivel", 0))
        if margem <= 0:
            return {"ok": False, "motivo": "SEM_MARGEM", "msg": "Margem zerada", "margem": margem}
        
        return {"ok": True}
    
    def _encontrar_melhor_tabela(self, cpf, trab, politica, parcela_max) -> dict:
        """
        Busca operações e filtra estritamente pelo prazo e valor da política.
        """
        nasc = trab.get("dataNascimento")
        resp = self.client.buscar_operacoes(cpf, nasc, valor_parcela=parcela_max)

        if resp["status"] != "SUCESSO":
            return {"aprovado": False, "motivo": "SEM_OPERACOES", "msg_tecnica": resp.get("msg_original")}
        
        tabelas = resp["dados"].get("tabelas", [])
        prazo_politica = int(politica.get("prazo", 0))
        teto_politica = float(politica.get("valor", 0))

        tabelas_no_prazo = [t for t in tabelas if t.get("prazo") == prazo_politica]

        if not tabelas_no_prazo:
            return {
                "aprovado": False,
                "motivo": "SEM_PRAZO_COMPATIVEL",
                "msg_tecnica": f"Nenhuma tabela encontrada para o prazo de {prazo_politica} meses"
            }
        
        melhor_opcao = None
        for tabela in tabelas_no_prazo:
            if tabela.get("valor_seguro", 0) > 0:
                melhor_opcao = tabela
                break
        
        if not melhor_opcao:
            melhor_opcao = tabelas_no_prazo[0]
        
        valor_liberado = float(melhor_opcao.get("valor_liquido", 0))

        if valor_liberado > teto_politica:
            logger.info(f"💰 [CLT] Valor {valor_liberado} excede teto {teto_politica}. Recalculando...")
            return self._recalcular_por_valor(cpf, nasc, prazo_politica, teto_politica)
        
        return {
            "aprovado": True,
            "motivo": "APROVADO",
            "oferta": {
                "valor_liquido": valor_liberado,
                "parcela": float(melhor_opcao.get("parcela")),
                "prazo": int(melhor_opcao.get("prazo")),
                "tabela_nome": melhor_opcao.get("descricao", "Padrão"),
                "codigo_tabela": melhor_opcao.get("codigoTabela"),
                "dados_bancarios": trab # Repassando dados para eventual confirmação
            }
        }
    
    def _recalcular_por_valor(self, cpf, nasc, prazo, valor_teto):
        """
        Refaz a simulação limitando pelo valor (Opção 1), mantendo o prazo.
        Baseado em operacoes_disponiveis_valor do api_facta.py.
        """
        resp = self.client.buscar_operacoes(cpf, nasc, valor_solicitado=valor_teto)
        
        if resp["status"] != "SUCESSO":
             return {"aprovado": False, "motivo": "ERRO_RECALCULO", "msg_tecnica": resp.get("msg_original")}

        tabelas = resp["dados"].get("tabelas", [])
        
        # Filtra novamente pelo prazo (garantia)
        tabelas_no_prazo = [t for t in tabelas if t.get("prazo") == prazo]
        
        if not tabelas_no_prazo:
             return {"aprovado": False, "motivo": "ERRO_RECALCULO", "msg_tecnica": "Falha ao ajustar valor no prazo correto"}

        # Mesma lógica de seleção (Prioriza Seguro)
        melhor_opcao = None
        for tabela in tabelas_no_prazo:
            if tabela.get("valor_seguro", 0) > 0:
                melhor_opcao = tabela
                break
        
        if not melhor_opcao:
            melhor_opcao = tabelas_no_prazo[0]
        
        return {
            "aprovado": True,
            "motivo": "APROVADO",
            "oferta": {
                "valor_liquido": float(melhor_opcao.get("valor_liquido")),
                "parcela": float(melhor_opcao.get("parcela")),
                "prazo": int(melhor_opcao.get("prazo")),
                "tabela_nome": melhor_opcao.get("descricao", "Padrão Ajustada"),
                "codigo_tabela": melhor_opcao.get("codigoTabela"),
                "dados_bancarios": {}
            }
        }

    def _calcular_idade(self, data_str):
        if not data_str: return 0
        try:
            d = datetime.strptime(data_str, "%d/%m/%Y")
            return (datetime.today() -d).days // 365
        except: return 0
    
    def _calcular_meses(self, data_str):
        if not data_str: return 0
        try:
            data_admissao = datetime.strptime(data_str, "%d/%m/%Y")
            data_atual = datetime.now()
            diferenca = relativedelta(data_atual, data_admissao)
            meses_completos = diferenca.years * 12 + diferenca.months

            return max(0, meses_completos)
        except Exception as e:
            logger.error(f"Erro ao calcular meses: {e}")
            return 0



    