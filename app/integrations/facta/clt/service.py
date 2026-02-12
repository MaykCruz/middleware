import logging
from datetime import datetime
from app.integrations.facta.clt.client import FactaCLTAdapter
from app.utils.formatters import parse_valor_monetario, formatar_display_tempo, calcular_meses, formatar_moeda

logger = logging.getLogger(__name__)

class FactaCLTService:

    def __init__(self):
        self.client = FactaCLTAdapter()
    
    def simular_clt(self, cpf: str, nome: str, celular: str, enviar_link_se_necessario: bool = True) -> dict:
        """
        Fluxo Otimista:
        1. Tenta Consultar Dados.
        2. Se der TOKEN_EXPIRADO -> Solicita Termo.
        3. Se não precisar de termo -> Segue fluxo normal.
        """
        resp_dados = self.client.consultar_dados_trabalhador(cpf)
        status_dados = resp_dados["status"]

        if status_dados == "PROCESSAMENTO_PENDENTE":
            return {
                "aprovado": False,
                "motivo": "PROCESSAMENTO_PENDENTE",
                "msg_tecnica": resp_dados.get("msg_original")
            }

        if status_dados == "TERMO_EXPIRADO":
            if not enviar_link_se_necessario:
                logger.info(f"⏳ [CLT] Token expirado, mas envio de link desativado (Reconsulta).")
                return {
                    "aprovado": False,
                    "motivo": "TERMO_AINDA_PENDENTE",
                    "msg_tecnica": "Cliente informou que autorizou, mas API Facta ainda retorna termo expirado."
                }

            logger.info(f"🔐 [CLT] Termo expirado para {cpf}. Solicitando novo termo...")

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
        
        matricula = trabalhador.get("matricula")

        if not matricula:
            logger.warning(f"⚠️ [CLT] CPF {cpf} sem matrícula retornada na consulta de dados. Abortando.")
            return {
                "aprovado": False,
                "motivo": "ERRO_TECNICO",
                "msg_tecnica": "Matrícula funcional não localizada nos dados do trabalhador."
            }
        
        margem = parse_valor_monetario(trabalhador.get("valorMargemDisponivel"))
        
        resp_politica = self.client.validar_politica_credito(
            cpf,
            matricula=trabalhador.get("matricula", ""),
            nascimento=trabalhador.get("dataNascimento"),
            admissao=trabalhador.get("dataAdmissao")
        )

        if resp_politica["status"] != "SUCESSO":

            if resp_politica["status"] == "REPROVADO_POLITICA_FACTA":
                data_admissao = trabalhador.get("dataAdmissao")
                tempo_trabalho = calcular_meses(data_admissao)
                texto_admissao = formatar_display_tempo(data_admissao)
            
                margem_minima_distribuicao = 100.00 if tempo_trabalho < 12 else 50.00

                if margem < margem_minima_distribuicao:
                    logger.info(f"🚫 [CLT] Reprovado Facta e margem baixa ({margem}). Min: {margem_minima_distribuicao}. Encerrando.")
                    return {
                        "aprovado": False,
                        "motivo": "SEM_MARGEM",
                        "msg_tecnica": f"Reprovado na política Facta e margem R$ {margem} insuficiente para transbordo (Mínimo: {margem_minima_distribuicao})."
                    }
                
                logger.info(f"🧐 [CLT] Reprovado Facta. Tempo de casa: {tempo_trabalho} meses.")
            
                if tempo_trabalho < 3:
                    return {
                        "aprovado": False,
                        "motivo": "MENOS_SEIS_MESES",
                        "msg_tecnica": "Tempo de trabalho inferior a 3 meses (Mínimo exigido pelo mercado)."
                    }
                
                data_inicio_empresa = trabalhador.get("dataInicioAtividadeEmpregador")
                tempo_empresa_meses = calcular_meses(data_inicio_empresa)

                logger.info(f"🏢 [CLT] Empresa iniciou em {data_inicio_empresa} ({tempo_empresa_meses} meses).")

                if tempo_empresa_meses < 24:
                    return {
                        "aprovado": False,
                        "motivo": "EMPRESA_RECENTE",
                        "msg_tecnica": f"Empresa empregadora muito recente ({tempo_empresa_meses} meses)."
                    }
                
                margem_disp = parse_valor_monetario(trabalhador.get("valorMargemDisponivel", 0))
                msg_base = resp_politica.get("msg_original", "Reprovado na política de crédito Facta.")

                msg_enriquecida = (
                    f"{msg_base}\n"
                    f"📊 *Dados para análise:*\n"
                    f"• Margem: R$ {formatar_moeda(margem_disp)}\n"
                    f"• Admissão: {texto_admissao}\n"
                    f"• Empresa: {formatar_display_tempo(data_inicio_empresa)}"
                )

                return {
                    "aprovado": False,
                    "motivo": resp_politica["status"],
                    "msg_tecnica": msg_enriquecida
                }
                
            return {
                "aprovado": False,
                "motivo": resp_politica["status"],
                "msg_tecnica": resp_politica.get("msg_original"),
                "dados_trabalhador": trabalhador
            }
        
        politica = resp_politica["dados"]

        salario = parse_valor_monetario(trabalhador.get("valorTotalVencimentos", 0))

        fator_comprometimento = self._definir_fator_margem(salario)

        parcela_maxima = round(margem * fator_comprometimento, 2)

        logger.debug(f"💰 [CLT] Salário: {salario} | Fator: {fator_comprometimento} | Margem Líq: {margem} -> Comprometida: {parcela_maxima}")

        return self._encontrar_melhor_tabela(cpf, trabalhador, politica, parcela_maxima, margem_real=margem)
    
    def _definir_fator_margem(self, salario: float) -> float:
        """
        Define a porcentagem da margem que pode ser utilizada baseada no salário.
        Regra:
        - Até 5.000: 97%
        - Entre 5.000 e 7.350: 90%
        - Acima de 7.350: 80%
        """
        if salario <= 5000.00:
            return 0.97
        elif salario <= 7350.00:
            return 0.90
        else:
            return 0.80
        
    def _validar_regras_basicas(self, dados: dict) -> dict:
        elegivel = str(dados.get("elegivel", "")).upper()
        if elegivel != "SIM":
            return {
                "ok": False,
                "motivo": "NAO_ELEGIVEL",
                "msg": f"Cliente marcado como não elegível na base Facta (Elegível: {elegivel})."
            }
        
        tipo_empregador = str(dados.get("inscricaoEmpregador_descricao", "CNPJ")).upper().strip()
        if tipo_empregador == "CPF":
            return {
                "ok": False,
                "motivo": "EMPREGADOR_CPF",
                "msg": "Empregador registrado como pessoa física (CPF), não permitido.",
            }
        
        try:
            qtd_contratos = int(dados.get("qtdEmprestimosAtivosSuspensos", 0))
        except ValueError:
            qtd_contratos = 0
        
        if qtd_contratos >= 9:
            return {
                "ok": False,
                "motivo": "LIMITE_CONTRATOS",
                "msg": f"Limite de contratos excedido: {qtd_contratos} ativos (Máx: 9).",
                "qtd_contratos": qtd_contratos
            }

        categoria = dados.get("codigoCategoriaTrabalhador")
        if categoria not in ["101", "102"]:
            return {"ok": False, "motivo": "CATEGORIA_CNAE_INVALIDA", "msg": "Categoria do trabalhador inválida.", "categoria": categoria}
        
        margem = parse_valor_monetario(dados.get("valorMargemDisponivel", 0))
        admissao = dados.get("dataAdmissao")

        idade = self._calcular_idade(dados.get("dataNascimento"))
        cod_sexo = str(dados.get("sexo_codigo", ""))
        sexo = "F" if cod_sexo == "3" else "M"

        aprovado_idade = False

        if sexo == "F":
            if 21 <= idade <= 57:
                aprovado_idade = True
        
        else:
            if 21 <= idade <= 62:
                aprovado_idade = True
        
        if not aprovado_idade:
            return {
                "ok": False,
                "motivo": "IDADE_INSUFICIENTE_FACTA", 
                "msg": f"Idade {idade} ({sexo}) fora da política Facta.", 
                "idade": idade,
                "sexo": sexo,
                "margem_disponivel": margem,
                "data_admissao": admissao,
                "dados_trabalhador": dados
            }

        if margem <= 20.00:
            return {"ok": False, "motivo": "SEM_MARGEM", "msg": f"Margem insuficiente: R$ {margem} (Mínimo R$ 20,01)", "margem": margem}
        
        return {"ok": True}
    
    def _encontrar_melhor_tabela(self, cpf, trab, politica, parcela_max, margem_real: float = 0.0) -> dict:
        """
        Busca operações e filtra estritamente pelo prazo e valor da política.
        """
        nasc = trab.get("dataNascimento")
        resp = self.client.buscar_operacoes(cpf, nasc, valor_parcela=parcela_max)

        oferta_encontrada = None
        motivo_falha = "SEM_OPERACOES"
        msg_falha = resp.get("msg_original", "Nenhuma tabela disponível.")

        if resp.get("status") == "SUCESSO":
            tabelas = resp["dados"].get("tabelas", [])

            try:
                prazo_politica = int(politica.get("prazo_maximo_disponivel", 0))
                teto_politica = float(politica.get("valor_maximo_disponivel", 0))
            except ValueError:
                return {"aprovado": False, "motivo": "ERRO_TECNICO", "msg_tecnica": "Erro na leitura dos valores da política."}

            tabelas_no_prazo = [t for t in tabelas if t.get("prazo") == prazo_politica]

            if tabelas_no_prazo:
                melhor_opcao = None

                tabelas_dentro_do_limite = [
                    t for t in tabelas_no_prazo
                    if float(t.get("valor_liquido", 0)) <= teto_politica
                ]

                grupo_para_analise = tabelas_dentro_do_limite if tabelas_dentro_do_limite else tabelas_no_prazo

                if not tabelas_dentro_do_limite:
                    logger.info(f"⚠️ [CLT] Todas as tabelas excedem o teto {teto_politica}. Selecionando a melhor para recalcular.")
                else:
                    logger.info(f"✅ [CLT] Encontradas {len(tabelas_dentro_do_limite)} tabelas dentro do teto. Aplicando regras de escolha.")
                
                tabelas_ordenadas = sorted(
                    grupo_para_analise,
                    key=lambda t: (
                        str(t.get("codigoTabela")) == "114300",
                        t.get("valor_seguro", 0) > 0,
                        float(t.get("valor_liquido", 0))
                    ),
                    reverse=True
                )
                melhor_opcao = tabelas_ordenadas[0]
                oferta_encontrada = melhor_opcao
                logger.info(f"🏆 [CLT] Tabela Eleita (Inicial): '{melhor_opcao.get('tabela')}' (Cód: {melhor_opcao.get('codigoTabela')}) | Líquido: {melhor_opcao.get('valor_liquido')}")
            else:
                motivo_falha = "SEM_PRAZO_COMPATIVEL"
                msg_falha = f"Tabelas encontradas, mas nenhuma para {prazo_politica} meses."
        
        if oferta_encontrada:
            valor_liberado = float(melhor_opcao.get("valor_liquido", 0))

            if valor_liberado > teto_politica:
                logger.info(f"💰 [CLT] Melhor opção ({valor_liberado}) excede teto {teto_politica}. Recalculando...")

                res_recalculo = self._recalcular_por_valor(cpf, nasc, prazo_politica, teto_politica, trab)
                
                if res_recalculo["aprovado"]:
                    return res_recalculo
                
                else:
                    logger.info(f"⚠️ [CLT] Falha no recálculo: {res_recalculo.get('msg_tecnica')}")
                    oferta_encontrada = None
                    motivo_falha = res_recalculo.get("motivo", "ERRO_RECALCULO")
                    msg_falha = res_recalculo.get("msg_tecnica", "Erro ao ajustar valor ao teto.")

        if not oferta_encontrada:
            data_admissao = trab.get("dataAdmissao")
            tempo_trabalho = calcular_meses(data_admissao)

            margem_minima_distribuicao = 100.00 if tempo_trabalho < 12 else 50.00
            if margem_real < margem_minima_distribuicao:
                return {
                    "aprovado": False,
                    "motivo": "SEM_MARGEM",
                    "msg_tecnica": f"Sem oferta Facta e margem R$ {margem_real} insuficiente para transbordo (Mínimo: {margem_minima_distribuicao})."
                }
            
            if tempo_trabalho < 3:
                return {
                    "aprovado": False,
                    "motivo": "MENOS_SEIS_MESES", 
                    "msg_tecnica": "Sem oferta Facta e tempo de trabalho inferior a 3 meses (Inviável para outros bancos)."
                }
            
            data_inicio_empresa = trab.get("dataInicioAtividadeEmpregador")
            tempo_empresa_meses = calcular_meses(data_inicio_empresa)

            if tempo_empresa_meses < 24:
                 return {
                    "aprovado": False,
                    "motivo": "EMPRESA_RECENTE",
                    "msg_tecnica": f"Sem oferta Facta e empresa muito recente ({tempo_empresa_meses} meses)." 
                }
            
            texto_admissao = formatar_display_tempo(data_admissao)
            texto_empresa = formatar_display_tempo(data_inicio_empresa)

            msg_enriquecida = (
                f"{msg_falha}\n"
                f"📊 *Dados para análise:*\n"
                f"• Margem: R$ {formatar_moeda(margem_real)}\n"
                f"• Admissão: {texto_admissao}\n"
                f"• Empresa: {texto_empresa}"
            )
            
            return {
                "aprovado": False,
                "motivo": motivo_falha,
                "msg_tecnica": msg_enriquecida,
                "dados_trabalhador": trab
            } 
        
        return {
            "aprovado": True,
            "motivo": "APROVADO",
            "oferta": {
                "valor_liquido": valor_liberado,
                "parcela": float(melhor_opcao.get("parcela")),
                "prazo": int(melhor_opcao.get("prazo")),
                "tabela_nome": melhor_opcao.get("tabela", "Padrão"),
                "codigo_tabela": melhor_opcao.get("codigoTabela"),
                "coeficiente": melhor_opcao.get("coeficiente"),
                "dados_trabalhador": trab # Repassando dados para eventual confirmação
            }
        }
    
    def _recalcular_por_valor(self, cpf, nasc, prazo, valor_teto, trab):
        """
        Refaz a simulação limitando pelo valor (Opção 1), mantendo o prazo.
        Baseado em operacoes_disponiveis_valor do api_facta.py.
        """
        resp = self.client.buscar_operacoes(cpf, nasc, valor_solicitado=valor_teto)

        msg_erro = str(resp.get("msg_original", "")).lower()

        if "nenhuma tabela" in msg_erro:
            logger.info(f"⚠️ [CLT] Recálculo recusado pela API (Valor muito baixo): {msg_erro}")
            return {"aprovado": False, "motivo": "SEM_OPERACOES", "msg_tecnica": "Nenhuma tabela disponível"}
        
        if resp["status"] != "SUCESSO":
             return {"aprovado": False, "motivo": "ERRO_RECALCULO", "msg_tecnica": resp.get("msg_original")}

        tabelas = resp["dados"].get("tabelas", [])
        
        # Filtra novamente pelo prazo (garantia)
        tabelas_no_prazo = [t for t in tabelas if t.get("prazo") == prazo]
        
        if not tabelas_no_prazo:
             return {"aprovado": False, "motivo": "ERRO_RECALCULO", "msg_tecnica": "Falha ao ajustar valor no prazo correto"}
        
        tabelas_dentro_do_limite = [
            t for t in tabelas_no_prazo
            if float(t.get("valor_liquido", 0)) <= valor_teto
        ]

        grupo_analise = tabelas_dentro_do_limite if tabelas_dentro_do_limite else tabelas_no_prazo

        tabelas_ordenadas = sorted(
            grupo_analise,
            key=lambda t: (
                str(t.get("codigoTabela")) == "114300",
                t.get("valor_seguro", 0) > 0, 
                float(t.get("valor_liquido", 0))
            ),
            reverse=True
        )

        melhor_opcao = tabelas_ordenadas[0]
        logger.info(f"♻️ [CLT] Tabela Eleita (Recálculo): '{melhor_opcao.get('tabela')}' (Cód: {melhor_opcao.get('codigoTabela')}) | Líquido Ajustado: {melhor_opcao.get('valor_liquido')}")
        
        return {
            "aprovado": True,
            "motivo": "APROVADO",
            "oferta": {
                "valor_liquido": float(melhor_opcao.get("valor_liquido")),
                "parcela": float(melhor_opcao.get("parcela")),
                "prazo": int(melhor_opcao.get("prazo")),
                "tabela_nome": melhor_opcao.get("tabela", "Padrão Ajustada"),
                "codigo_tabela": melhor_opcao.get("codigoTabela"),
                "coeficiente": melhor_opcao.get("coeficiente"),
                "dados_trabalhador": trab
            }
        }

    def _calcular_idade(self, data_str):
        if not data_str: return 0
        try:
            d = datetime.strptime(data_str, "%d/%m/%Y")
            return (datetime.today() -d).days // 365
        except: return 0