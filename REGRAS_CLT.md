# 📋 Regras de Negócio - Produto CLT

*Documento de Referência (Single Source of Truth)*
*Última atualização: Inclusão V8 e Presença*

---

## 1. Filtros de Entrada (Hard Fails)
Antes de qualquer simulação, o cliente é barrado se:

* **CPF Inválido:** Falha na validação matemática.
* **Elegibilidade Facta:** Campo `elegivel` na consulta deve ser estritamente "SIM".
* **Tipo de Empregador:** Descrição "CPF" (Pessoa Física) não é aceita.
* **Limite de Contratos:** Cliente possui **9 ou mais** contratos ativos/suspensos.
* **Categoria CNAE:** Apenas categorias **101** e **102** são aceitas.

---

## 2. Validação Cadastral & Margem (Facta)
Regras aplicadas durante a consulta na Facta (Parceiro Principal).

### Faixa Etária (Política Facta)
* **Mulheres:** 21 a 57 anos.
* **Homens:** 21 a 62 anos.
* *Nota: A Facta NÃO possui regra de tempo mínimo de empresa.*

### Margem Mínima
* **Regra Geral:** Margem < R$ 20,00 é recusada imediatamente (`SEM_MARGEM`), salvo se atender critérios de transbordo.

---

## 3. Lógica de Transbordo ("O Porteiro")
Quando a Facta recusa (por Política, Idade, Score, Sem Oferta), aplicamos um filtro para decidir se **vale a pena** tentar outro banco.

*Se o cliente não passar neste filtro, o atendimento é encerrado com Recusa Definitiva.*

1.  **Tempo de Casa (Admissão):**
    * Mínimo Global: **3 meses** (Regra do V8).

2.  **Tempo de Empresa (Existência do CNPJ):**
    * Mínimo Global: **3 meses** (Regra do V8).
    * *Antigamente a regra era 22 meses, mas foi reduzida para atender o V8.*

3.  **Margem Dinâmica (Corte de Qualidade):**
    * Tempo de Casa **>= 12 meses**: Aceita margem > **R$ 50,00**.
    * Tempo de Casa **< 12 meses**: Aceita margem > **R$ 150,00**.

---

## 4. Matriz de Sugestão de Bancos ("O Gerente")
Regras específicas para sugerir bancos na mensagem interna quando ocorre o transbordo.

| Banco | Idade | Tempo de Casa (Admissão) | Tempo de Empresa (CNPJ) |
| :--- | :--- | :--- | :--- |
| **Mercantil** | 20 a 58 anos | ≥ 12 meses | **≥ 36 meses** |
| **Presença** | 21 a 65 anos | ≥ 3 meses | **≥ 36 meses** |
| **C6 Bank** | 21 a 60 anos | ≥ 6 meses | **≥ 24 meses** |
| **V8** | 21 a 65 anos | ≥ 3 meses | **≥ 3 meses** |

---

## 5. Cálculo de Oferta (Se Aprovado Facta)
Fator de proteção de renda aplicado sobre a margem:

* **Salário <= R$ 5.000:** Usa 97% da margem.
* **Salário <= R$ 7.350:** Usa 90% da margem.
* **Salário > R$ 7.350:** Usa 80% da margem.

## 6. Ordenação e Escolha da Melhor Tabela (Filtro Facta)
Quando a API retorna múltiplas opções de crédito aprovadas, o sistema filtra estritamente pelo prazo liberado na política e, em seguida, aplica 4 regras de prioridade absolutas para escolher a oferta campeã (do maior para o menor peso):

1. **Fuga de Tabela Desvantajosa**: Rebaixa sumariamente qualquer tabela que tenha o prazo de **18 meses** atrelado a uma taxa exata de **5.99%**.

2. **Tabela Favorita (Prioridade Ouro)**: Força a escolha da tabela de código `(114389) 64106 - CLT NOVO GOLD 3PMT SB`

3. **Comissão Extra (Seguro)**: Dá preferência para tabelas que possuam seguro embutido (`valor_seguro > 0`).

4. **Desempate a Favor do Cliente**: Em caso de tabelas que empatem nos critérios acima, o sistema escolhe a que libera o **maior valor líquido**.

*Nota de Recálculo*: Se a melhor tabela escolhida ultrapassar o teto de crédito liberado pela política da Facta, o sistema recalcula buscando tabelas que não estourem o valor máximo, mas respeitando rigorosamente a mesma ordem de prioridades acima.