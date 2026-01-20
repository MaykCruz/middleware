# 🤖 Huggy Middleware (Empreste Digital)
Este projeto atual como um **Middleware de Orquestração** entre a plataforma de atendimento **Huggy** e as APIs de crédito (principalmente **Facta**).

O objetivo é automatizar a triagem, simulação e qualificação de leads para crédito consignado (FGTS e CLT) se forma assíncrona, garantindo alta disponibilidade e resiliência a falhas.

# 🏗️ Arquitetura e Fluxo de Dados
O sistema utiliza uma arquitetura baseada em eventos e filas para garantir que o atendimento ao cliente não trave, mesmo que as APIs bancárias estejam lentas.

```mermaid
graph TD
    User((Usuário)) -->|Envia msg| Huggy[Huggy Platform]
    Huggy -->|Webhook POST| API[FastAPI Webhook]
    
    subgraph "Middleware Core"
        API -->|Filtra Evento| Dispatcher[Event Dispatcher]
        Dispatcher -->|Enfileira Task| RedisQueue[(Redis Broker)]
        
        RedisQueue -->|Consome| Worker[Celery Worker]
        
        Worker -->|Gerencia Estado| Session[(Redis Cache)]
        Worker -->|Decide Fluxo| Engine[Bot Engine]
    end
    
    subgraph "Integrações"
        Worker -->|Simula Crédito| Facta[Facta API]
        Worker -->|Responde e Move Flow| Huggy
    end

    Facta --x|Erro ou Timeout| Worker
    Worker -->|Retry e Fallback| Huggy
```
***
# 🧠 Decisões Arquiteturais (O "Porquê")
**1. Processamento Assíncrono (Celery + Redis)**

**Por que?** As APIs de crédito (Facta) frequentemente demoram entre 10s a 40s para responder, e sofrem de instabilidades. **Decisão:**Não processamos nada na rota da API (`/webhook`). A API apenas recebe, valida e joga na fila. O Worker processa em background. Isso evita Timeouts da Huggy e mantém a API sempre responsiva.

**2. Resiliência e "At-Least-Once" Delivery**

**Configuração:** `acks_late=True` nas Tasks. **Por que?** Se um container do Worker for reiniciado (Deploy) ou "morrer" (OOM) enquanto processa um cliente, a tarefa **não é perdida**. O Redis devolve a tarefa para a fila e outro worker pega. **Efeito Colateral**: Em casos raros, o cliente pode receber uma mensagem duplicada, mas nunca ficará sem resposta.

**3. Gerencimaento de Concorrência (Lock Distribuido)**

**Arquivo**: `app/infrastructure/token_manager.py` **Problema:** A Facta invalida o token anterior se gerarmos um novo. Se 10 workers tentarem renovar o token ao mesmo tempo, todos falham. **Solução:** Implementamos um **Mutex Distribuido no Redis**. Apenas um worker consegue o "Lock" para renovar o token. Os outros aguardam (sleep) e usam o token gerado pelo líder.

**4. Micro-Retries e Blindagem SSL**

**Arquivo:** `app/integrations/facta/clt/client.py` **Problema:** Erros de `SSL: UNEXPECTED_EOF_WHILE_READING` ocorrem aleatóriamente na infraestrutura da Facta. **Solução:** Implementamos um loop de tentativas local (`for tentativa in range(3)`) com `time.sleep(1)` dentro do Client HTTP. Isso resolve falhas de milissegundos sem precisar reiniciar a task inteira do Celery.

**5. Mensagens Dinâmicas (Gist + Cache)**

**Problema:** Alterar um texto do bot exigia Deply da aplicação. **Solução:** As mensagens ficam em um JSON no GitHub Gist. O sistema usa um **Singleton com Cache TTL (`MenssageLoader`). Ele busca do Gist a cada 10 minutos ou via endpoint administrativo.

# 🛠️ Stack Tecnológica
* **Linguagem:** Python 3.11+
* **Web Framework:** FastAPI (Uvicorn)
* **Async Task Queue:** Celery 5.x
* **Broker & Caching:** Redis 7 (Alpine)
* **Monitoramento:** Better Stack (Longtail)
* **HTTP Client:** Httpx (com suporte a Proxy e Timeouts rígidos)

# ⚙️ Configuração e Execução

**Variáveis de Ambiente (.env)**

Crie um arquivo `.env` na raiz baseado nos exemplos abaixo:

```bash
# --- CORE ---
LOG_LEVEL=INFO
ADMIN_API_TOKEN=sua_senha_segura_para_limpar_cache

# --- HUGGY ---
HUGGY_API_TOKEN=seu_token_v3
HUGGY_COMPANY_ID=351946

# --- FILAS (REDIS) ---
CELERY_BROKEN_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# --- FACTA ---
FACTA_USER=usuario_api
FACTA_PASSWORD=senha_api
FACTA_API_URL=https://webservice-homol.facta.com.br
FACTA_PROXY_URL=http://user:pass@proxy.com:8080 (Opcional)

# --- CONTEÚDO ---
MESSAGES_URL=https://gist.githubusercontent.com/.../raw/messages.json
```

**Rodando com Docker**
```bash
# Subir todo o ambiente (API + Worker + Redis + Postgres)
docker-compose up --build -d

# Ver logs em tempo real
docker-compose logs -f
```

# 🔎 Monitoramento e Debug

**Logs (Better Stack)**

O sistema envia logs estruturados para o Better Stack.

* `INFO:` Fluxo nromal (Cliente, entrou, simulou, resultado).
* `WARNING:` Retentativas, falhas parciais de API, instabilidades.
* `ERROR:` Erros de lógica ou falhas de conexão persistentes.
* `CRITICAL:` Falha no Redis ou incapacidade de autodistribuição.

**Endpoint de Saúde**

* `GET /health/celery`: Verifica se os workers estão ativos e conectados ao Redis.

**Limpeza de Cache (Mensagens)**

Para atualizar os textos do bot sem redeploy:
```bash
curl -X POST http://localhost:8000/admin/refresh-messages \
     -H "x-admin-token: SUA_SENHA_DO_ENV"
```

# 🧪 Testes
 
 O projeto utiliza `pytest` com mocks para garantir que a lógica não dependa das APIs externas durante o desenvolvimento.
 ```bash
# Rodar testes
pytest -v

# Rodar testes de um arquivo específico
pytest tests/services/test_fgts_service.py
```

# 📝 Notas de Desenvolvimento

1. **Timeouts:** Nunca confie no timeout padrão das bibliotecas. O `httpx` está configurado com timeouts explícitos (30s a 60s) para evitar que workers fiquem presos (zumbis).

2. **Redis:** O Redis é configurado com `socket_timeout=5.0`. Se a conexão de rede cair, o worker falha rápido e reinicia, em vez de travar eternamente.

3. **Deploy:** Ao realizar deploy, o Celery tenta finalizar a tarefa atual. Se não conseguir a tempo, a task volta para a fila (graças ao `acks_late`).

## 📦 Estrutura de Pastas

* `app/core`: Configurações base (Logger, Timeouts).

* `app/events`: Roteamento e filtros de Webhooks (Dispatcher).

* `app/infrastructure`: Conexões de banco e filas (Celery, Redis TokenManager).

* `app/integrations`: Adaptadores para APIs externas (Facta, Huggy).

* `app/routers`: Endpoints da API (Webhooks, API Interna).

* `app/services`: Lógica de Negócio (Bot Engine, Regras de Produto).

* `app/tasks`: Definição dos Workers do Celery.