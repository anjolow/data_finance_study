# 📈 Financial Data Engineering & Market Data Pipeline

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%2B-blue?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Python](https://img.shields.io/badge/Python-3.11%2B-yellow?logo=python&logoColor=white)](https://www.python.org/)
[![CI/CD](https://img.shields.io/badge/GitHub_Actions-ETL_Diário-green?logo=github-actions&logoColor=white)](https://github.com/features/actions)
[![License](https://img.shields.io/badge/License-MIT-lightgrey.svg)](LICENSE)

Pipeline de Engenharia de Dados para ingestão, processamento e carga de cotações financeiras em alta volumetria. Desenvolvido com foco em ambientes de **missão crítica**, **baixa latência**, **idempotência estrita** e **integridade relacional** para o mercado de capitais.

---

## 🏛️ Arquitetura do Projeto

```
               [ Alpha Vantage API ]
                         │
                         ▼ (HTTP GET / JSON)
              [ pipeline_cotacoes.py ]
                         │
        ┌────────────────┴────────────────┐
        │  1. Sincroniza Dimensões        │
        │  2. Tipagem e Higienização     │
        │  3. Bulk Load Idempotente       │
        └────────────────┬────────────────┘
                         ▼ (psycopg2 execute_values)
            [ PostgreSQL: banco_financeiro ]
        ┌────────────────────────────────┐
        │ ├── Dim_Ativo (SCD / Dimensão) │
        │ ├── Fato_Cotacao (OHLCV)       │
        │ └── Fato_Analise_Mercado       │
        └────────────────────────────────┘
```

---

## 💎 Decisões de Arquitetura e Engenharia

1. **Bulk Insert com `psycopg2.extras.execute_values`:**
   - Redução drástica de round-trips de rede via batching otimizado (`page_size=1000`).
   - Throughput exponencialmente superior a inserts individuais linha a linha.

2. **Idempotência Estrita (Upsert):**
   - Utilização de `ON CONFLICT (ticker, data_pregao) DO UPDATE`.
   - Reprocessamentos e re-execuções manuais ou por falha de rede nunca geram registros duplicados e atualizam eventuais correções pós-fechamento do pregão.

3. **Garantia de Integridade Referencial:**
   - `Fato_Cotacao` possui foreign key para `Dim_Ativo(ticker)`.
   - O pipeline insere proativamente metadados na tabela dimensão (`ON CONFLICT (ticker) DO NOTHING`) antes da ingestão dos fatos, impedindo exceções de chave estrangeira em tempo de execução.

4. **Tratamento de Rate Limit e Contingência:**
   - Suporte nativo a ativos da B3 (sufixo `.SA`) e bolsas globais.
   - Pacing de requisições respeitando o rate limit da API financeira.
   - Fallback de contingência integrado para testes de desenvolvimento e validações offline.

5. **CI/CD Automatizado:**
   - Workflow agendado via GitHub Actions de segunda a sexta-feira após o fechamento dos mercados (22:00 UTC / 19:00 BRT).

---

## 🗄️ Modelo de Dados

### `Dim_Ativo`
Armazena os metadados cadastrais dos ativos negociados:
- `ticker` (PK, VARCHAR(10))
- `nome_empresa` (VARCHAR(100))
- `tipo_ativo` (VARCHAR(20))
- `setor` (VARCHAR(50))
- `data_criacao` (TIMESTAMPTZ)

### `Fato_Cotacao`
Histórico de negociação e série temporal OHLCV:
- `id` (BIGSERIAL PK)
- `ticker` (FK -> `Dim_Ativo.ticker`)
- `data_pregao` (DATE)
- `preco_abertura`, `preco_maximo`, `preco_minimo`, `preco_fechamento` (NUMERIC(15,4))
- `volume_negociado` (BIGINT)
- **Constraint:** `UNIQUE (ticker, data_pregao)`
- **Índices:** `idx_fato_cotacao_data`, `idx_fato_cotacao_ticker`

### `Fato_Analise_Mercado`
Registro de relatórios e metadados analíticos em formato semiestruturado:
- `id` (BIGSERIAL PK)
- `data_analise` (DATE)
- `ticker` (FK -> `Dim_Ativo.ticker`, opcional)
- `fonte_origem` (VARCHAR(100))
- `roteiro_texto` (TEXT)
- `metadados_ia` (JSONB com índice GIN para buscas ultra-rápidas)

---

## 🚀 Como Executar Localmente

### 1. Clonar o Repositório
```bash
git clone https://github.com/anjolow/data_finance_study.git
cd data_finance_study
```

### 2. Configurar o Ambiente Virtual Python
```bash
# Windows
py -m venv venv
.\venv\Scripts\activate

# Linux / MacOS
python3 -m venv venv
source venv/bin/activate
```

### 3. Instalar Dependências
```bash
pip install -r requirements.txt
```

### 4. Configurar as Variáveis de Ambiente
Copie o template de variáveis:
```bash
cp .env.example .env
```
Edite o arquivo `.env` com suas credenciais:
```ini
DB_HOST=localhost
DB_PORT=5432
DB_NAME=banco_financeiro
DB_USER=postgres
DB_PASSWORD=sua_senha
API_KEY=sua_chave_alphavantage
```

### 5. Executar o Pipeline
```bash
python pipeline_cotacoes.py
```

---

## ⚙️ Configuração do CI/CD (GitHub Actions)

Para que o workflow `.github/workflows/etl_diario.yml` funcione de forma autônoma na nuvem, adicione as seguintes **Repository Secrets** no GitHub (**Settings > Secrets and variables > Actions**):

| Secret | Descrição | Exemplo |
| :--- | :--- | :--- |
| `DB_HOST` | Host do banco de dados | `db.meuservidor.com` |
| `DB_PORT` | Porta de conexão do PostgreSQL | `5432` |
| `DB_NAME` | Nome do banco de dados | `banco_financeiro` |
| `DB_USER` | Usuário com permissão de DML | `postgres` |
| `DB_PASSWORD` | Senha segura do banco | `********` |
| `API_KEY` | Chave de API da Alpha Vantage | `********` |

---

## 👨‍💻 Autor
Desenvolvido por **Lucas Anjos da Silva**.
Projeto de portfólio voltado para Engenharia de Dados e Infraestrutura de Dados para o Mercado Financeiro.
