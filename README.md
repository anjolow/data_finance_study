# Financial Data Engineering & Market Data Pipeline

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17%2B-blue?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Python](https://img.shields.io/badge/Python-3.12%2B-yellow?logo=python&logoColor=white)](https://www.python.org/)
[![AI](https://img.shields.io/badge/Google_Gemini-3.8_Flash-orange?logo=google&logoColor=white)](https://ai.google.dev/)
[![CI/CD](https://img.shields.io/badge/GitHub_Actions-ETL_Diário-green?logo=github-actions&logoColor=white)](https://github.com/features/actions)
[![License](https://img.shields.io/badge/License-MIT-lightgrey.svg)](LICENSE)

Pipeline de Engenharia de Dados para ingestão, processamento quantitativo e geração de inteligência de mercado com IA generativa em alta volumetria. Desenvolvido com foco em ambientes de **missão crítica**, **baixa latência**, **idempotência estrita** e **modelagem dimensional** para o mercado de capitais.

---

## 🏛️ Arquitetura do Projeto

```
                [ Alpha Vantage API ]
                          │
                          ▼ (HTTP GET / JSON)
               [ pipeline_cotacoes.py ]
                          │
         ┌────────────────┴────────────────┐
         │  1. Sincroniza Dim_Ativo        │
         │  2. Tipagem Estrita (Decimal)   │
         │  3. Bulk Load Idempotente       │
         └────────────────┬────────────────┘
                          ▼ (execute_values)
              ┌───────────────────────┐
              │      PostgreSQL       │
              │  ├── Dim_Ativo        │
              │  └── Fato_Cotacao     │◄────────┐
              └───────────┬───────────┘         │ (Leitura OHLCV Recente)
                          │                     │
                          ▼                     │
 [ RSS News (B3 / Macro) ] ──► [ pipeline_analise_mercado.py ] ─────┘
                                       │
                      ┌────────────────┴────────────────┐
                      │  1. Motor Quantitativo:         │
                      │     SMA 9/21, RSI 14, Volume    │
                      │  2. Scoring Sentimento Notícias │
                      │  3. Grounding com Gemini 3.8:   │
                      │     Roteiro Executivo + Sentim. │
                      │  4. Bulk Upsert JSONB           │
                      └────────────────┬────────────────┘
                                       ▼
                           ┌───────────────────────┐
                           │ Fato_Analise_Mercado  │
                           │ (GIN Index em JSONB)  │
                           └───────────────────────┘
```

---

## 💎 Decisões de Arquitetura e Engenharia

1. **Bulk Insert com `psycopg2.extras.execute_values`:**
   - Redução drástica de round-trips de rede via batching otimizado (`page_size=1000`).
   - Throughput exponencialmente superior a inserts individuais linha a linha.

2. **Idempotência Estrita (Upsert):**
   - Utilização de `ON CONFLICT ... DO UPDATE` em todas as tabelas fato (`Fato_Cotacao` e `Fato_Analise_Mercado`).
   - Reprocessamentos e re-execuções manuais ou por falha de rede nunca geram registros duplicados e atualizam eventuais correções pós-fechamento do pregão.

3. **Arquitetura Híbrida Data-to-Text (Quant + LLM sem Alucinações):**
   - **Abordagem C (Motor Quantitativo):** Cálculos matemáticos determinísticos (Médias Móveis SMA 9/21, IFR/RSI 14, Volume Ratio, Amplitude Intraday) executados diretamente sobre a série histórica.
   - **Abordagem B (Enriquecimento com IA Gemini 3.8 Flash):** O modelo recebe métricas exatas (*grounding*) e gera pareceres em linguagem natural (`roteiro_texto`) e metadados estruturados (`metadados_ia`), eliminando qualquer risco de alucinação de preços.
   - **Panorama Macro (`ticker = NULL`):** Visão consolidada do pregão do dia sintetizando a cesta de ativos monitorados.

4. **Inteligência de Mercado e Extração de Notícias em Tempo Real:**
   - **Ingestão RSS B3 & Macro:** Extração em tempo real de manchetes, fontes e links dos principais veículos financeiros (InfoMoney, Valor Econômico, Estadão E-Investidor), contornando limitações de planos pagos em APIs fechadas.
   - **Scoring Contínuo de Sentimento (-1.0 a +1.0):** Cada notícia é avaliada individualmente pelo Gemini 3.8 Flash, recebendo score numérico contínuo e classificação categórica (`Bullish`, `Bearish`, `Neutral`), além da consolidação de um termômetro geral de mídia no pregão.

5. **Colunas Semiestruturadas e Índices GIN:**
   - O campo `metadados_ia` armazena o payload da IA, notícias avaliadas e indicadores técnicos em formato `JSONB`.
   - Um índice `GIN` acelera consultas complexas por propriedades aninhadas em milissegundos (ex: busca por ativos com sentimento `bullish` ou `RSI > 70`).

6. **Infraestrutura em Nuvem & Compatibilidade IPv4:**
   - Conexão segura com banco gerenciado no Supabase via **Connection Pooler IPv4** (`aws-0-us-west-2.pooler.supabase.com`) e `sslmode="require"`, permitindo que os runners do GitHub Actions se conectem sem restrições de rede.

7. **Dashboard Interativo em Streamlit com Sincronização Temporal:**
   - Sincronização bidirecional de datas entre a análise macroeconômica e os pareceres individuais.
   - Exibição de notícias do pregão posicionadas diretamente abaixo dos gráficos técnicos, sincronizadas e atualizadas dinamicamente conforme a data selecionada.

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
- `ticker` (FK -> `Dim_Ativo.ticker`, `NULL` para fechamento macro de mercado)
- `fonte_origem` (VARCHAR(100))
- `roteiro_texto` (TEXT)
- `metadados_ia` (JSONB com sentimento, confiança, risco, drivers, indicadores quantitativos, notícias da mídia e score de sentimento)
- **Índice GIN:** `idx_fato_analise_json`
- **Índice de Idempotência:** `idx_fato_analise_idempotencia (COALESCE(ticker, 'GERAL'), data_analise, fonte_origem)`

---

## 🚀 Como Executar Localmente

### 1. Clonar o Repositório e Configurar Ambiente
```powershell
git clone https://github.com/anjolow/data_finance_study.git
cd data_finance_study

# Criação e ativação da venv (Windows)
py -m venv venv
.\venv\Scripts\Activate.ps1

# Instalação de dependências
pip install -r requirements.txt
```

### 2. Configurar as Variáveis de Ambiente
Copie o template de variáveis:
```powershell
cp .env.example .env
```
Preencha o `.env` com suas credenciais:
```ini
DB_HOST=aws-0-us-west-2.pooler.supabase.com
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres.seu_projeto
DB_PASSWORD=sua_senha
API_KEY=sua_chave_alphavantage
GEMINI_API_KEY=sua_chave_google_ai_studio
```

### 3. Execução dos Pipelines e Dashboard
```powershell
# 1. Ingestão de Cotações Diárias (Fato_Cotacao)
python pipeline_cotacoes.py

# 2. Análise Quantitativa + Enriquecimento com IA (Fato_Analise_Mercado)
python pipeline_analise_mercado.py

# 3. Execução do Dashboard Interativo no Navegador (Streamlit)
streamlit run app.py
```
*(O Streamlit abrirá automaticamente a interface web em `http://localhost:8501`)*

---

## 🔍 Exemplos de Consultas Avançadas (SQL + JSONB)

```sql
-- 1. Buscar todas as análises com sentimento Bullish usando o índice GIN
SELECT ticker, data_analise, metadados_ia->>'confianca_score' as confianca
FROM Fato_Analise_Mercado
WHERE metadados_ia @> '{"sentimento": "bullish"}';

-- 2. Filtrar ativos em sobrecompra crítica (RSI > 70) via JSONB aninhado
SELECT ticker, data_analise, metadados_ia->'indicadores_quant'->>'rsi_14' as rsi
FROM Fato_Analise_Mercado
WHERE ticker IS NOT NULL 
  AND (metadados_ia->'indicadores_quant'->>'rsi_14')::float > 70;

-- 3. Consultar o Panorama Geral de Mercado (Macro)
SELECT data_analise, roteiro_texto, metadados_ia->'resumo_estatistico' as stats
FROM Fato_Analise_Mercado
WHERE ticker IS NULL;

-- 4. Filtrar análises com sentimento positivo expressivo no noticiário (> 0.20)
SELECT ticker, data_analise, 
       metadados_ia->>'classificacao_sentimento_noticias' as sentimento_midia,
       metadados_ia->>'score_sentimento_noticias' as score_midia
FROM Fato_Analise_Mercado
WHERE (metadados_ia->>'score_sentimento_noticias')::float > 0.20;
```

---

## ⚙️ Configuração do CI/CD (GitHub Actions)

O workflow `.github/workflows/etl_diario.yml` roda automaticamente de segunda a sexta-feira às 22:00 UTC (19:00 BRT).

Cadastre as seguintes **Repository Secrets** no GitHub (**Settings > Secrets and variables > Actions**):

| Secret | Descrição | Exemplo |
| :--- | :--- | :--- |
| `DB_HOST` | Host do Connection Pooler IPv4 | `aws-0-us-west-2.pooler.supabase.com` |
| `DB_PORT` | Porta de conexão | `5432` |
| `DB_NAME` | Nome do banco | `postgres` |
| `DB_USER` | Usuário com ID do projeto | `postgres.seu_projeto` |
| `DB_PASSWORD` | Senha segura do banco | `********` |
| `API_KEY` | Chave da Alpha Vantage | `********` |
| `GEMINI_API_KEY` | Chave da Google AI Studio | `********` |

---

## 👨‍💻 Autor
Desenvolvido por **Lucas Anjos da Silva**.
Projeto de portfólio voltado para Engenharia de Dados, Finanças Quantitativas e IA Generativa.
