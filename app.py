"""
Plataforma de Inteligencia de Mercado & Analise Quantitativa
Dashboard Interativo em Streamlit + Plotly
Engenharia de Dados & Finanças
"""

import os
import json
from datetime import datetime
import pandas as pd
import psycopg2
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# 1. CONFIGURAÇÃO DA PÁGINA E ESTILO VISUAL
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Market Intel Platform | Financial Data",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilização customizada para estética de terminal financeiro profissional
st.markdown("""
<style>
    .metric-card {
        background-color: #1E222D;
        border: 1px solid #2A2E39;
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 12px;
    }
    .badge-bullish {
        background-color: rgba(38, 166, 154, 0.2);
        color: #26A69A;
        border: 1px solid #26A69A;
        padding: 4px 10px;
        border-radius: 4px;
        font-weight: 700;
        display: inline-block;
    }
    .badge-bearish {
        background-color: rgba(239, 83, 80, 0.2);
        color: #EF5350;
        border: 1px solid #EF5350;
        padding: 4px 10px;
        border-radius: 4px;
        font-weight: 700;
        display: inline-block;
    }
    .badge-neutral {
        background-color: rgba(255, 179, 0, 0.2);
        color: #FFB300;
        border: 1px solid #FFB300;
        padding: 4px 10px;
        border-radius: 4px;
        font-weight: 700;
        display: inline-block;
    }
    .tag-driver {
        background-color: #2A2E39;
        color: #D1D4DC;
        padding: 3px 8px;
        border-radius: 12px;
        font-size: 0.82rem;
        margin-right: 6px;
        display: inline-block;
    }
    .news-item {
        background-color: #1E222D;
        border: 1px solid #2A2E39;
        border-left: 3px solid #3875F6;
        border-radius: 6px;
        padding: 10px 14px;
        margin-bottom: 8px;
    }
    .news-title {
        color: #E0E3EB !important;
        font-size: 0.88rem;
        font-weight: 600;
        text-decoration: none;
    }
    .news-title:hover {
        color: #2962FF !important;
        text-decoration: underline;
    }
    .news-meta {
        color: #848E9C;
        font-size: 0.78rem;
        margin-top: 5px;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 2. CONEXÃO COM O BANCO DE DADOS (SUPABASE / POSTGRESQL)
# -----------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"), override=True)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")


def get_connection():
    """Cria conexao com o PostgreSQL."""
    conn_kwargs = {
        "host": DB_HOST,
        "port": DB_PORT,
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "connect_timeout": 10
    }
    if DB_HOST not in ("localhost", "127.0.0.1"):
        conn_kwargs["sslmode"] = "require"
    return psycopg2.connect(**conn_kwargs)


@st.cache_data(ttl=300)
def carregar_dim_ativos():
    """Carrega lista de ativos cadastrados na Dim_Ativo."""
    try:
        with get_connection() as conn:
            query = "SELECT ticker, nome_empresa, tipo_ativo, setor FROM Dim_Ativo ORDER BY ticker;"
            df = pd.read_sql_query(query, conn)
            return df
    except Exception as e:
        st.error(f"Erro ao consultar Dim_Ativo: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def carregar_cotacoes(ticker: str):
    """Carrega historico de pregoes da Fato_Cotacao e calcula indicadores tecnicos."""
    try:
        with get_connection() as conn:
            query = """
                SELECT data_pregao, preco_abertura, preco_maximo, preco_minimo, preco_fechamento, volume_negociado
                FROM Fato_Cotacao
                WHERE ticker = %s
                ORDER BY data_pregao ASC;
            """
            df = pd.read_sql_query(query, conn, params=(ticker,))
            if df.empty:
                return df

            # Conversão de tipos
            df["data_pregao"] = pd.to_datetime(df["data_pregao"])
            for col in ["preco_abertura", "preco_maximo", "preco_minimo", "preco_fechamento"]:
                df[col] = df[col].astype(float)
            df["volume_negociado"] = df["volume_negociado"].fillna(0).astype(int)

            # Cálculo de Médias Móveis para o gráfico
            df["sma_9"] = df["preco_fechamento"].rolling(window=9).mean()
            df["sma_21"] = df["preco_fechamento"].rolling(window=21).mean()

            # Cálculo do IFR / RSI (14 períodos)
            delta = df["preco_fechamento"].diff()
            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)
            avg_gain = gain.rolling(window=14, min_periods=14).mean()
            avg_loss = loss.rolling(window=14, min_periods=14).mean()
            rs = avg_gain / avg_loss
            df["rsi_14"] = 100 - (100 / (1 + rs))

            return df
    except Exception as e:
        st.error(f"Erro ao consultar Fato_Cotacao: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def carregar_analises():
    """Carrega pareceres e metadados JSONB da Fato_Analise_Mercado."""
    try:
        with get_connection() as conn:
            query = """
                SELECT data_analise, ticker, fonte_origem, roteiro_texto, metadados_ia
                FROM Fato_Analise_Mercado
                ORDER BY data_analise DESC, ticker NULLS LAST;
            """
            df = pd.read_sql_query(query, conn)
            if not df.empty:
                df["data_analise"] = pd.to_datetime(df["data_analise"])
            return df
    except Exception as e:
        st.error(f"Erro ao consultar Fato_Analise_Mercado: {e}")
        return pd.DataFrame()


# -----------------------------------------------------------------------------
# 3. BARRA LATERAL (SIDEBAR) & FILTROS
# -----------------------------------------------------------------------------
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/bullish.png", width=64)
    st.title("Market Intel Platform")
    st.markdown("**Engenharia de Dados & IA**")
    st.caption("Plataforma de Mercado Financeiro & Séries Temporais")
    st.divider()

    df_ativos = carregar_dim_ativos()
    df_analises = carregar_analises()

    if not df_ativos.empty:
        opcoes_tickers = df_ativos["ticker"].tolist()
        ticker_selecionado = st.selectbox(
            "Selecione o Ativo para Análise:",
            options=opcoes_tickers,
            index=0
        )
        ativo_info = df_ativos[df_ativos["ticker"] == ticker_selecionado].iloc[0]
        total_analises_ticker = len(df_analises[df_analises["ticker"] == ticker_selecionado]) if not df_analises.empty else 0
        st.info(f"**{ativo_info['nome_empresa']}**\n\nSetor: {ativo_info['setor']}\nTipo: {ativo_info['tipo_ativo']}\n\n🤖 *{total_analises_ticker} pareceres de IA no histórico*")
    else:
        ticker_selecionado = "PETR4"
        st.warning("Nenhum ativo encontrado em Dim_Ativo.")

    st.divider()

    # Informações de Infraestrutura
    st.markdown("### Infraestrutura")
    st.markdown(f"**Banco:** PostgreSQL 17 (Supabase)")
    st.markdown(f"**Host:** `{DB_HOST[:15]}...`")
    st.markdown(f"**Motor IA:** `Gemini 3.8 Flash`")
    st.markdown(f"**Status Conexão:** 🟢 Conectado")

    st.divider()
    if st.button("🔄 Atualizar Dados (Limpar Cache)"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("[Repositório no GitHub](https://github.com/anjolow/data_finance_study)")


# -----------------------------------------------------------------------------
# 4. CABEÇALHO & SEÇÃO MACROECONÔMICA (PANORAMA DO PREGÃO)
# -----------------------------------------------------------------------------
st.title("Plataforma de Inteligência Quantitativa & IA de Mercado")
st.markdown("Pipeline ETL de alta volumetria em PostgreSQL integrado com motor quantitativo e IA generativa.")

# Sincronização bidirecional de datas entre Macro e Cenário do Ativo
datas_disponiveis = sorted(df_analises["data_analise"].dt.date.unique(), reverse=True) if not df_analises.empty else []

if "data_analise_sync" not in st.session_state and datas_disponiveis:
    st.session_state["data_analise_sync"] = datas_disponiveis[0]

if "sel_data_macro_sync" not in st.session_state and datas_disponiveis:
    st.session_state["sel_data_macro_sync"] = datas_disponiveis[0]

if "sel_data_ativo_sync" not in st.session_state and datas_disponiveis:
    st.session_state["sel_data_ativo_sync"] = datas_disponiveis[0]

def sync_macro_to_ativo():
    nova_data = st.session_state["sel_data_macro_sync"]
    st.session_state["data_analise_sync"] = nova_data
    st.session_state["sel_data_ativo_sync"] = nova_data

def sync_ativo_to_macro():
    nova_data = st.session_state["sel_data_ativo_sync"]
    st.session_state["data_analise_sync"] = nova_data
    st.session_state["sel_data_macro_sync"] = nova_data

# Busca análise Macro Geral (ticker IS NULL)
analise_macro = None
if not df_analises.empty:
    macro_matches = df_analises[df_analises["ticker"].isna()]
    if not macro_matches.empty:
        datas_macro = sorted(macro_matches["data_analise"].dt.date.unique(), reverse=True)

        with st.expander("🌐 **PANORAMA GERAL DO PREGÃO (Fechamento Macro com IA)**", expanded=True):
            col_m_info, col_m_date = st.columns([2.5, 1.5])
            with col_m_date:
                idx_macro = datas_macro.index(st.session_state["sel_data_macro_sync"]) if st.session_state.get("sel_data_macro_sync") in datas_macro else 0
                data_macro_sel = st.selectbox(
                    "📅 Data do Fechamento Macro:",
                    options=datas_macro,
                    index=idx_macro,
                    format_func=lambda d: f"{d.strftime('%d/%m/%Y')} (Mais Recente)" if d == datas_macro[0] else d.strftime("%d/%m/%Y"),
                    key="sel_data_macro_sync",
                    on_change=sync_macro_to_ativo,
                    help="Selecione a data para visualizar o fechamento macroeconômico daquele pregão."
                )

            analise_macro = macro_matches[macro_matches["data_analise"].dt.date == data_macro_sel].iloc[0]

            meta_macro = analise_macro["metadados_ia"]
            if isinstance(meta_macro, str):
                meta_macro = json.loads(meta_macro)

            sentimento_macro = meta_macro.get("sentimento", "neutral").upper()
            classe_badge = f"badge-{sentimento_macro.lower()}"
            stats = meta_macro.get("resumo_estatistico", {})

            col_m1, col_m2, col_m3, col_m4 = st.columns(4)
            with col_m1:
                st.markdown(f"**Sentimento do Mercado:** <br><span class='{classe_badge}'>{sentimento_macro}</span>", unsafe_allow_html=True)
            with col_m2:
                st.metric("Retorno Médio da Cesta", f"{stats.get('retorno_medio_cesta_pct', 0.0):+.2f}%")
            with col_m3:
                st.metric("Balanço de Ações", f"{stats.get('altas', 0)} Altas / {stats.get('baixas', 0)} Baixas")
            with col_m4:
                st.metric("Apetite a Risco", meta_macro.get("apetite_a_risco", "Moderado").capitalize())

            st.markdown(f"**Data de Referência:** {analise_macro['data_analise'].strftime('%d/%m/%Y')} | **Fonte:** *{analise_macro['fonte_origem']}*")
            st.markdown(f"> *\"{analise_macro['roteiro_texto']}\"*")

            noticias_macro = meta_macro.get("noticias_mercado", [])
            if noticias_macro:
                st.markdown("##### Notícias & Sentimento da Mídia (Mercado Geral)")
                score_macro = meta_macro.get("score_sentimento_noticias")
                classif_macro = meta_macro.get("classificacao_sentimento_noticias")
                if score_macro is not None and classif_macro:
                    badge_class = f"badge-{classif_macro.lower()}"
                    st.markdown(f"**Score da Mídia:** <span class='{badge_class}'>{classif_macro.upper()} ({score_macro:+.2f})</span>", unsafe_allow_html=True)

                for n in noticias_macro:
                    sent_n = (n.get("label_sentimento") or n.get("sentimento") or "neutral").upper()
                    badge_n = f"badge-{sent_n.lower()}"
                    score_n = float(n.get("score_sentimento") if n.get("score_sentimento") is not None else n.get("score", 0.0))
                    titulo = n.get("titulo", "Sem título")
                    url = n.get("url", "#")
                    fonte = n.get("fonte", "Mídia")
                    data_pub = n.get("data_publicacao", "")
                    data_str = f" • {data_pub}" if data_pub else ""

                    st.markdown(f"""
                    <div class='news-item'>
                        <a href='{url}' target='_blank' class='news-title'>🔗 {titulo}</a>
                        <div class='news-meta'>
                            <span class='{badge_n}' style='font-size: 0.72rem; padding: 2px 6px;'>{sent_n} ({score_n:+.2f})</span>
                            <span style='margin-left: 8px;'>Fonte: <b>{fonte}</b>{data_str}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

st.divider()


# -----------------------------------------------------------------------------
# 5. VISÃO DETALHADA DO ATIVO SELECIONADO
# -----------------------------------------------------------------------------
df_cotacoes = carregar_cotacoes(ticker_selecionado)

if df_cotacoes.empty:
    st.warning(f"Nenhuma cotação encontrada para {ticker_selecionado}.")
else:
    ultimo_pregao = df_cotacoes.iloc[-1]
    penultimo_pregao = df_cotacoes.iloc[-2] if len(df_cotacoes) > 1 else ultimo_pregao

    fechamento_atual = ultimo_pregao["preco_fechamento"]
    fechamento_anterior = penultimo_pregao["preco_fechamento"]
    var_dia = ((fechamento_atual - fechamento_anterior) / fechamento_anterior) * 100 if fechamento_anterior else 0.0

    # Linha de Métricas Rápidas
    col_kpi1, col_kpi2, col_kpi3, col_kpi4, col_kpi5 = st.columns(5)
    with col_kpi1:
        st.metric(f"Preço de Fechamento ({ticker_selecionado})", f"R$ {fechamento_atual:.2f}", f"{var_dia:+.2f}%")
    with col_kpi2:
        st.metric("Máxima do Dia", f"R$ {ultimo_pregao['preco_maximo']:.2f}")
    with col_kpi3:
        st.metric("Mínima do Dia", f"R$ {ultimo_pregao['preco_minimo']:.2f}")
    with col_kpi4:
        vol_fmt = f"{ultimo_pregao['volume_negociado'] / 1e6:.1f}M" if ultimo_pregao['volume_negociado'] >= 1e6 else f"{ultimo_pregao['volume_negociado']:,}"
        st.metric("Volume Financeiro", vol_fmt)
    with col_kpi5:
        rsi_val = ultimo_pregao["rsi_14"]
        st.metric("IFR / RSI (14)", f"{rsi_val:.1f}" if pd.notnull(rsi_val) else "N/A")

    st.write("")

    # -------------------------------------------------------------------------
    # Pré-resolução da Análise e Data do Ativo Selecionado (antes dos blocos visuais)
    # -------------------------------------------------------------------------
    analise_ativo = None
    matches_ativo = pd.DataFrame()
    datas_ativo = []
    data_ativo_sel = None

    if not df_analises.empty:
        matches_ativo = df_analises[df_analises["ticker"] == ticker_selecionado]
        if not matches_ativo.empty:
            datas_ativo = sorted(matches_ativo["data_analise"].dt.date.unique(), reverse=True)
            # Garante que a data sincronizada seja válida para o ativo
            if st.session_state.get("sel_data_ativo_sync") not in datas_ativo:
                st.session_state["sel_data_ativo_sync"] = datas_ativo[0]

            data_ativo_sel = st.session_state["sel_data_ativo_sync"]
            filtro_data = matches_ativo[matches_ativo["data_analise"].dt.date == data_ativo_sel]
            if not filtro_data.empty:
                analise_ativo = filtro_data.iloc[0]

    meta_ativo = None
    if analise_ativo is not None:
        meta_ativo = analise_ativo["metadados_ia"]
        if isinstance(meta_ativo, str):
            meta_ativo = json.loads(meta_ativo)

    # Layout Principal: Gráfico Técnico vs Parecer da IA
    col_grafico, col_ia = st.columns([1.5, 1])

    with col_grafico:
        st.subheader("Gráfico Técnico & Volume (OHLCV)")

        # Criação de Subplots: Linha 1 = Candlestick, Linha 2 = Volume, Linha 3 = RSI
        fig = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.09,
            row_heights=[0.56, 0.22, 0.22],
            subplot_titles=("<b>Preço e Médias Móveis (SMA 9 e 21)</b>", "<b>Volume de Negociação</b>", "<b>IFR / RSI (14)</b>")
        )

        # 1. Candlestick
        fig.add_trace(
            go.Candlestick(
                x=df_cotacoes["data_pregao"],
                open=df_cotacoes["preco_abertura"],
                high=df_cotacoes["preco_maximo"],
                low=df_cotacoes["preco_minimo"],
                close=df_cotacoes["preco_fechamento"],
                name="OHLC",
                increasing_line_color="#26A69A",
                decreasing_line_color="#EF5350"
            ),
            row=1, col=1
        )

        # 2. Médias Móveis
        fig.add_trace(
            go.Scatter(x=df_cotacoes["data_pregao"], y=df_cotacoes["sma_9"], name="SMA 9 (Curta)", line=dict(color="#2962FF", width=1.5)),
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(x=df_cotacoes["data_pregao"], y=df_cotacoes["sma_21"], name="SMA 21 (Média)", line=dict(color="#FF6D00", width=1.5)),
            row=1, col=1
        )

        # 3. Barras de Volume
        cores_volume = ["#26A69A" if c >= o else "#EF5350" for c, o in zip(df_cotacoes["preco_fechamento"], df_cotacoes["preco_abertura"])]
        fig.add_trace(
            go.Bar(x=df_cotacoes["data_pregao"], y=df_cotacoes["volume_negociado"], name="Volume", marker_color=cores_volume, showlegend=False),
            row=2, col=1
        )

        # 4. RSI (14) com faixas de sobrecompra e sobrevenda
        fig.add_trace(
            go.Scatter(x=df_cotacoes["data_pregao"], y=df_cotacoes["rsi_14"], name="RSI 14", line=dict(color="#AB47BC", width=1.5), showlegend=False),
            row=3, col=1
        )
        fig.add_hline(y=70, line_dash="dash", line_color="#EF5350", line_width=1, row=3, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="#26A69A", line_width=1, row=3, col=1)

        # Remove dias sem pregão (fins de semana e feriados) para eliminar espaços em branco
        todas_datas = pd.date_range(start=df_cotacoes["data_pregao"].min(), end=df_cotacoes["data_pregao"].max())
        datas_pregao = set(df_cotacoes["data_pregao"].dt.strftime("%Y-%m-%d"))
        datas_sem_pregao = [d.strftime("%Y-%m-%d") for d in todas_datas if d.strftime("%Y-%m-%d") not in datas_pregao]

        fig.update_xaxes(
            rangebreaks=[
                dict(values=datas_sem_pregao)
            ]
        )

        # Alinha e estiliza títulos dos subplots em negrito e com maior destaque
        for annotation in fig["layout"]["annotations"]:
            annotation.update(
                x=0.0,
                xanchor="left",
                yshift=10,
                font=dict(size=16, color="#E0E0E0")
            )

        fig.update_layout(
            template="plotly_dark",
            height=760,
            margin=dict(l=20, r=20, t=50, b=25),
            xaxis_rangeslider_visible=False,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0, font=dict(size=11))
        )
        st.plotly_chart(fig, width="stretch")

        # ---------------------------------------------------------------------
        # Notícias do Dia do Ativo Selecionado (Abaixo dos Gráficos)
        # ---------------------------------------------------------------------
        st.markdown(f"#### Notícias do Dia ({ticker_selecionado})")
        if analise_ativo is not None and meta_ativo:
            noticias_ativo = meta_ativo.get("noticias_mercado", [])
            score_noticias = meta_ativo.get("score_sentimento_noticias")
            classif_noticias = meta_ativo.get("classificacao_sentimento_noticias")
            data_str = data_ativo_sel.strftime('%d/%m/%Y') if data_ativo_sel else ""

            if score_noticias is not None and classif_noticias:
                badge_class = f"badge-{classif_noticias.lower()}"
                st.markdown(
                    f"**Data de Referência:** {data_str} &nbsp;|&nbsp; **Sentimento Médio da Mídia:** <span class='{badge_class}'>{classif_noticias.upper()} ({score_noticias:+.2f})</span>",
                    unsafe_allow_html=True
                )
            elif data_str:
                st.markdown(f"**Data de Referência:** {data_str}")

            if noticias_ativo:
                for n in noticias_ativo:
                    sent_n = (n.get("label_sentimento") or n.get("sentimento") or "neutral").upper()
                    badge_n = f"badge-{sent_n.lower()}"
                    score_n = float(n.get("score_sentimento") if n.get("score_sentimento") is not None else n.get("score", 0.0))
                    titulo = n.get("titulo", "Sem título")
                    url = n.get("url", "#")
                    fonte = n.get("fonte", "Mídia")
                    data_pub = n.get("data_publicacao", "")
                    data_pub_str = f" • {data_pub}" if data_pub else ""

                    st.markdown(f"""
                    <div class='news-item'>
                        <div style='display: flex; justify-content: space-between; align-items: flex-start; gap: 12px;'>
                            <a href='{url}' target='_blank' class='news-title'>🔗 {titulo}</a>
                            <span class='{badge_n}' style='font-size: 0.72rem; padding: 2px 6px; white-space: nowrap;'>{sent_n} ({score_n:+.2f})</span>
                        </div>
                        <div class='news-meta'>
                            Fonte: <b>{fonte}</b>{data_pub_str}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info(f"Nenhuma notícia registrada para {ticker_selecionado} na data {data_str}.")
        else:
            st.info(f"Nenhuma análise ou notícia registrada para {ticker_selecionado} na data selecionada.")

    with col_ia:
        st.subheader("🤖 Parecer de Mercado com IA Generativa")

        if not matches_ativo.empty:
            idx_ativo = datas_ativo.index(data_ativo_sel) if data_ativo_sel in datas_ativo else 0
            st.selectbox(
                "📅 Selecione a Data do Cenário:",
                options=datas_ativo,
                index=idx_ativo,
                format_func=lambda d: f"{d.strftime('%d/%m/%Y')} (Mais Recente)" if d == datas_ativo[0] else d.strftime("%d/%m/%Y"),
                key="sel_data_ativo_sync",
                on_change=sync_ativo_to_macro,
                help="Escolha uma data para visualizar o parecer e os cenários gerados pela IA naquele dia."
            )

        if analise_ativo is not None and meta_ativo:
            sentimento = meta_ativo.get("sentimento", "neutral").upper()
            classe_sent = f"badge-{sentimento.lower()}"
            confianca = float(meta_ativo.get("confianca_score", 0.0))
            risco = meta_ativo.get("risco_curto_prazo", "medio").upper()

            st.markdown(f"""
            <div class='metric-card'>
                <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;'>
                    <span class='{classe_sent}'>{sentimento}</span>
                    <span style='color: #848E9C; font-size: 0.85rem;'>Confiança: <b>{confianca*100:.0f}%</b></span>
                    <span style='color: #848E9C; font-size: 0.85rem;'>Risco: <b>{risco}</b></span>
                </div>
                <div style='margin-bottom: 12px;'>
                    {''.join([f"<span class='tag-driver'>#{d}</span>" for d in meta_ativo.get('drivers', [])])}
                </div>
                <p style='color: #D1D4DC; font-size: 0.95rem; line-height: 1.6;'>
                    {analise_ativo['roteiro_texto']}
                </p>
                <div style='color: #5E6673; font-size: 0.8rem; border-top: 1px solid #2A2E39; padding-top: 8px;'>
                    Data da Análise: {analise_ativo['data_analise'].strftime('%d/%m/%Y')} | Fonte: {analise_ativo['fonte_origem']}
                </div>
            </div>
            """, unsafe_allow_html=True)

            # Tabela de Indicadores Quantitativos Grounded
            ind_quant = meta_ativo.get("indicadores_quant", {})
            if ind_quant:
                st.markdown("##### Indicadores Calculados pelo Motor Quantitativo")
                st.dataframe(
                    pd.DataFrame([
                        {"Indicador": "SMA 9 (Média Rápida)", "Valor": f"R$ {ind_quant.get('sma_9', 0):.2f}"},
                        {"Indicador": "SMA 21 (Média Intermediária)", "Valor": f"R$ {ind_quant.get('sma_21', 0):.2f}"},
                        {"Indicador": "IFR / RSI 14", "Valor": f"{ind_quant.get('rsi_14', 0):.2f}"},
                        {"Indicador": "Volume Ratio", "Valor": f"{ind_quant.get('volume_ratio', 0):.2f}x da média"},
                        {"Indicador": "Diagnóstico Técnico", "Valor": ind_quant.get('tendencia', 'Neutro')}
                    ]),
                    hide_index=True,
                    width="stretch"
                )

            score_noticias = meta_ativo.get("score_sentimento_noticias")
            classif_noticias = meta_ativo.get("classificacao_sentimento_noticias")
            if score_noticias is not None and classif_noticias:
                st.caption(f"Sentimento Médio das Notícias ({data_ativo_sel.strftime('%d/%m/%Y')}): **{classif_noticias.upper()} ({score_noticias:+.2f})** — matérias detalhadas exibidas abaixo do gráfico.")
        else:
            st.info(f"Nenhuma análise de IA registrada ainda para {ticker_selecionado}.")

    # -------------------------------------------------------------------------
    # 6. TABELA HISTÓRICA OHLCV RECENTE
    # -------------------------------------------------------------------------
    with st.expander(f"📋 Visualizar Série Histórica Bruta ({ticker_selecionado})"):
        df_exibicao = df_cotacoes.sort_values(by="data_pregao", ascending=False).copy()
        df_exibicao["data_pregao"] = df_exibicao["data_pregao"].dt.strftime("%d/%m/%Y")
        st.dataframe(
            df_exibicao[["data_pregao", "preco_abertura", "preco_maximo", "preco_minimo", "preco_fechamento", "volume_negociado", "rsi_14", "sma_9", "sma_21"]],
            width="stretch",
            hide_index=True
        )
        csv = df_exibicao.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Baixar Dados em CSV", data=csv, file_name=f"cotacoes_{ticker_selecionado}.csv", mime="text/csv")
