"""
Pipeline de Inteligencia de Mercado Hibrido (Quantitativo + IA Generativa)
Projeto de Engenharia de Dados / Mercado Financeiro

Responsabilidades:
1. Motor Quantitativo (Abordagem C):
   - Extrai series historicas recentes de Fato_Cotacao.
   - Calcula indicadores tecnicos matematicos deterministicos:
     * Retorno diario (%) e Amplitude Intraday (%)
     * Medias Moveis Simples (SMA 9 e SMA 21)
     * Indice de Forca Relativa / IFR (RSI 14 periodos)
     * Volume Ratio (Volume atual vs. Media Movel de Volume 20)
     * Classificacao de momento tecnico (Golden/Death cross, Sobrecompra/Sobrevenda).

2. Enriquecimento com IA Generativa (Abordagem B):
   - Prompt estruturado ao Google Gemini (gemini-3.8-flash) com schema JSON estrito.
   - Gera sintese executiva em linguagem natural (roteiro_texto) ancorada nos indicadores quantitativos.
   - Gera metadados de mercado (sentimento, score de confianca, risco, drivers).
   - Gera analise consolidada do mercado macro/geral (ticker = NULL).

3. Persistencia Idempotente no PostgreSQL:
   - Bulk upsert na tabela Fato_Analise_Mercado via psycopg2.extras.execute_values.
   - Suporte nativo a colunas JSONB e buscas aceleradas por indice GIN.
"""

import os
import sys
import json
import logging
from datetime import datetime, date
from decimal import Decimal
import urllib.parse
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
import requests
import psycopg2
from psycopg2.extras import execute_values, Json
from dotenv import load_dotenv

# Configuracao de Logging Estruturado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("Analise_Mercado_IA")

# Carregamento seguro das variaveis de ambiente
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"), override=True)

# Configuracoes do Banco de Dados
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "banco_financeiro")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Configuracoes da API do Gemini
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-3.8-flash"
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

# Fonte padrao para rastreabilidade
FONTE_ORIGEM_PADRAO = "Pipeline Quant + Gemini 3.8 Flash"


def obter_conexao():
    """Cria e retorna uma conexao segura com o PostgreSQL (local ou Supabase)."""
    try:
        conn_kwargs = {
            "host": DB_HOST,
            "port": DB_PORT,
            "dbname": DB_NAME,
            "user": DB_USER,
            "password": DB_PASSWORD,
            "connect_timeout": 15
        }
        if DB_HOST not in ("localhost", "127.0.0.1"):
            conn_kwargs["sslmode"] = "require"

        conn = psycopg2.connect(**conn_kwargs)
        return conn
    except Exception as err:
        logger.error("Falha critica ao conectar com PostgreSQL em %s:%s - %s", DB_HOST, DB_PORT, err)
        raise


# ==============================================================================
# 1. MOTOR QUANTITATIVO (ABORDAGEM C)
# ==============================================================================

def buscar_historico_cotacoes(conn, ticker: str, limite: int = 30) -> list[dict]:
    """
    Busca o historico recente de pregoes de um ativo ordenado cronologicamente.
    Retorna lista de dicionarios com data, OHLCV.
    """
    query = """
        SELECT data_pregao, preco_abertura, preco_maximo, preco_minimo, preco_fechamento, volume_negociado
        FROM Fato_Cotacao
        WHERE ticker = %s
        ORDER BY data_pregao ASC;
    """
    with conn.cursor() as cur:
        cur.execute(query, (ticker,))
        rows = cur.fetchall()

    if not rows:
        return []

    # Se houver mais registros que o limite, pega os ultimos N
    rows = rows[-limite:]

    historico = []
    for r in rows:
        historico.append({
            "data_pregao": r[0],
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": int(r[5]) if r[5] is not None else 0
        })
    return historico


def calcular_indicadores_tecnicos(historico: list[dict]) -> dict:
    """
    Calcula indicadores tecnicos deterministicos sobre a serie temporal OHLCV:
    - Retorno diário (%)
    - Amplitude intraday (%)
    - Médias Móveis Simples (SMA 9 e SMA 21)
    - IFR / RSI (14 períodos)
    - Volume Ratio (vs média móvel de 20 períodos)
    - Status de tendência técnica
    """
    if len(historico) < 2:
        raise ValueError("Historico insuficiente para calculo de indicadores tecnicos.")

    n = len(historico)
    ultimo = historico[-1]
    penultimo = historico[-2]

    fechamento_atual = ultimo["close"]
    fechamento_anterior = penultimo["close"]

    # 1. Variação Diária e Amplitude Intraday
    variacao_dia_pct = round(((fechamento_atual - fechamento_anterior) / fechamento_anterior) * 100, 2)
    abertura_atual = ultimo["open"] if ultimo["open"] > 0 else fechamento_atual
    amplitude_intraday_pct = round(((ultimo["high"] - ultimo["low"]) / abertura_atual) * 100, 2)

    # 2. Médias Móveis Simples (SMA 9 e SMA 21)
    closes = [h["close"] for h in historico]
    sma_9 = round(sum(closes[-9:]) / len(closes[-9:]), 2) if n >= 9 else round(sum(closes) / n, 2)
    sma_21 = round(sum(closes[-21:]) / len(closes[-21:]), 2) if n >= 21 else round(sum(closes) / n, 2)

    # 3. IFR / RSI (14 períodos - Método Clássico Wilder)
    rsi_14 = 50.0  # valor neutro default se historico for curto
    if n >= 15:
        deltas = [closes[i] - closes[i - 1] for i in range(1, n)]
        deltas_14 = deltas[-14:]
        ganhos = [d for d in deltas_14 if d > 0]
        perdas = [abs(d) for d in deltas_14 if d < 0]

        avg_gain = sum(ganhos) / 14 if ganhos else 0.0
        avg_loss = sum(perdas) / 14 if perdas else 0.0

        if avg_loss == 0:
            rsi_14 = 100.0
        elif avg_gain == 0:
            rsi_14 = 0.0
        else:
            rs = avg_gain / avg_loss
            rsi_14 = round(100.0 - (100.0 / (1.0 + rs)), 2)

    # 4. Volume Ratio (Volume atual vs Média de 20 períodos)
    volumes = [h["volume"] for h in historico]
    vol_slice = volumes[-20:] if n >= 20 else volumes
    media_volume_20 = sum(vol_slice) / len(vol_slice) if vol_slice else 1
    volume_ratio = round(ultimo["volume"] / media_volume_20, 2) if media_volume_20 > 0 else 1.0

    # 5. Classificação do Momento Técnico
    if rsi_14 >= 70:
        condicao_rsi = "Sobrecomprado (Alerta de Correcao)"
    elif rsi_14 <= 30:
        condicao_rsi = "Sobrevendido (Oportunidade de Repique)"
    else:
        condicao_rsi = "Neutro"

    if sma_9 > sma_21 and fechamento_atual >= sma_9:
        tendencia = "Alta (Medias Alinhadas e Preco Acima)"
    elif sma_9 < sma_21 and fechamento_atual <= sma_9:
        tendencia = "Baixa (Pressao Vendedora e Medias Alinhadas para Baixo)"
    else:
        tendencia = "Consolidacao / Lateralizacao"

    return {
        "data_pregao": ultimo["data_pregao"],
        "preco_fechamento": fechamento_atual,
        "variacao_dia_pct": variacao_dia_pct,
        "amplitude_intraday_pct": amplitude_intraday_pct,
        "sma_9": sma_9,
        "sma_21": sma_21,
        "rsi_14": rsi_14,
        "condicao_rsi": condicao_rsi,
        "volume_atual": ultimo["volume"],
        "volume_ratio": volume_ratio,
        "tendencia": tendencia
    }


# ==============================================================================
# 1.1 EXTRATOR DE NOTÍCIAS FINANCEIRAS B3 (FEEDS RSS EM TEMPO REAL)
# ==============================================================================

def buscar_noticias_b3(termo_busca: str, limite: int = 3) -> list[dict]:
    """
    Extrai notícias financeiras recentes em tempo real via feeds RSS públicos (B3 / Mercado Brasileiro).
    Retorna lista de dicionários contendo:
    - titulo: Manchete limpa
    - fonte: Veículo de comunicação (ex: InfoMoney, Valor Econômico, Estadão)
    - url: Link para a matéria original
    - data_publicacao: Data/hora formatada
    """
    noticias = []
    try:
        query = urllib.parse.quote(f"{termo_busca} mercado financeiro")
        url = f"https://news.google.com/rss/search?q={query}&hl=pt-BR&gl=BR&ceid=BR:pt-419"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            logger.warning("Feed de notícias retornou HTTP %d para '%s'.", resp.status_code, termo_busca)
            return []

        root = ET.fromstring(resp.content)
        items = root.findall("./channel/item")

        for item in items[:limite]:
            titulo_raw = item.find("title").text if item.find("title") is not None else ""
            link = item.find("link").text if item.find("link") is not None else ""
            pub_date_raw = item.find("pubDate").text if item.find("pubDate") is not None else ""
            source_el = item.find("source")
            fonte = source_el.text if source_el is not None and source_el.text else "Mercado"

            if " - " in titulo_raw:
                partes = titulo_raw.rsplit(" - ", 1)
                titulo = partes[0].strip()
                if fonte == "Mercado":
                    fonte = partes[1].strip()
            else:
                titulo = titulo_raw.strip()

            data_pub = ""
            if pub_date_raw:
                try:
                    dt = parsedate_to_datetime(pub_date_raw)
                    data_pub = dt.strftime("%d/%m/%Y %H:%M")
                except Exception:
                    data_pub = pub_date_raw[:16]

            noticias.append({
                "titulo": titulo,
                "fonte": fonte,
                "url": link,
                "data_publicacao": data_pub
            })

        logger.info("Coletadas %d notícias para '%s'.", len(noticias), termo_busca)
    except Exception as err:
        logger.warning("Falha ao coletar notícias RSS para '%s': %s", termo_busca, err)

    return noticias


# ==============================================================================
# 2. ENRIQUECIMENTO COM IA GENERATIVA (ABORDAGEM B - GEMINI 3.8 FLASH)
# ==============================================================================

def gerar_analise_ia_gemini(ticker: str, meta_ativo: dict, metricas: dict, noticias: list[dict] = None) -> tuple[str, dict]:
    """
    Envia as metricas quantitativas exatas e as manchetes recentes para a API do Gemini.
    Retorna uma tupla (roteiro_texto: str, metadados_ia: dict) com scoring de sentimento e notícias.
    """
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY nao configurada. Utilizando sintetizador analitico interno.")
        return _gerar_analise_fallback(ticker, meta_ativo, metricas, noticias)

    bloco_noticias = ""
    if noticias:
        itens_txt = [f"{i}. [{n['fonte']}] \"{n['titulo']}\"" for i, n in enumerate(noticias, 1)]
        bloco_noticias = "\nNOTÍCIAS E FATOS RELEVANTES RECENTES:\n" + "\n".join(itens_txt) + "\n"

    prompt = f"""
Você é um Engenheiro de Dados e Analista Quantitativo sênior do mercado financeiro brasileiro.
Analise os seguintes indicadores técnicos EXATOS e OFICIAIS do ativo {ticker} ({meta_ativo.get('nome', '')}), setor {meta_ativo.get('setor', '')}, referente ao pregão de {metricas['data_pregao']}:

MÉTRICAS QUANTITATIVAS:
- Preço de Fechamento: R$ {metricas['preco_fechamento']:.2f}
- Variação do Dia: {metricas['variacao_dia_pct']:+.2f}%
- Amplitude Intraday: {metricas['amplitude_intraday_pct']:.2f}%
- Média Móvel Curta (SMA 9): R$ {metricas['sma_9']:.2f}
- Média Móvel Média (SMA 21): R$ {metricas['sma_21']:.2f}
- IFR / RSI (14 períodos): {metricas['rsi_14']:.2f} ({metricas['condicao_rsi']})
- Volume vs Média 20 períodos: {metricas['volume_ratio']:.2f}x
- Tendência Técnica Detectada: {metricas['tendencia']}
{bloco_noticias}
INSTRUÇÕES OBRIGATÓRIAS:
Gere uma resposta em JSON com duas propriedades:
1. "roteiro_texto": Texto fluido, analítico e formal em português (2 parágrafos concisos) descrevendo o comportamento do papel no pregão. Correlacione os números técnicos com os fatos e catalisadores noticiosos listados (se houver), citando suportes, resistências e pressão compradora/vendedora.
2. "metadados_ia": Um objeto JSON contendo:
   - "sentimento": "bullish" (alta), "bearish" (baixa) ou "neutral" (neutro).
   - "confianca_score": número float entre 0.0 e 1.0.
   - "risco_curto_prazo": "baixo", "medio" ou "alto".
   - "drivers": lista com 3 a 5 palavras-chave resumindo os fatores determinantes (ex: ["volume_acima_media", "recorde_producao"]).
   - "score_sentimento_noticias": float entre -1.0 e +1.0 representando o sentimento médio ponderado das notícias sobre o ativo (0.0 se neutro/sem notícias).
   - "classificacao_sentimento_noticias": "Bullish", "Bearish" ou "Neutral".
   - "noticias_avaliadas": lista de objetos para cada notícia fornecida contendo:
       * "indice": número inteiro (1, 2, ...) correspondente à notícia fornecida
       * "score_sentimento": float entre -1.0 e +1.0
       * "label_sentimento": "Bullish", "Bearish" ou "Neutral"
"""

    payload = {
        "contents": [
            {
                "parts": [{"text": prompt}]
            }
        ],
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0.2
        }
    }

    try:
        url = f"{GEMINI_API_URL}?key={GEMINI_API_KEY}"
        response = requests.post(url, json=payload, timeout=20)
        response.raise_for_status()
        res_data = response.json()

        raw_text = res_data["candidates"][0]["content"]["parts"][0]["text"]
        parsed = json.loads(raw_text)

        roteiro_texto = parsed.get("roteiro_texto", "")
        metadados = parsed.get("metadados_ia", {})

        # Anexa o snapshot quantitativo oficial aos metadados JSONB para consulta auditavel
        metadados["indicadores_quant"] = {
            "preco_fechamento": metricas["preco_fechamento"],
            "variacao_dia_pct": metricas["variacao_dia_pct"],
            "sma_9": metricas["sma_9"],
            "sma_21": metricas["sma_21"],
            "rsi_14": metricas["rsi_14"],
            "volume_ratio": metricas["volume_ratio"],
            "tendencia": metricas["tendencia"]
        }

        # Mescla o scoring do Gemini com a estrutura das notícias coletadas
        noticias_avaliadas = metadados.pop("noticias_avaliadas", [])
        noticias_finais = []
        for i, n in enumerate(noticias or [], 1):
            avaliacao = next((a for a in noticias_avaliadas if a.get("indice") == i), {})
            score_val = float(avaliacao.get("score_sentimento") if avaliacao.get("score_sentimento") is not None else avaliacao.get("score", 0.0))
            label_val = avaliacao.get("label_sentimento") or avaliacao.get("sentimento") or "Neutral"
            noticias_finais.append({
                "titulo": n["titulo"],
                "fonte": n["fonte"],
                "url": n["url"],
                "data_publicacao": n["data_publicacao"],
                "score_sentimento": score_val,
                "label_sentimento": label_val,
                "score": score_val,
                "sentimento": label_val
            })

        metadados["noticias_mercado"] = noticias_finais
        if "score_sentimento_noticias" not in metadados:
            metadados["score_sentimento_noticias"] = 0.0
        if "classificacao_sentimento_noticias" not in metadados:
            metadados["classificacao_sentimento_noticias"] = "Neutral"

        logger.info("Analise gerada com sucesso via Gemini para %s [Sentimento: %s | Notícias: %d].", ticker, metadados.get("sentimento"), len(noticias_finais))
        return roteiro_texto, metadados

    except Exception as err:
        logger.warning("Falha na chamada da API do Gemini para %s: %s. Utilizando contingencia estruturada.", ticker, err)
        return _gerar_analise_fallback(ticker, meta_ativo, metricas, noticias)


def _gerar_analise_fallback(ticker: str, meta_ativo: dict, metricas: dict, noticias: list[dict] = None) -> tuple[str, dict]:
    """
    Gera relatorio analitico estruturado de contingencia deterministico caso
    haja falha de rede ou indisponibilidade da API externa.
    """
    sentimento = "bullish" if metricas["variacao_dia_pct"] > 0.5 else ("bearish" if metricas["variacao_dia_pct"] < -0.5 else "neutral")
    confianca = 0.85 if abs(metricas["variacao_dia_pct"]) > 1.5 else 0.70

    roteiro = (
        f"No pregão de {metricas['data_pregao']}, {ticker} ({meta_ativo.get('nome', ticker)}) "
        f"encerrou cotado a R$ {metricas['preco_fechamento']:.2f}, registrando variação de {metricas['variacao_dia_pct']:+.2f}%. "
        f"A amplitude intraday foi de {metricas['amplitude_intraday_pct']:.2f}%, com volume financeiro em {metricas['volume_ratio']:.2f}x "
        f"em relação à média histórica recente de 20 pregões.\n\n"
        f"Sob a ótica de análise técnica, o ativo operou com {metricas['tendencia'].lower()}. "
        f"O IFR de 14 períodos posicionou-se em {metricas['rsi_14']:.2f} ({metricas['condicao_rsi']}), "
        f"enquanto as médias móveis SMA 9 (R$ {metricas['sma_9']:.2f}) e SMA 21 (R$ {metricas['sma_21']:.2f}) "
        f"sinalizam patamares relevantes de suporte dinâmico no curto prazo."
    )

    if noticias:
        roteiro += f"\n\nNo radar de notícias, o papel foi impactado por matérias como: \"{noticias[0]['titulo']}\" ({noticias[0]['fonte']})."

    noticias_finais = [
        {
            "titulo": n["titulo"],
            "fonte": n["fonte"],
            "url": n["url"],
            "data_publicacao": n["data_publicacao"],
            "score_sentimento": 0.0,
            "label_sentimento": "Neutral",
            "score": 0.0,
            "sentimento": "Neutral"
        }
        for n in (noticias or [])
    ]

    metadados = {
        "sentimento": sentimento,
        "confianca_score": confianca,
        "risco_curto_prazo": "alto" if metricas["amplitude_intraday_pct"] > 3.0 else "medio",
        "drivers": [
            "volume_acima_media" if metricas["volume_ratio"] > 1.1 else "volume_estavel",
            "movimento_direcional",
            metricas["condicao_rsi"].split()[0].lower()
        ],
        "indicadores_quant": {
            "preco_fechamento": metricas["preco_fechamento"],
            "variacao_dia_pct": metricas["variacao_dia_pct"],
            "sma_9": metricas["sma_9"],
            "sma_21": metricas["sma_21"],
            "rsi_14": metricas["rsi_14"],
            "volume_ratio": metricas["volume_ratio"],
            "tendencia": metricas["tendencia"]
        },
        "score_sentimento_noticias": 0.0,
        "classificacao_sentimento_noticias": "Neutral",
        "noticias_mercado": noticias_finais
    }
    return roteiro, metadados


def gerar_analise_macro_mercado(data_pregao: date, analises_ativos: list[dict], noticias_macro: list[dict] = None) -> tuple[str, dict]:
    """
    Gera um parecer consolidado do panorama geral de mercado (ticker = NULL),
    sintetizando o comportamento conjunto das acoes monitoradas no dia e manchetes macro.
    """
    if not analises_ativos:
        return "", {}

    total = len(analises_ativos)
    altas = sum(1 for a in analises_ativos if a["metricas"]["variacao_dia_pct"] > 0)
    baixas = sum(1 for a in analises_ativos if a["metricas"]["variacao_dia_pct"] < 0)
    neutras = total - altas - baixas

    media_var = round(sum(a["metricas"]["variacao_dia_pct"] for a in analises_ativos) / total, 2)
    sentimento_macro = "bullish" if media_var > 0.3 else ("bearish" if media_var < -0.3 else "neutral")

    destaques = []
    for a in analises_ativos:
        destaques.append(f"{a['ticker']}: {a['metricas']['variacao_dia_pct']:+.2f}% (RSI {a['metricas']['rsi_14']:.1f})")
    destaques_str = ", ".join(destaques)

    bloco_noticias = ""
    if noticias_macro:
        itens_txt = [f"{i}. [{n['fonte']}] \"{n['titulo']}\"" for i, n in enumerate(noticias_macro, 1)]
        bloco_noticias = "\nNOTÍCIAS E MANCHETES MACROECONÔMICAS RECENTES:\n" + "\n".join(itens_txt) + "\n"

    if GEMINI_API_KEY:
        prompt = f"""
Você é um Estrategista-Chefe de Mercado Financeiro brasileiro.
Escreva o 'Panorama Geral de Fechamento de Mercado' para a data {data_pregao}, analisando a cesta de ativos monitorados:
- Total de ações analisadas: {total} (Altas: {altas}, Baixas: {baixas}, Neutras: {neutras})
- Retorno Médio da Cesta: {media_var:+.2f}%
- Destaques individuais: {destaques_str}
{bloco_noticias}
Gere uma resposta em JSON com:
1. "roteiro_texto": Parecer macroeconômico formal e executivo (2 parágrafos) sintetizando o tom geral do pregão, apetite a risco, comportamento setorial, repercussão das manchetes macro (se houver) e perspectivas para os próximos pregões.
2. "metadados_ia": Objeto JSON com:
   - "sentimento": "{sentimento_macro}"
   - "confianca_score": 0.90
   - "apetite_a_risco": "moderado"
   - "palavras_chave": ["fechamento_mercado", "apetite_risco", "bolsa_brasileira"]
   - "score_sentimento_noticias": float entre -1.0 e +1.0 representando o sentimento médio do noticiário macroeconômico
   - "classificacao_sentimento_noticias": "Bullish", "Bearish" ou "Neutral"
   - "noticias_avaliadas": lista de objetos com "indice", "score_sentimento", "label_sentimento"
"""
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"responseMimeType": "application/json", "temperature": 0.2}
        }
        try:
            url = f"{GEMINI_API_URL}?key={GEMINI_API_KEY}"
            resp = requests.post(url, json=payload, timeout=20)
            resp.raise_for_status()
            parsed = json.loads(resp.json()["candidates"][0]["content"]["parts"][0]["text"])
            roteiro = parsed.get("roteiro_texto", "")
            meta = parsed.get("metadados_ia", {})
            meta["resumo_estatistico"] = {
                "total_ativos": total,
                "altas": altas,
                "baixas": baixas,
                "retorno_medio_cesta_pct": media_var
            }

            noticias_avaliadas = meta.pop("noticias_avaliadas", [])
            noticias_finais = []
            for i, n in enumerate(noticias_macro or [], 1):
                avaliacao = next((a for a in noticias_avaliadas if a.get("indice") == i), {})
                score_val = float(avaliacao.get("score_sentimento") if avaliacao.get("score_sentimento") is not None else avaliacao.get("score", 0.0))
                label_val = avaliacao.get("label_sentimento") or avaliacao.get("sentimento") or "Neutral"
                noticias_finais.append({
                    "titulo": n["titulo"],
                    "fonte": n["fonte"],
                    "url": n["url"],
                    "data_publicacao": n["data_publicacao"],
                    "score_sentimento": score_val,
                    "label_sentimento": label_val,
                    "score": score_val,
                    "sentimento": label_val
                })
            meta["noticias_mercado"] = noticias_finais
            if "score_sentimento_noticias" not in meta:
                meta["score_sentimento_noticias"] = 0.0
            if "classificacao_sentimento_noticias" not in meta:
                meta["classificacao_sentimento_noticias"] = "Neutral"

            logger.info("Panorama Macro de Mercado gerado via Gemini com sucesso [Notícias Macro: %d].", len(noticias_finais))
            return roteiro, meta
        except Exception as err:
            logger.warning("Falha ao gerar panorama macro via Gemini: %s. Utilizando sintese interna.", err)

    # Contingencia Macro
    roteiro = (
        f"O pregão de {data_pregao} encerrou com desempenho médio de {media_var:+.2f}% na carteira de ativos acompanhados. "
        f"Do universo de {total} ações monitoradas, {altas} fecharam em território positivo, {baixas} em queda e {neutras} estáveis. "
        f"O saldo do dia reflete {('predomínio de compras' if media_var > 0 else 'predomínio de realização de lucros')}, "
        f"com os principais destaques negociados sendo: {destaques_str}.\n\n"
        f"Para as próximas sessões, o mercado mantém atenção aos patamares de suporte e resistência delineados pelas médias móveis, "
        f"com operadores monitorando a continuidade do fluxo e liquidez dos papéis líderes."
    )
    if noticias_macro:
        roteiro += f"\n\nNo panorama de notícias, o mercado repercutiu: \"{noticias_macro[0]['titulo']}\" ({noticias_macro[0]['fonte']})."

    noticias_finais = [
        {
            "titulo": n["titulo"],
            "fonte": n["fonte"],
            "url": n["url"],
            "data_publicacao": n["data_publicacao"],
            "score_sentimento": 0.0,
            "label_sentimento": "Neutral",
            "score": 0.0,
            "sentimento": "Neutral"
        }
        for n in (noticias_macro or [])
    ]

    meta = {
        "sentimento": sentimento_macro,
        "confianca_score": 0.85,
        "apetite_a_risco": "moderado",
        "palavras_chave": ["fechamento_mercado", "liquidez", "panorama_macro"],
        "resumo_estatistico": {
            "total_ativos": total,
            "altas": altas,
            "baixas": baixas,
            "retorno_medio_cesta_pct": media_var
        },
        "score_sentimento_noticias": 0.0,
        "classificacao_sentimento_noticias": "Neutral",
        "noticias_mercado": noticias_finais
    }
    return roteiro, meta


# ==============================================================================
# 3. CARGA NO POSTGRESQL (IDEMPOTENCIA & BULK UPSERT)
# ==============================================================================

def carregar_analises_bulk(conn, registros: list[tuple]) -> int:
    """
    Insere em lote na tabela Fato_Analise_Mercado garantindo idempotencia:
    ON CONFLICT (COALESCE(ticker, 'GERAL'), data_analise, fonte_origem) DO UPDATE.
    """
    if not registros:
        logger.info("Nenhuma analise para carregar.")
        return 0

    # Usamos o indice unico de idempotencia criado no banco
    query = """
        INSERT INTO Fato_Analise_Mercado (
            data_analise,
            ticker,
            fonte_origem,
            roteiro_texto,
            metadados_ia
        ) VALUES %s
        ON CONFLICT (COALESCE(ticker, 'GERAL'), data_analise, fonte_origem)
        DO UPDATE SET
            roteiro_texto = EXCLUDED.roteiro_texto,
            metadados_ia = EXCLUDED.metadados_ia;
    """

    with conn.cursor() as cur:
        # Prepara registros convertendo metadados_ia em Json adapter do psycopg2
        dados_formatados = [
            (r[0], r[1], r[2], r[3], Json(r[4]))
            for r in registros
        ]
        execute_values(cur, query, dados_formatados, page_size=100)
    conn.commit()

    logger.info("Bulk Upsert concluido: %d analise(s) persistida(s) em Fato_Analise_Mercado.", len(registros))
    return len(registros)


# ==============================================================================
# 4. ORQUESTRADOR PRINCIPAL DO PIPELINE DE ANALISE
# ==============================================================================

def executar_pipeline_analise():
    """
    Orquestrador completo:
    1. Busca ativos cadastrados em Dim_Ativo.
    2. Para cada ativo, calcula indicadores tecnicos e enriquece com Gemini.
    3. Gera analise macro consolidada (ticker = NULL).
    4. Persiste tudo em lote na Fato_Analise_Mercado com idempotencia.
    """
    logger.info("================ INICIANDO PIPELINE DE ANALISE DE MERCADO (QUANT + IA) ================")
    conn = None

    try:
        conn = obter_conexao()

        # 1. Identifica ativos monitorados
        with conn.cursor() as cur:
            cur.execute("SELECT ticker, nome_empresa, tipo_ativo, setor FROM Dim_Ativo ORDER BY ticker;")
            ativos = cur.fetchall()

        if not ativos:
            logger.warning("Nenhum ativo encontrado em Dim_Ativo. Execute o pipeline_cotacoes.py primeiro.")
            return

        logger.info("Ativos encontrados para analise: %s", [a[0] for a in ativos])

        # Verificacao de idempotencia temporal para evitar consumo redundante da API do Gemini
        force_reanalise = os.getenv("FORCE_REANALISE", "false").lower() == "true"
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(data_pregao) FROM Fato_Cotacao;")
            res_pregao = cur.fetchone()
            data_max_cotacao = res_pregao[0] if res_pregao else None

            if data_max_cotacao and not force_reanalise:
                cur.execute("SELECT COUNT(*) FROM Fato_Analise_Mercado WHERE data_analise = %s;", (data_max_cotacao,))
                total_ja_analisados = cur.fetchone()[0]
                if total_ja_analisados >= len(ativos) + 1:
                    logger.info("As analises para o pregao de %s ja estao completas no banco (%d registros encontrados).", data_max_cotacao, total_ja_analisados)
                    logger.info("Pulando execucao do pipeline de IA para economizar cota da API (para forcar reprocessamento, defina FORCE_REANALISE=true).")
                    return

        registros_para_banco = []
        analises_coletadas = []
        data_referencia = None

        # 2. Processamento individual por ativo (Abordagem C + Abordagem B + Notícias B3)
        for ticker, nome, tipo, setor in ativos:
            meta_ativo = {"nome": nome, "tipo": tipo, "setor": setor}
            historico = buscar_historico_cotacoes(conn, ticker, limite=30)

            if len(historico) < 2:
                logger.warning("Historico insuficiente de cotacoes para %s (%d registros). Pulando.", ticker, len(historico))
                continue

            # Abordagem C: Indicadores Tecnicos
            metricas = calcular_indicadores_tecnicos(historico)
            data_referencia = metricas["data_pregao"]

            # Extração de notícias B3 em tempo real para o ativo
            termo_noticia = f"{ticker} {nome}"
            noticias_ativo = buscar_noticias_b3(termo_noticia, limite=3)

            # Abordagem B: IA Generativa Gemini
            roteiro, metadados = gerar_analise_ia_gemini(ticker, meta_ativo, metricas, noticias=noticias_ativo)

            registros_para_banco.append((
                metricas["data_pregao"],
                ticker,
                FONTE_ORIGEM_PADRAO,
                roteiro,
                metadados
            ))

            analises_coletadas.append({
                "ticker": ticker,
                "meta": meta_ativo,
                "metricas": metricas,
                "metadados": metadados
            })

        # 3. Analise Consolidada do Panorama Geral de Mercado (ticker = NULL)
        if analises_coletadas and data_referencia:
            logger.info("Gerando Panorama Macro de Mercado para a data %s...", data_referencia)
            noticias_macro = buscar_noticias_b3("fechamento Ibovespa mercado financeiro acoes B3", limite=4)
            roteiro_macro, meta_macro = gerar_analise_macro_mercado(data_referencia, analises_coletadas, noticias_macro=noticias_macro)
            registros_para_banco.append((
                data_referencia,
                None,  # ticker = NULL conforme modelagem dimensional para o mercado geral
                "Gemini Macro Intel",
                roteiro_macro,
                meta_macro
            ))

        # 4. Ingestao em Lote no PostgreSQL
        if registros_para_banco:
            total = carregar_analises_bulk(conn, registros_para_banco)
            logger.info("================ PIPELINE DE ANALISE FINALIZADO COM SUCESSO ================")
            logger.info("Total de analises salvas no banco: %d", total)
        else:
            logger.warning("Nenhuma analise foi gerada para persistencia.")

    except Exception as err:
        logger.exception("Falha critica na execucao da analise de mercado: %s", err)
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            logger.info("Conexao com o PostgreSQL encerrada com seguranca.")


if __name__ == "__main__":
    executar_pipeline_analise()
