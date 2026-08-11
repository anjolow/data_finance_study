CREATE TABLE Fato_Analise_Mercado (
    id BIGSERIAL PRIMARY KEY,
    data_analise DATE NOT NULL,
    ticker VARCHAR(10) REFERENCES Dim_Ativo(ticker), -- Pode ser NULO se a análise for do mercado geral
    fonte_origem VARCHAR(100) NOT NULL, -- Ex: 'Newsletter X', 'Portal Y'
    roteiro_texto TEXT, -- O roteiro finalizado ou extração bruta
    metadados_ia JSONB -- Excelente para salvar o response bruto da API do Gemini (ex: {"sentimento": "bullish", "palavras_chave": ["juros", "selic"]})
);

-- Índice GIN, essencial para acelerar buscas dentro de propriedades JSONB no PostgreSQL
CREATE INDEX idx_fato_analise_json ON Fato_Analise_Mercado USING GIN (metadados_ia);