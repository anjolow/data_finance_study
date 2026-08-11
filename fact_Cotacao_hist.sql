CREATE TABLE Fato_Cotacao (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) REFERENCES Dim_Ativo(ticker),
    data_pregao DATE NOT NULL,
    preco_abertura NUMERIC(15,4) NOT NULL,
    preco_maximo NUMERIC(15,4) NOT NULL,
    preco_minimo NUMERIC(15,4) NOT NULL,
    preco_fechamento NUMERIC(15,4) NOT NULL,
    volume_negociado BIGINT,
    UNIQUE(ticker, data_pregao) -- Evita duplicidade do mesmo ativo no mesmo dia
);

-- Índices cruciais para performance de leitura no BI e análises de janela de tempo
CREATE INDEX idx_fato_cotacao_data ON Fato_Cotacao(data_pregao DESC);
CREATE INDEX idx_fato_cotacao_ticker ON Fato_Cotacao(ticker);