CREATE TABLE Dim_Ativo (
    ticker VARCHAR(10) PRIMARY KEY, -- Ex: PETR4, VALE3, BTC
    nome_empresa VARCHAR(100) NOT NULL,
    tipo_ativo VARCHAR(20) NOT NULL, -- Acao, Cripto, FII
    setor VARCHAR(50),
    data_criacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Índice para acelerar filtros por setor
CREATE INDEX idx_dim_ativo_setor ON Dim_Ativo(setor);