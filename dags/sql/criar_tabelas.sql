/* brModelo: */

CREATE TABLE IF NOT EXISTS Advogado (
    id_advogado SERIAL PRIMARY KEY,
    nome VARCHAR
);

CREATE TABLE IF NOT EXISTS Fornecedor (
    id_fornecedor SERIAL PRIMARY KEY,
    nome VARCHAR
);

CREATE TABLE IF NOT EXISTS Unidade_Gestora (
    codigo_unidade_gestora INTEGER PRIMARY KEY UNIQUE,
    denominacao VARCHAR
);

CREATE TABLE IF NOT EXISTS Licitacao (
    id_licitacao SERIAL PRIMARY KEY,
    numero_licitacao VARCHAR UNIQUE,
    modalidade VARCHAR,
    valor_estimado DECIMAL,
    objeto_resumido VARCHAR,
    data_emissao DATE,
    data_abertura DATE,
    finalidade VARCHAR,
    forma_julgamento VARCHAR,
    id_advogado INTEGER,
    FOREIGN KEY (id_advogado) REFERENCES Advogado (id_advogado)
);

CREATE TABLE IF NOT EXISTS Contrato (
    id_contrato SERIAL PRIMARY KEY,
    numero_contrato VARCHAR,
    assinatura INTEGER,
    inicio_vigencia DATE,
    vencimento DATE,
    valor_total DECIMAL,
    objeto_resumido VARCHAR,
    codigo_unidade_gestora INTEGER,
    id_fornecedor INTEGER,
    id_licitacao INTEGER,
    FOREIGN KEY (codigo_unidade_gestora) REFERENCES Unidade_Gestora (codigo_unidade_gestora),
    FOREIGN KEY (id_fornecedor) REFERENCES Fornecedor (id_fornecedor),
    FOREIGN KEY (id_licitacao) REFERENCES Licitacao (id_licitacao)
);

CREATE TABLE IF NOT EXISTS Empenho (
    id_empenho SERIAL PRIMARY KEY,
    emissao DATE,
    numero_empenho INTEGER,
    objeto_resumido VARCHAR,
    especie VARCHAR,
    categoria VARCHAR,
    id_contrato INTEGER,
    id_licitacao INTEGER,
    recurso_diaria VARCHAR,
    FOREIGN KEY (id_contrato) REFERENCES Contrato (id_contrato),
    FOREIGN KEY (id_licitacao) REFERENCES Licitacao (id_licitacao)
);

CREATE TABLE IF NOT EXISTS Item (
    id_item SERIAL PRIMARY KEY,
    numero_item VARCHAR,
    denominacao VARCHAR,
    quantidade DECIMAL,
    unidade_medida VARCHAR,
    valor_unitario_estimado DECIMAL,
    situacao VARCHAR,
    id_licitacao INTEGER,
    id_contrato INTEGER,
    valor_total DECIMAL,
    FOREIGN KEY (id_licitacao) REFERENCES Licitacao (id_licitacao),
    FOREIGN KEY (id_contrato) REFERENCES Contrato (id_contrato)
);

CREATE TABLE IF NOT EXISTS Vencedor (
    id_licitacao INTEGER,
    id_fornecedor INTEGER,
    quantidade DECIMAL,
    valor_unitario DECIMAL,
    situacao VARCHAR,
    PRIMARY KEY (id_licitacao, id_fornecedor),
    FOREIGN KEY (id_licitacao) REFERENCES Licitacao (id_licitacao),
    FOREIGN KEY (id_fornecedor) REFERENCES Fornecedor (id_fornecedor)
);

CREATE TABLE IF NOT EXISTS Texto (
    id_texto SERIAL PRIMARY KEY,
    id_licitacao INTEGER,
    denominacao VARCHAR,
    link VARCHAR,
    FOREIGN KEY (id_licitacao) REFERENCES Licitacao (id_licitacao)
);

CREATE TABLE IF NOT EXISTS Licitacao_Unidade_Gestora (
    id_licitacao INTEGER,
    codigo_unidade_gestora INTEGER,
    FOREIGN KEY (id_licitacao) REFERENCES Licitacao (id_licitacao),
    FOREIGN KEY (codigo_unidade_gestora) REFERENCES Unidade_Gestora (codigo_unidade_gestora)
);