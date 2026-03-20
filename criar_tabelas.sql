CREATE TABLE empresas (
    cnpj_base TEXT PRIMARY KEY,
    nome_empresarial TEXT,
    natureza_juridica INTEGER,
    qualificacao_responsavel INTEGER,
    capital_social REAL,
    porte_empresa INTEGER,
    ente_federativo TEXT,
    FOREIGN KEY (natureza_juridica) REFERENCES naturezas_juridicas (codigo),
    FOREIGN KEY (porte_empresa) REFERENCES portes_empresas (codigo)
);

CREATE TABLE estabelecimentos (
    cnpj_base TEXT,
    cnpj_ordem TEXT,
    cnpj_dv TEXT,
    identificador INTEGER,
    nome_fantasia TEXT,
    situacao_cadastral INTEGER,
    data_situacao_cadastral TEXT,
    motivo_situacao_cadastral INTEGER,
    nome_cidade_exterior TEXT,
    pais INTEGER,
    data_inicio_atividade TEXT,
    cnae_fiscal_principal INTEGER,
    cnaes_fiscais_secundarios TEXT, -- array vira TEXT (ex: JSON ou CSV)
    tipo_logradouro TEXT,
    logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cep TEXT,
    uf TEXT,
    municipio INTEGER,
    ddd1 TEXT,
    telefone_1 TEXT,
    ddd2 TEXT,
    telefone_2 TEXT,
    ddd_fax TEXT,
    fax TEXT,
    correio_eletronico TEXT,
    situacao_especial TEXT,
    data_situacao_especial TEXT,
    PRIMARY KEY (cnpj_base, cnpj_ordem, cnpj_dv),
    FOREIGN KEY (cnpj_base) REFERENCES empresas (cnpj_base),
    FOREIGN KEY (identificador) REFERENCES identificador_matriz_filial (codigo),
    FOREIGN KEY (situacao_cadastral) REFERENCES situacoes_cadastrais (codigo),
    FOREIGN KEY (cnae_fiscal_principal) REFERENCES cnaes (codigo),
    FOREIGN KEY (municipio) REFERENCES municipios (codigo)
);

CREATE TABLE dados_simples (
    cnpj_base TEXT PRIMARY KEY,
    opcao_simples INTEGER, -- BOOLEAN -> INTEGER (0/1)
    data_opcao_simples TEXT,
    data_exclusao_simples TEXT,
    opcao_mei INTEGER,
    data_opcao_mei TEXT,
    data_exclusao_mei TEXT,
    FOREIGN KEY (cnpj_base) REFERENCES empresas (cnpj_base)
);

CREATE TABLE socios (
    cnpj_base TEXT,
    identificador INTEGER,
    nome TEXT,
    cnpj_cpf TEXT,
    qualificacao INTEGER,
    data_entrada_sociedade TEXT,
    pais INTEGER,
    cpf_representante TEXT,
    nome_representante TEXT,
    qualificacao_representante INTEGER,
    faixa_etaria INTEGER,
    UNIQUE (cnpj_base, nome, cnpj_cpf),
    FOREIGN KEY (cnpj_base) REFERENCES empresas (cnpj_base),
    FOREIGN KEY (identificador) REFERENCES identificadores_socios (codigo),
    FOREIGN KEY (qualificacao) REFERENCES qualificacoes_socios (codigo),
    FOREIGN KEY (qualificacao_representante) REFERENCES qualificacoes_representantes (codigo),
    FOREIGN KEY (faixa_etaria) REFERENCES faixas_etarias (codigo)
);

CREATE TABLE paises (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE municipios (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE qualificacoes_socios (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE naturezas_juridicas (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE cnaes (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE motivos_situacoes (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE faixas_etarias (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);

CREATE table identificador_matriz_filial (
	codigo SMALLINT PRIMARY KEY,
	descricao TEXT
);


CREATE TABLE situacoes_cadastrais (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE portes_empresas (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE identificadores_socios (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE qualificacoes_representantes (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);

