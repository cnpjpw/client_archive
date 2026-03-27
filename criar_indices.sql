CREATE INDEX IF NOT EXISTS estabelecimentos_pais_idx
ON estabelecimentos(pais);

CREATE INDEX IF NOT EXISTS motivo_situacao_cadastral_idx
ON estabelecimentos(motivo_situacao_cadastral);

CREATE INDEX IF NOT EXISTS idx_socios_cnpj_base
ON socios (cnpj_base);

CREATE INDEX IF NOT EXISTS razao_social_idx
ON empresas (nome_empresarial);

CREATE INDEX IF NOT EXISTS capital_social_idx
ON empresas (capital_social);

CREATE INDEX IF NOT EXISTS natureza_juridica_idx
ON empresas (natureza_juridica);

CREATE INDEX IF NOT EXISTS capital_cnpj_empresas_idx
ON empresas (capital_social, cnpj_base);

CREATE INDEX IF NOT EXISTS estabelecimentos_municipio_idx
ON estabelecimentos (municipio);

CREATE INDEX IF NOT EXISTS data_abertura_idx
ON estabelecimentos (data_inicio_atividade);

CREATE INDEX IF NOT EXISTS cnpj_e_data_inscricao_idx
ON estabelecimentos (data_inicio_atividade, cnpj_base, cnpj_ordem, cnpj_dv);

CREATE INDEX IF NOT EXISTS cnae_fiscal_estabelecimentos_idx
ON estabelecimentos (cnae_fiscal_principal);

CREATE INDEX IF NOT EXISTS cnpj_e_cnae_idx
ON estabelecimentos (cnae_fiscal_principal, data_inicio_atividade, cnpj_base, cnpj_ordem, cnpj_dv);

CREATE INDEX IF NOT EXISTS data_cnae_estabelecimentos_idx
ON estabelecimentos (data_inicio_atividade, cnae_fiscal_principal, cnpj_base, cnpj_ordem, cnpj_dv);

-- INCLUDE não existe no SQLite → ignorado
CREATE INDEX IF NOT EXISTS idx_est_cnae_sit_data
ON estabelecimentos (cnae_fiscal_principal, situacao_cadastral, data_inicio_atividade);

CREATE INDEX IF NOT EXISTS idx_est_full_query
ON estabelecimentos (cnae_fiscal_principal, situacao_cadastral, data_inicio_atividade, cnpj_base, cnpj_ordem, cnpj_dv);

CREATE INDEX IF NOT EXISTS estabelecimentos_uf_idx
ON estabelecimentos (uf);

CREATE INDEX IF NOT EXISTS uf_data_abertura_cnpj_estabelecimentos_idx
ON estabelecimentos (uf, data_inicio_atividade, cnpj_base, cnpj_ordem, cnpj_dv);

CREATE INDEX IF NOT EXISTS razao_social_segmentos_texto_idx
ON empresas (nome_empresarial COLLATE NOCASE, cnpj_base);

CREATE INDEX IF NOT EXISTS nome_fantasia_e_cnpj_idx
ON estabelecimentos (nome_fantasia COLLATE NOCASE, cnpj_base, cnpj_ordem, cnpj_dv);

