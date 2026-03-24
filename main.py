import csv
import sqlite3
from pathlib import Path
import requests
import zipfile
import json


def baixar_e_descompactar_stream(url, tmp_dir):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(tmp_dir / "tmp.zip", "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)

    with zipfile.ZipFile(tmp_dir / "tmp.zip") as z:
        z.extractall(tmp_dir)
    (tmp_dir / 'tmp.zip').unlink()


def executar_sql_arquivo(conn, sql_path):
    with open(sql_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())


def carregar_csv_banco(nome_tabela, csv_path, conn, chunk_size=10000):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=';')
        cols = list(reader)  # remova se NÃO tiver header
        placeholders = ",".join(["?"] * len(cols[0]))
        query = f"INSERT INTO {nome_tabela} VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        batch = []
        cur = conn.cursor()
        for row in cols:
            batch.append(row)
            if len(batch) >= chunk_size:
                cur.executemany(query, batch)
                batch.clear()
        if batch:
            cur.executemany(query, batch)
        conn.commit()


def pegar_empresas_json(conn, dia):
    query = """
    SELECT json_object(
        -- empresas
        'cnpj_base', e.cnpj_base,
        'nome_empresarial', e.nome_empresarial,
        'natureza_juridica', e.natureza_juridica,
        'qualificacao_responsavel', e.qualificacao_responsavel,
        'capital_social', e.capital_social,
        'porte_empresa', e.porte_empresa,
        'ente_federativo', e.ente_federativo,

        -- estabelecimentos
        'cnpj_ordem', est.cnpj_ordem,
        'cnpj_dv', est.cnpj_dv,
        'identificador', est.identificador,
        'nome_fantasia', est.nome_fantasia,
        'situacao_cadastral', est.situacao_cadastral,
        'data_situacao_cadastral', est.data_situacao_cadastral,
        'motivo_situacao_cadastral', est.motivo_situacao_cadastral,
        'nome_cidade_exterior', est.nome_cidade_exterior,
        'pais', est.pais,
        'data_inicio_atividade', est.data_inicio_atividade,
        'cnae_fiscal_principal', est.cnae_fiscal_principal,
        'cnaes_fiscais_secundarios_raw', est.cnaes_fiscais_secundarios,
        'tipo_logradouro', est.tipo_logradouro,
        'logradouro', est.logradouro,
        'numero', est.numero,
        'complemento', est.complemento,
        'bairro', est.bairro,
        'cep', est.cep,
        'uf', est.uf,
        'municipio', est.municipio,
        'ddd1', est.ddd1,
        'telefone_1', est.telefone_1,
        'ddd2', est.ddd2,
        'telefone_2', est.telefone_2,
        'ddd_fax', est.ddd_fax,
        'fax', est.fax,
        'correio_eletronico', est.correio_eletronico,
        'situacao_especial', est.situacao_especial,
        'data_situacao_especial', est.data_situacao_especial,

        -- descrições
        'natureza_juridica_desc', nj.descricao,
        'motivo_situacao_desc', ms.descricao,
        'municipio_desc', m.descricao,
        'pais_desc', p.descricao,
        'cnae_fiscal_principal_descricao', c.descricao,
        'identificador_descricao', imf.descricao,
        'porte_empresa_descricao', pe.descricao,
        'situacao_cadastral_descricao', sc.descricao,

        -- dados simples
        'opcao_simples', ds.opcao_simples,
        'data_opcao_simples', ds.data_opcao_simples,
        'data_exclusao_simples', ds.data_exclusao_simples,
        'opcao_mei', ds.opcao_mei,
        'data_opcao_mei', ds.data_opcao_mei,
        'data_exclusao_mei', ds.data_exclusao_mei,

        -- socios
        'socios', (
            SELECT json_group_array(
                json_object(
                    'nome', s.nome,
                    'cnpj_cpf', s.cnpj_cpf,
                    'qualificacao_descricao', qs.descricao
                )
            )
            FROM socios s
            LEFT JOIN qualificacoes_socios qs ON s.qualificacao = qs.codigo
            WHERE s.cnpj_base = est.cnpj_base
        )
    )
    FROM estabelecimentos est
    JOIN empresas e ON est.cnpj_base = e.cnpj_base
    LEFT JOIN dados_simples ds ON est.cnpj_base = ds.cnpj_base
    LEFT JOIN naturezas_juridicas nj ON e.natureza_juridica = nj.codigo
    LEFT JOIN cnaes c ON est.cnae_fiscal_principal = c.codigo
    LEFT JOIN motivos_situacoes ms ON est.motivo_situacao_cadastral = ms.codigo
    LEFT JOIN municipios m ON est.municipio = m.codigo
    LEFT JOIN paises p ON est.pais = p.codigo
    LEFT JOIN identificador_matriz_filial imf ON est.identificador = imf.codigo
    LEFT JOIN portes_empresas pe ON pe.codigo = e.porte_empresa
    LEFT JOIN situacoes_cadastrais sc ON sc.codigo = est.situacao_cadastral
    WHERE est.data_inicio_atividade = ?
    """

    cur = conn.cursor()
    cur.execute(query, (dia, ))
    results = [json.loads(i[0]) for i in cur.fetchall()]
    results_json = json.dumps(
      results, sort_keys=True, indent=2, ensure_ascii=False
    )

    return str(results_json)

def carregar_auxiliares_banco(conn, tmp_dir):
    auxiliares_para_tabelas = {
        "paises.csv": "paises",
        "municipios.csv": "municipios",
        "naturezas_juridicas.csv": "naturezas_juridicas",
        "qualificacoes_socios.csv": "qualificacoes_socios",
        "cnaes.csv": "cnaes",
        "motivos_situacoes.csv": "motivos_situacoes",

        "identificador_matriz_filial.csv": "identificador_matriz_filial",
        "situacoes_cadastrais.csv": "situacoes_cadastrais",
        "portes_empresas.csv": "portes_empresas",
        "faixas_etarias.csv": "faixas_etarias",
        "qualificacoes_representantes.csv": "qualificacoes_representantes",
        "identificadores_socios.csv": "identificadores_socios",
    }
    for arq, tabela_nome in auxiliares_para_tabelas.items():
        url = 'https://archive.cnpj.pw/tabelas_auxiliares/' + arq
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(tmp_dir / 'tmp.csv', "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
        carregar_csv_banco(tabela_nome, tmp_dir / 'tmp.csv', conn)
        (tmp_dir / 'tmp.csv').unlink()


def carregar_principais_banco(conn, dias, tmp_dir):
    principais_para_tabelas = {
        "Empresas.csv": "empresas",
        "Socios.csv": "socios",
        "Estabelecimentos.csv": "estabelecimentos",
        "Simples.csv": "dados_simples",
    }
    for dia in dias:
        url = 'https://archive.cnpj.pw/dias_passados/' + dia + '.zip'
        baixar_e_descompactar_stream(url, tmp_dir)
        for arq, tabela_nome in principais_para_tabelas.items():
            carregar_csv_banco(tabela_nome, (tmp_dir / arq), conn) 


PATH_SCRIPT = Path(__file__).parent
Path(PATH_SCRIPT / 'cnpjpw.db').unlink(missing_ok=True)
Path(PATH_SCRIPT / 'cnpjpw.db').touch()
conn = sqlite3.connect(PATH_SCRIPT / 'cnpjpw.db')
executar_sql_arquivo(conn, PATH_SCRIPT / 'criar_tabelas.sql')

(PATH_SCRIPT / 'tmp').mkdir(exist_ok=True)
tmp_dir = PATH_SCRIPT / 'tmp'
carregar_auxiliares_banco(conn, tmp_dir)
carregar_principais_banco(conn, ['19-03-2026'], tmp_dir)
executar_sql_arquivo(conn, PATH_SCRIPT / 'criar_indices.sql')
with open(PATH_SCRIPT / '2026-03-19.json', 'w') as f:
    f.write(pegar_empresas_json(conn, '2026-03-19'))
conn.close()
