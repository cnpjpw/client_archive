import sqlite3
from pathlib import Path
import json
from datetime import datetime, timezone, timedelta



def gerar_json(dia_iso):
    PATH_SCRIPT = Path(__file__).parent

    db_path = PATH_SCRIPT / 'cnpjpw.db'
    db_path.touch(exist_ok=True)
    conn = sqlite3.connect(db_path)



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
    cur.execute(query, (dia_iso, ))
    results = [json.loads(i[0]) for i in cur.fetchall()]
    results_json = json.dumps(
      results, sort_keys=True, indent=2, ensure_ascii=False
    )

    data = datetime.strptime(data_iso, '%Y-%m-%d')
    data_str = datetime.strftime(data, '%d-%m-%Y')
    path_json = (PATH_SCRIPT / f'{data_str}.json')
    with open(path_json, 'w') as f:
        f.write(str(results_json))
        f.write('\n')

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2 or sys.argv[1] == '--help':
        print(
    """
    Uso: python gerar_json.py DD-MM-YYYY
    gera json análogo ao formato do endpoint /cnpj/{cnpj} de todos os cnpjs da data passada e salva no arquivo DD-MM-YYYY.json
    """
        )
        sys.exit(1)

    data = datetime.strptime(sys.argv[1], '%d-%m-%Y')
    data_iso = datetime.strftime(data, '%Y-%m-%d')
    data_inicial = datetime(
        data.year, data.month, data.day,
        tzinfo=timezone(timedelta(hours=-3))
    )
    gerar_json(data_iso)
 
