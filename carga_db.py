import csv
import sqlite3
from pathlib import Path
import requests
import zipfile
import json
from config import TABELAS_PRINCIPAIS, TABELAS_AUXILIARES
import shutil
from datetime import datetime, timezone, timedelta


def baixar_e_descompactar_stream(url, tmp_dir):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(tmp_dir / "tmp.zip", "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)

    with zipfile.ZipFile(tmp_dir / "tmp.zip") as z:
        nomes_arquivos = z.namelist()
        z.extractall(tmp_dir)
    (tmp_dir / 'tmp.zip').unlink()
    return nomes_arquivos


def executar_sql_arquivo(conn, sql_path):
    with open(sql_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())

def carregar_archive_corrente_banco(url, tmp_dir, conn):
    for tabela in TABELAS_PRINCIPAIS:
        with requests.get(url + f'{tabela}.csv', stream=True) as r:
            if r.status_code == 404:
                continue
            with open(tmp_dir / f"{tabela}.csv", "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
        carregar_csv_banco(tabela, tmp_dir / f'{tabela}.csv', conn)


def carregar_csv_banco(nome_tabela, csv_path, conn, chunk_size=10000):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=';')
        cols = list(reader)  # remova se NÃO tiver header
        if not cols:
            return
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


def carregar_auxiliares_banco(conn, tmp_dir):
    for tabela_nome in TABELAS_AUXILIARES:
        url = f'https://archive.cnpj.pw/tabelas_auxiliares/{tabela_nome}.csv'
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(tmp_dir / f'{tabela_nome}.csv', "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
        carregar_csv_banco(tabela_nome, tmp_dir / f'{tabela_nome}.csv', conn)
        (tmp_dir / f'{tabela_nome}.csv').unlink()


def carregar_archives_banco(conn, dia_inicial, tmp_dir):
    dia_atual = datetime.now(tz=timezone(timedelta(hours=-3)))
    for i in range((dia_atual- dia_inicial).days):
        dia_str = (dia_inicial + timedelta(days=i)).strftime("%d-%m-%Y")
        url = 'https://archive.cnpj.pw/dias_passados/' + dia_str + '.zip'
        arquivos_nomes = baixar_e_descompactar_stream(url, tmp_dir)
        tabelas_destino = [nome.split('.')[0] for nome in arquivos_nomes]
        for nome_tabela in tabelas_destino:
            carregar_csv_banco(nome_tabela, (tmp_dir / f'{nome_tabela}.csv'), conn)

    dia_atual_str = dia_atual.strftime("%d-%m-%Y")
    url = 'https://archive.cnpj.pw/dias_passados/' + dia_atual_str + '/'
    carregar_archive_corrente_banco(url, tmp_dir, conn)


def executar_carga(data_inicial):
    PATH_SCRIPT = Path(__file__).parent

    db_path = PATH_SCRIPT / 'cnpjpw.db'
    db_path.touch(exist_ok=True)
    conn = sqlite3.connect(db_path)

    executar_sql_arquivo(conn, PATH_SCRIPT / 'criar_tabelas.sql')

    tmp_dir = PATH_SCRIPT / 'tmp'
    tmp_dir.mkdir(exist_ok=True)
    carregar_auxiliares_banco(conn, tmp_dir)
    carregar_archives_banco(conn, data_inicial, tmp_dir)
    executar_sql_arquivo(conn, PATH_SCRIPT / 'criar_indices.sql')

    conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2 or sys.argv[1] == '--help':
        print(
"""
Uso: python carga_db.py DD-MM-YYYY
baixa todos os dumps do archive do cnpjpw a partir da data informada e carrega no sqlite cnpjpw.db
"""
        )
        sys.exit(1)

    data = datetime.strptime(sys.argv[1], '%d-%m-%Y')
    data_inicial = datetime(
        data.year, data.month, data.day,
        tzinfo=timezone(timedelta(hours=-3))
    )
    executar_carga(data_inicial)
