import csv
import sqlite3
from pathlib import Path
import urllib.request
import urllib.error
import zipfile
import json
from config import TABELAS_PRINCIPAIS, TABELAS_AUXILIARES
import shutil
from datetime import datetime, timezone, timedelta


def baixar_e_descompactar_stream(url, tmp_dir):
    try:
        with urllib.request.urlopen(url) as r:
            with open(tmp_dir / "tmp.zip", "wb") as f:
                while True:
                    chunk = r.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise

    with zipfile.ZipFile(tmp_dir / "tmp.zip") as z:
        nomes_arquivos = z.namelist()
        z.extractall(tmp_dir)

    (tmp_dir / 'tmp.zip').unlink()
    return nomes_arquivos


def executar_sql_arquivo(conn, sql_path):
    with open(sql_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())


def carregar_csv_banco(nome_tabela, csv_path, conn, chunk_size=10000):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=';')
        cols = list(reader)
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
        with urllib.request.urlopen(url) as r:
            with open(tmp_dir / f'{tabela_nome}.csv', "wb") as f:
                while True:
                    chunk = r.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)

        carregar_csv_banco(tabela_nome, tmp_dir / f'{tabela_nome}.csv', conn)
        (tmp_dir / f'{tabela_nome}.csv').unlink()


def carregar_archives_banco(conn, dia_inicial, tmp_dir):
    dia_atual = datetime.now(tz=timezone(timedelta(hours=-3)))
    acumuladores = {}
    def append_arquivo(nome_tabela, origem_path):
        destino_path = tmp_dir / f"{nome_tabela}_acc.csv"
        modo = "a" if destino_path.exists() else "w"
        with open(origem_path, "r", encoding="utf-8") as src, \
             open(destino_path, modo, encoding="utf-8") as dst:
            shutil.copyfileobj(src, dst)
        acumuladores[nome_tabela] = destino_path
    for i in range((dia_atual - dia_inicial).days):
        dia_str = (dia_inicial + timedelta(days=i)).strftime("%d-%m-%Y")
        url = 'https://archive.cnpj.pw/dias_passados/' + dia_str + '.zip'
        arquivos_nomes = baixar_e_descompactar_stream(url, tmp_dir)
        for nome_arquivo in arquivos_nomes:
            nome_tabela = nome_arquivo.split('.')[0]
            origem = tmp_dir / nome_arquivo
            append_arquivo(nome_tabela, origem)
            origem.unlink()
    dia_atual_str = dia_atual.strftime("%d-%m-%Y")
    url = 'https://archive.cnpj.pw/dias_passados/' + dia_atual_str + '/'
    for tabela in TABELAS_PRINCIPAIS:
        file_url = url + f'{tabela}.csv'
        try:
            with urllib.request.urlopen(file_url) as r:
                tmp_file = tmp_dir / f"{tabela}.csv"
                with open(tmp_file, "wb") as f:
                    while True:
                        chunk = r.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)
                append_arquivo(tabela, tmp_file)
                tmp_file.unlink()
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
    for nome_tabela, path in acumuladores.items():
        carregar_csv_banco(nome_tabela, path, conn)
        path.unlink()

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
