from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from pathlib import Path
from typing import Iterator
import requests
import re
import click
import textwrap
import time


SYMBOLS_DIR = Path('symbols')
BASE_SYMBOLS_URL = "https://symbols.mozilla.org/"


client = bigquery.Client()

init_sql = Path('init.sql')
get_symbol_files_sql = Path('get_symbol_files.sql')
init_symbol_files_sql = Path('init_symbol_files.sql')


def query(q: str) -> Iterator[bigquery.Row]:
    print("\nRunning Query")
    print(textwrap.indent(q, '\t'))
    res = client.query(q)
    return res.result()

def download_file(file_path: str) -> str:
    local_filename = SYMBOLS_DIR / file_path

    with requests.get(BASE_SYMBOLS_URL + file_path, stream=True) as r:
        # Ignore missing symbol files - why?
        if r.status_code == 404:
            return None
        r.raise_for_status()

        # We must have a file, create dir and download
        local_filename.parent.mkdir(parents=True, exist_ok=True)
        print(f"Downloading {file_path}")
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return local_filename

def insert_file_rows(file_path: str) -> int:
    file = SYMBOLS_DIR / file_path
    if not file.exists():
        return 0

    print(f"Inserting file rows for {file_path}")
    _, debug_file, debug_id, _ = file.parts
    lines = []
    for line in file.read_text().split('\n'):
        if 'FUNC' in line or 'PUBLIC' in line:
            try:
                _, addr, _, _, func = line.split(' ', maxsplit=4)
                lines.append((debug_file, debug_id, int(addr, 16), func))
            except ValueError:
                pass

    table = "frank-sandbox.telemetry_stable.symbol_files"
    while True:
        try:
            res = client.insert_rows(table, lines, client.get_table(table).schema)
            break
        except NotFound:
            time.sleep(5)

    print(res)
    return len(lines)


@click.command()
@click.option('--refresh', default=False, is_flag=True)
@click.option('--download', default=False, is_flag=True)
@click.option('--insert', default=False, is_flag=True)
def main(refresh: bool, download: bool, insert: bool):
    if refresh:
        query(init_sql.read_text())
    rows = query(get_symbol_files_sql.read_text())
    files = [row.get("filepath") for row in rows]

    if download:
        for file_path in files:
            res = download_file(file_path)

    if insert:
        query(init_symbol_files_sql.read_text())
        for file_path in files:
            insert_file_rows(file_path)

if __name__ == '__main__':
    main()
