"""
Extrai uma tabela do Trino pro DuckDB local e registra a base+colunas no
Postgres (dim_base/dim_atributo), SEM perfilar. Serve pra:
  - a base aparecer no seletor do Streamlit (app_regras.py) antes do primeiro
    profiling completo: dim_atributo é o que alimenta a lista de colunas lá
  - já poder usar "carregar valores distintos" / prévia de regras sem esperar
    a pipeline inteira (persistir_datamart_duckdb.py) rodar

Seguro rodar antes OU depois da pipeline completa: o registro é get-or-create
(loader.registrar_base_leve), então não duplica nem conflita quando a
pipeline de verdade rodar depois.

Lê a fonte do mesmo esquema FONTE + fontes.ini que persistir_datamart_duckdb.py
usa (só aceita fontes com tipo=trino aqui). Mantido como script separado em
vez de importar persistir_datamart_duckdb.py porque esse módulo roda a
resolução de configuração inteira (FONTE obrigatória, fontes.ini etc.) no
nível de import.

Uso: FONTE=<nome> .venv/bin/python extrair_apenas.py <nome_da_tabela> [limite]
"""

import configparser
import os
import sys

import trino
from dotenv import load_dotenv

sys.path.insert(0, "src")

from data_mart.loader import DataMartLoader  # noqa: E402
from extraction.trino_to_duckdb import extrair_para_duckdb  # noqa: E402

load_dotenv()

CAMINHO_DUCKDB = "/tmp/dqp_extrato.duckdb"
CAMINHO_FONTES_INI = "fontes.ini"


def _carregar_secao_fonte_trino(fonte: str) -> dict:
    """Lê a seção [<fonte>] de fontes.ini e confere que é tipo=trino (único
    tipo que este script sabe extrair)."""
    if not os.path.exists(CAMINHO_FONTES_INI):
        raise ValueError(
            f"Arquivo '{CAMINHO_FONTES_INI}' não encontrado: copie "
            f"'{CAMINHO_FONTES_INI}.example' para '{CAMINHO_FONTES_INI}' e preencha."
        )
    config = configparser.ConfigParser()
    config.read(CAMINHO_FONTES_INI)
    if fonte not in config:
        secoes = ", ".join(config.sections()) or "(nenhuma)"
        raise ValueError(f"FONTE='{fonte}' não tem seção em '{CAMINHO_FONTES_INI}' (seções: {secoes}).")
    secao = dict(config[fonte])
    tipo = secao.get("tipo", "").strip().lower()
    if tipo != "trino":
        raise ValueError(f"extrair_apenas.py só suporta tipo=trino (seção [{fonte}] tem tipo='{tipo}').")
    return secao


def get_trino_connection(secao: dict):
    return trino.dbapi.connect(
        host=secao["host"],
        port=int(secao["port"]),
        user=secao["user"],
        catalog=secao["catalog"],
        http_scheme="https",
        auth=trino.auth.BasicAuthentication(secao["user"], secao["password"]),
    )


def datamart_connection_string() -> str:
    return (
        f"postgresql+psycopg2://{os.environ['PG_DATAMART_USER']}:{os.environ['PG_DATAMART_PASSWORD']}"
        f"@{os.environ['PG_DATAMART_HOST']}:{os.environ['PG_DATAMART_PORT']}/{os.environ['PG_DATAMART_DATABASE']}"
    )


def main():
    if len(sys.argv) < 2:
        print("Uso: FONTE=<nome> .venv/bin/python extrair_apenas.py <nome_da_tabela> [limite]")
        sys.exit(1)

    fonte = os.environ.get("FONTE", "").strip()
    if not fonte:
        print("FONTE não definida: ex.: FONTE=trino_bigdata .venv/bin/python extrair_apenas.py <tabela>")
        sys.exit(1)
    secao_fonte = _carregar_secao_fonte_trino(fonte)

    table_name = sys.argv[1]
    limite = int(sys.argv[2]) if len(sys.argv) > 2 else None

    trino_conn = get_trino_connection(secao_fonte)
    total_linhas, colunas = extrair_para_duckdb(
        trino_conn,
        secao_fonte["catalog"],
        secao_fonte["schema"],
        table_name,
        CAMINHO_DUCKDB,
        limite=limite,
    )
    trino_conn.close()
    print(f"Extração: {total_linhas} linhas, {len(colunas)} colunas -> {CAMINHO_DUCKDB}")

    loader = DataMartLoader(datamart_connection_string())
    sk_base = loader.registrar_base_leve(
        nome_base=table_name,
        sistema_origem=fonte,
        catalogo=secao_fonte["catalog"],
        schema_origem=secao_fonte["schema"],
        colunas=colunas,
    )
    print(f"Base registrada no data mart (sem perfilar): sk_base={sk_base}")
    print("Já aparece no Streamlit (app_regras.py) pra configurar regras.")
    print("Rode persistir_datamart_duckdb.py quando quiser o profiling completo.")


if __name__ == "__main__":
    main()
