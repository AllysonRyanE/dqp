"""
Descoberta automática de tabelas por fonte, usada só no modo "base inteira"
(TABLES=TODAS) de `persistir_datamart_duckdb.py`. Só descobre NOMES de
tabela; a extração em si continua a cargo de `extraction/*_to_duckdb.py`.

Postgres/MySQL/SQL Server usam a mesma query via `information_schema.tables`
(padrão ANSI). Oracle não expõe `information_schema`, usa `ALL_TABLES`, e
precisa do schema em MAIÚSCULO (mesma particularidade de
`extraction/oracle_to_duckdb.py`). Trino também expõe
`information_schema.tables`, por analogia, mas não validado contra um Trino
real nesta sandbox.

Parquet não tem "schema": "base inteira" vira um diretório com múltiplos
`.parquet`, cada um uma tabela distinta (diferente do glob particionado,
onde vários arquivos formam uma tabela só). `listar_arquivos_parquet` não
desce em subpastas, pra não misturar com particionamento Hive
(`dados/ano=2024/*.parquet` é uma tabela só, não várias).
"""

import os
from typing import List

_QUERY_INFORMATION_SCHEMA = (
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_schema = {marcador} AND table_type = 'BASE TABLE' "
    "ORDER BY table_name"
)


def listar_tabelas_postgres(conn, schema: str) -> List[str]:
    """Lista tabelas base (exclui views) do schema Postgres via
    `information_schema.tables`, na mesma conexão já aberta pra extração."""
    cur = conn.cursor()
    cur.execute(_QUERY_INFORMATION_SCHEMA.format(marcador="%s"), (schema,))
    nomes = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.commit()
    return nomes


def listar_tabelas_mysql(conn, schema: str) -> List[str]:
    """Lista tabelas base do "schema" MySQL (schema == database em MySQL)
    via `information_schema.tables`, mesma query do Postgres."""
    cur = conn.cursor()
    cur.execute(_QUERY_INFORMATION_SCHEMA.format(marcador="%s"), (schema,))
    nomes = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.commit()
    return nomes


def listar_tabelas_sqlserver(conn, schema: str) -> List[str]:
    """Lista tabelas base do schema SQL Server (ex. "dbo") via
    `information_schema.tables`, mesma query do Postgres/MySQL."""
    cur = conn.cursor()
    cur.execute(_QUERY_INFORMATION_SCHEMA.format(marcador="%s"), (schema,))
    nomes = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.commit()
    return nomes


def listar_tabelas_oracle(conn, schema: str) -> List[str]:
    """Lista tabelas do schema (owner) Oracle via `ALL_TABLES`, já que
    Oracle não expõe `information_schema`. `schema` vai em MAIÚSCULO pra
    bater com `ALL_TABLES.OWNER` (mesma particularidade de
    `extraction/oracle_to_duckdb.py`). oracledb usa paramstyle 'named'
    (`:schema`), diferente do `%s` dos outros três drivers."""
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name FROM all_tables WHERE owner = :schema ORDER BY table_name",
        {"schema": schema.upper()},
    )
    nomes = [r[0] for r in cur.fetchall()]
    cur.close()
    return nomes


def listar_tabelas_trino(conn, schema: str) -> List[str]:
    """Lista tabelas do schema Trino via `information_schema.tables`
    (catálogo já selecionado na conexão). Por analogia ao mesmo padrão ANSI
    testado nos outros bancos, não validado contra um Trino real nesta
    sandbox. `trino.dbapi.paramstyle` é 'qmark' (`?`), diferente dos outros."""
    cur = conn.cursor()
    cur.execute(_QUERY_INFORMATION_SCHEMA.format(marcador="?"), (schema,))
    nomes = [r[0] for r in cur.fetchall()]
    cur.close()
    return nomes


def listar_arquivos_parquet(diretorio: str) -> List[str]:
    """Lista arquivos `.parquet` diretamente dentro de `diretorio`, SEM
    descer recursivamente (ver docstring do módulo pro motivo). Cada arquivo
    vira o nome de uma tabela (nome do arquivo sem extensão). Ordenado
    alfabeticamente pra execução determinística (mesma ordem entre rodadas)."""
    if not os.path.isdir(diretorio):
        raise NotADirectoryError(
            f"TABLES=TODAS com FONTE=parquet espera um DIRETÓRIO em PARQUET_SOURCE_PATH "
            f"(um arquivo .parquet por tabela): '{diretorio}' não é um diretório existente."
        )
    arquivos = sorted(f for f in os.listdir(diretorio) if f.lower().endswith(".parquet"))
    return [os.path.splitext(f)[0] for f in arquivos]
