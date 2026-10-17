"""
Extrai dados do Trino pra um arquivo DuckDB local, em lotes, sem carregar o
resultado inteiro na memória de uma vez.

As colunas do DuckDB usam o tipo nativo informado pelo Trino
(`cursor.description`), não VARCHAR genérico: melhora a compressão. Tipo
sem mapeamento conhecido cai em VARCHAR. Mapeamento feito por analogia ao
de Postgres/MySQL; não validado contra um Trino real (sem acesso de rede
ao BigDataPE nesta sandbox).

Cada lote vira um DataFrame pandas pequeno, inserido via
"INSERT ... SELECT FROM df" (bem mais rápido que inserir linha a linha).
"""

import re

import duckdb
import pandas as pd
from loguru import logger

TAMANHO_LOTE_PADRAO = 10_000

_DECIMAL_PRECISAO_MAXIMA_DUCKDB = 38
_REGEX_DECIMAL = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")


def _tipo_duckdb_trino(type_signature: str) -> str:
    """
    Converte a string de tipo do Trino (ex. "bigint", "decimal(10,2)") pro
    tipo DuckDB correspondente. Tipo não reconhecido cai em VARCHAR.
    """
    t = type_signature.strip().lower()

    if t in ("tinyint", "smallint", "integer", "bigint"):
        return "BIGINT"
    if t in ("real", "double"):
        return "DOUBLE"
    if t == "boolean":
        return "BOOLEAN"
    if t.startswith("decimal"):
        m = _REGEX_DECIMAL.match(t)
        if m:
            precisao, escala = int(m.group(1)), int(m.group(2))
            if precisao <= _DECIMAL_PRECISAO_MAXIMA_DUCKDB:
                return f"DECIMAL({precisao},{escala})"
        return "DOUBLE"
    if t == "date":
        return "DATE"
    if t.startswith("timestamp"):
        return "TIMESTAMPTZ" if "with time zone" in t else "TIMESTAMP"
    if t.startswith("time"):
        return "TIME"
    if t.startswith("varchar") or t.startswith("char"):
        return "VARCHAR"
    return "VARCHAR"


def extrair_para_duckdb(
    trino_conn,
    catalogo: str,
    schema: str,
    tabela: str,
    caminho_duckdb: str,
    tamanho_lote: int = TAMANHO_LOTE_PADRAO,
    limite: int = None,
) -> tuple[int, list[str]]:
    """
    Extrai `catalogo.schema.tabela` do Trino pra uma tabela de mesmo nome
    num arquivo DuckDB local, lendo em lotes de `tamanho_lote` linhas.

    Returns:
        (qtd_linhas_extraidas, nomes_das_colunas)
    """
    cur = trino_conn.cursor()
    query = f"SELECT * FROM {catalogo}.{schema}.{tabela}"
    if limite is not None:
        query += f" LIMIT {limite}"
    cur.execute(query)
    colunas = [d[0] for d in cur.description]
    tipos_duckdb = [_tipo_duckdb_trino(d[1]) for d in cur.description]

    con = duckdb.connect(caminho_duckdb)
    con.execute(f'DROP TABLE IF EXISTS "{tabela}"')
    colunas_ddl = ", ".join(f'"{c}" {t}' for c, t in zip(colunas, tipos_duckdb))
    con.execute(f'CREATE TABLE "{tabela}" ({colunas_ddl})')

    total = 0
    while True:
        lote = cur.fetchmany(tamanho_lote)
        if not lote:
            break
        df_lote = pd.DataFrame(
            [[None if v is None else str(v) for v in linha] for linha in lote],
            columns=colunas,
            dtype="string",
        )
        con.execute(f'INSERT INTO "{tabela}" SELECT * FROM df_lote')
        total += len(lote)
        logger.info(f"Extraído {total} linhas de {tabela} até agora...")

    con.close()
    logger.success(f"Extração de {tabela} concluída: {total} linhas em {caminho_duckdb}")
    return total, colunas
