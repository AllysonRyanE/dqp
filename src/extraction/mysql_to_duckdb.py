"""
Extração de um MySQL arbitrário (fonte a perfilar, não o Postgres do data
mart) pra um arquivo DuckDB local, em lotes: mesma lógica de
`extraction/postgres_to_duckdb.py`, trocando o client pra PyMySQL.

A tabela usa o tipo nativo da coluna, lido do `FIELD_TYPE` do protocolo
MySQL em `cursor.description`. Com `SSCursor`, `.description` já vem
populado logo após `execute()` (o protocolo MySQL manda os metadados das
colunas antes de qualquer linha), diferente do cursor nomeado do psycopg2,
então a tabela é criada antes do loop de fetch, igual ao Trino. Tipo sem
mapeamento claro (JSON, BLOB, ENUM, SET, BIT, GEOMETRY etc.) cai em VARCHAR.

Carga de cada lote via DataFrame pandas + "INSERT ... SELECT * FROM df",
igual ao caminho Trino/Postgres.
"""

import duckdb
import pandas as pd
from loguru import logger
from pymysql.constants import FIELD_TYPE
from pymysql.cursors import SSCursor

TAMANHO_LOTE_PADRAO = 10_000

# Mapeamento de FIELD_TYPE (protocolo MySQL) -> tipo DuckDB. O resto cai em
# VARCHAR (ver `_tipo_duckdb_mysql`).
#
# MySQL não tem BOOLEAN nativo: BOOLEAN/BOOL são só apelidos de TINYINT(1),
# indistinguíveis no protocolo de um TINYINT comum. Por isso TINY entra como
# BIGINT, não BOOLEAN: um valor como 5 quebraria um CAST pra BOOLEAN.
_MYSQL_TYPE_PARA_DUCKDB = {
    FIELD_TYPE.TINY: "BIGINT",
    FIELD_TYPE.SHORT: "BIGINT",
    FIELD_TYPE.LONG: "BIGINT",
    FIELD_TYPE.LONGLONG: "BIGINT",
    FIELD_TYPE.INT24: "BIGINT",
    FIELD_TYPE.YEAR: "BIGINT",
    FIELD_TYPE.FLOAT: "DOUBLE",
    FIELD_TYPE.DOUBLE: "DOUBLE",
    FIELD_TYPE.DATE: "DATE",
    FIELD_TYPE.NEWDATE: "DATE",
    FIELD_TYPE.TIME: "TIME",
    FIELD_TYPE.TIMESTAMP: "TIMESTAMP",
    FIELD_TYPE.DATETIME: "TIMESTAMP",
    FIELD_TYPE.VARCHAR: "VARCHAR",
    FIELD_TYPE.VAR_STRING: "VARCHAR",
    FIELD_TYPE.STRING: "VARCHAR",
}

_MYSQL_TIPOS_DECIMAL = (FIELD_TYPE.DECIMAL, FIELD_TYPE.NEWDECIMAL)
_DECIMAL_PRECISAO_MAXIMA_DUCKDB = 38


def _tipo_duckdb_mysql(col) -> str:
    """Resolve o tipo DuckDB a partir de um item de `cursor.description` do
    PyMySQL, uma tupla crua de 7 posições (name, type_code, display_size,
    internal_size, precision, scale, null_ok), daí o acesso por índice.

    DECIMAL/NEWDECIMAL com precisão/escala exatas vira DECIMAL(p,s); sem
    precisão ou acima do limite do DuckDB (38), cai pra DOUBLE."""
    type_code, precision, scale = col[1], col[4], col[5]
    if type_code in _MYSQL_TIPOS_DECIMAL:
        if precision is not None and scale is not None and precision <= _DECIMAL_PRECISAO_MAXIMA_DUCKDB:
            return f"DECIMAL({precision},{scale})"
        return "DOUBLE"
    return _MYSQL_TYPE_PARA_DUCKDB.get(type_code, "VARCHAR")


def extrair_para_duckdb_mysql(
    mysql_conn,
    schema: str,
    tabela: str,
    caminho_duckdb: str,
    tamanho_lote: int = TAMANHO_LOTE_PADRAO,
    limite: int = None,
) -> tuple[int, list[str]]:
    """
    Extrai `schema.tabela` de um MySQL (fonte) pra uma tabela de mesmo nome
    num arquivo DuckDB local, lendo em lotes via `cursor.fetchmany` com
    `SSCursor` (server-side), senão o cursor padrão baixaria o result set
    inteiro já no `execute()`.

    Espelha `postgres_to_duckdb.extrair_para_duckdb_postgres`, sem
    `catalogo`: em MySQL o "catálogo" ANSI já é o banco da conexão.

    Returns:
        (qtd_linhas_extraidas, nomes_das_colunas)
    """
    cur = mysql_conn.cursor(SSCursor)
    query = f"SELECT * FROM `{schema}`.`{tabela}`"
    if limite is not None:
        query += f" LIMIT {limite}"
    cur.execute(query)
    colunas = [d[0] for d in cur.description]
    tipos_duckdb = [_tipo_duckdb_mysql(d) for d in cur.description]

    # Garante fechar a conexão DuckDB mesmo se um lote falhar, senão ela
    # fica presa no arquivo e bloqueia as próximas tabelas em TABLES=TODAS.
    con = duckdb.connect(caminho_duckdb)
    try:
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
    finally:
        con.close()

    cur.close()
    logger.success(f"Extração de {tabela} concluída: {total} linhas em {caminho_duckdb}")
    return total, colunas
