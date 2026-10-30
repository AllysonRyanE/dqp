"""
Extração de um SQL Server arbitrário (fonte a perfilar, não o Postgres do
data mart) pra um arquivo DuckDB local, em lotes: mesma lógica de
`extraction/postgres_to_duckdb.py`, trocando o client pra pymssql.

Driver escolhido: pymssql, não pyodbc. pymssql instala como wheel
autocontido (bundla o FreeTDS); pyodbc exige o driver ODBC da Microsoft
instalado a nível de SO, com privilégio de root pra instalar.

Limitação do pymssql: `cursor.description` não expõe o tipo nativo da
coluna, só 5 categorias DB-API genéricas (BIT cai no mesmo bucket de
INT/BIGINT, e DATE/DATETIME2/DATETIMEOFFSET caem todos em BINARY). Por
isso esta extração consulta `INFORMATION_SCHEMA.COLUMNS` pra pegar
`DATA_TYPE`/`NUMERIC_PRECISION`/`NUMERIC_SCALE` exatos, casados por nome
com as colunas de `cursor.description` (que continua confiável pra nome e
ordem). Coluna ausente no INFORMATION_SCHEMA cai em VARCHAR.

`cursor.description` fica disponível logo após `execute()`, igual
Trino/MySQL, diferente do cursor nomeado do psycopg2. `fetchmany()` busca
linha a linha via DB-Library, streaming real mesmo sem SSCursor.

Carga de cada lote via DataFrame pandas + "INSERT ... SELECT * FROM df",
igual às outras fontes.
"""

import duckdb
import pandas as pd
from loguru import logger

TAMANHO_LOTE_PADRAO = 10_000

_DECIMAL_PRECISAO_MAXIMA_DUCKDB = 38

# Mapeamento de DATA_TYPE do SQL Server -> tipo DuckDB. O resto (UNIQUEIDENTIFIER,
# XML, GEOGRAPHY etc.) cai em VARCHAR (ver `_tipo_duckdb_sqlserver`).
_SQLSERVER_TIPO_PARA_DUCKDB = {
    "int": "BIGINT",
    "bigint": "BIGINT",
    "smallint": "BIGINT",
    "tinyint": "BIGINT",
    "float": "DOUBLE",
    "real": "DOUBLE",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "datetime2": "TIMESTAMP",
    "smalldatetime": "TIMESTAMP",
    "datetimeoffset": "TIMESTAMPTZ",
    "bit": "BOOLEAN",
    "varchar": "VARCHAR",
    "nvarchar": "VARCHAR",
    "char": "VARCHAR",
    "nchar": "VARCHAR",
    "text": "VARCHAR",
    "ntext": "VARCHAR",
    # money/smallmoney têm precisão/escala fixas no próprio padrão T-SQL.
    "money": "DECIMAL(19,4)",
    "smallmoney": "DECIMAL(10,4)",
}

_SQLSERVER_TIPOS_DECIMAL = ("decimal", "numeric")


def _tipo_duckdb_sqlserver(data_type: str, precision, scale) -> str:
    """Resolve o tipo DuckDB a partir de `INFORMATION_SCHEMA.COLUMNS`.
    DECIMAL/NUMERIC com precisão/escala exatas vira DECIMAL(p,s); sem
    precisão ou acima do limite do DuckDB (38), cai pra DOUBLE."""
    data_type = (data_type or "").strip().lower()
    if data_type in _SQLSERVER_TIPOS_DECIMAL:
        if precision is not None and scale is not None and precision <= _DECIMAL_PRECISAO_MAXIMA_DUCKDB:
            return f"DECIMAL({precision},{scale})"
        return "DOUBLE"
    return _SQLSERVER_TIPO_PARA_DUCKDB.get(data_type, "VARCHAR")


def _metadado_colunas_information_schema(conn, schema: str, tabela: str) -> dict:
    """Busca DATA_TYPE/NUMERIC_PRECISION/NUMERIC_SCALE de cada coluna via
    `INFORMATION_SCHEMA.COLUMNS` (ver docstring do módulo pra motivo).

    Returns:
        dict {nome_coluna: (data_type, precision, scale)}, chaves em
        lower-case (SQL Server é case-insensitive por collation padrão).
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        (schema, tabela),
    )
    metadado = {}
    for nome, data_type, precision, scale in cur.fetchall():
        metadado[nome.lower()] = (data_type, precision, scale)
    cur.close()
    return metadado


def extrair_para_duckdb_sqlserver(
    conn,
    schema: str,
    tabela: str,
    caminho_duckdb: str,
    tamanho_lote: int = TAMANHO_LOTE_PADRAO,
    limite: int = None,
) -> tuple[int, list[str]]:
    """
    Extrai `schema.tabela` de um SQL Server (fonte) pra uma tabela de mesmo
    nome num arquivo DuckDB local, lendo em lotes via `cursor.fetchmany`.

    Espelha `postgres_to_duckdb.extrair_para_duckdb_postgres`, sem
    `catalogo`: em SQL Server o "catálogo" ANSI já é o banco da conexão.

    Returns:
        (qtd_linhas_extraidas, nomes_das_colunas)
    """
    metadado_colunas = _metadado_colunas_information_schema(conn, schema, tabela)

    cur = conn.cursor()
    query = f"SELECT {'TOP ' + str(limite) + ' ' if limite is not None else ''}* FROM [{schema}].[{tabela}]"
    cur.execute(query)

    # pymssql popula `.description` logo após `execute()`, diferente do
    # cursor nomeado do psycopg2/Postgres.
    colunas = [d[0] for d in cur.description]
    tipos_duckdb = []
    for nome_coluna in colunas:
        info = metadado_colunas.get(nome_coluna.lower())
        if info is None:
            logger.warning(
                f"Coluna '{nome_coluna}' de {tabela} não encontrada em "
                "INFORMATION_SCHEMA.COLUMNS: usando VARCHAR como fallback seguro."
            )
            tipos_duckdb.append("VARCHAR")
            continue
        data_type, precision, scale = info
        tipos_duckdb.append(_tipo_duckdb_sqlserver(data_type, precision, scale))

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
