"""
Extração de um Oracle arbitrário (fonte a perfilar, não o Postgres do data
mart) pra um arquivo DuckDB local, em lotes: mesma lógica de
`extraction/postgres_to_duckdb.py`, trocando o client pra python-oracledb.

Driver: python-oracledb em modo THIN, sem precisar do Oracle Instant Client
instalado. `cursor.description` já vem populado logo após `execute()`,
antes do fetch (igual Trino/PyMySQL, diferente do cursor nomeado do
psycopg2), então a tabela DuckDB é criada antes do loop, sem o truque de
"buscar o primeiro lote antes" que o Postgres precisa.

A tabela usa o tipo nativo da coluna (`cursor.description[i].type_code`, um
`oracledb.DbType`), em vez de forçar tudo pra VARCHAR.

Particularidades do sistema de tipos do Oracle (ver `_tipo_duckdb_oracle_number`
e `_ORACLE_DBTYPE_PARA_DUCKDB` abaixo):

1. NUMBER é o único tipo numérico do Oracle, sem INTEGER/DECIMAL separados.
   Sem precisão/escala declarada (sentinela precision=0, scale=-127) cai em
   DOUBLE, por não ter teto seguro pra decidir um DECIMAL. Com escala <= 0
   e precisão <= 18 vira BIGINT; entre 19-38, DECIMAL(p, 0). Com escala > 0,
   DECIMAL(p, s) preservando a precisão exata, evita erro de arredondamento
   em colunas monetárias. p > 38 cai em DOUBLE.

2. DATE do Oracle tem componente de hora, diferente do DATE ANSI do
   Postgres/MySQL. Mapeado pra TIMESTAMP, nunca pra DATE, que truncaria a
   hora silenciosamente no INSERT.

3. TIMESTAMP WITH TIME ZONE e WITH LOCAL TIME ZONE viram TIMESTAMPTZ, os
   dois DbTypes distintos mapeiam pro mesmo tipo no DuckDB.

4. VARCHAR2/NVARCHAR2/CHAR/NCHAR -> VARCHAR.

5. CLOB/NCLOB -> VARCHAR, com custo de leitura maior, aceitável pro volume
   testado aqui.

6. BLOB/RAW -> VARCHAR na DDL, mas o valor precisa de tratamento especial:
   `str()` num LOB binário lança TypeError, então é convertido pra
   hexadecimal (`v.read().hex()`) em vez de estourar a extração inteira.

7. Qualquer tipo sem mapeamento explícito cai em VARCHAR, mesmo
   comportamento conservador dos outros extratores.

Carga de cada lote via DataFrame pandas + "INSERT ... SELECT * FROM df",
igual aos outros caminhos.
"""

import oracledb
import duckdb
import pandas as pd
from loguru import logger

TAMANHO_LOTE_PADRAO = 10_000

_DECIMAL_PRECISAO_MAXIMA_DUCKDB = 38
_NUMBER_PRECISAO_MAXIMA_BIGINT = 18  # int64 com folga (max ~9.2e18, 19 dígitos)

# Mapeamento de tipo Oracle -> tipo DuckDB. NUMBER é tratado à parte (ver
# `_tipo_duckdb_oracle_number`). DbType fora daqui cai em VARCHAR.
_ORACLE_DBTYPE_PARA_DUCKDB = {
    oracledb.DB_TYPE_BINARY_FLOAT: "DOUBLE",
    oracledb.DB_TYPE_BINARY_DOUBLE: "DOUBLE",
    # Oracle DATE inclui hora: mapeia pra TIMESTAMP, nunca pra DATE.
    oracledb.DB_TYPE_DATE: "TIMESTAMP",
    oracledb.DB_TYPE_TIMESTAMP: "TIMESTAMP",
    oracledb.DB_TYPE_TIMESTAMP_TZ: "TIMESTAMPTZ",
    oracledb.DB_TYPE_TIMESTAMP_LTZ: "TIMESTAMPTZ",
    oracledb.DB_TYPE_VARCHAR: "VARCHAR",
    oracledb.DB_TYPE_NVARCHAR: "VARCHAR",
    oracledb.DB_TYPE_CHAR: "VARCHAR",
    oracledb.DB_TYPE_NCHAR: "VARCHAR",
    oracledb.DB_TYPE_CLOB: "VARCHAR",  # fallback: funciona, custo de leitura maior (nota 5)
    oracledb.DB_TYPE_NCLOB: "VARCHAR",
    oracledb.DB_TYPE_LONG: "VARCHAR",
    # BLOB/RAW: fallback VARCHAR físico, mas o VALOR precisa de tratamento
    # especial (ver `_valor_para_str_oracle` e nota 6 no docstring do módulo)
    # porque `str()` num LOB binário lança TypeError.
    oracledb.DB_TYPE_BLOB: "VARCHAR",
    oracledb.DB_TYPE_RAW: "VARCHAR",
    oracledb.DB_TYPE_LONG_RAW: "VARCHAR",
}


def _tipo_duckdb_oracle_number(precision, scale) -> str:
    """Heurística de NUMBER -> tipo DuckDB (ver nota 1 do módulo).
    precision=0 identifica um NUMBER puro, sem precisão/escala declarada."""
    if precision is None or precision == 0:
        return "DOUBLE"
    if scale is not None and scale > 0:
        if precision <= _DECIMAL_PRECISAO_MAXIMA_DUCKDB:
            return f"DECIMAL({precision},{scale})"
        return "DOUBLE"
    # scale <= 0 (inclui NUMBER(p) sem escala, que o driver relata scale=0,
    # e o caso incomum de escala negativa) -> tratado como inteiro exato.
    if precision <= _NUMBER_PRECISAO_MAXIMA_BIGINT:
        return "BIGINT"
    if precision <= _DECIMAL_PRECISAO_MAXIMA_DUCKDB:
        return f"DECIMAL({precision},0)"
    return "DOUBLE"


def _tipo_duckdb_oracle(col) -> str:
    """Resolve o tipo DuckDB a partir do `cursor.description` (type_code é
    um `oracledb.DbType`, não um int cru como no MySQL)."""
    type_code, precision, scale = col[1], col[4], col[5]
    if type_code == oracledb.DB_TYPE_NUMBER:
        return _tipo_duckdb_oracle_number(precision, scale)
    return _ORACLE_DBTYPE_PARA_DUCKDB.get(type_code, "VARCHAR")


def _valor_para_str_oracle(v) -> str:
    """Conversão valor -> string antes do DataFrame. Igual aos outros
    extratores (`str(v)`), exceto BLOB: `str(v)` lança TypeError em LOB
    binário, então vira hex (ver nota 6 do módulo)."""
    if isinstance(v, oracledb.LOB):
        try:
            return str(v)  # CLOB/NCLOB: texto
        except TypeError:
            return v.read().hex()  # BLOB: binário sem representação textual óbvia -> hex
    return str(v)


def extrair_para_duckdb_oracle(
    conn,
    schema: str,
    tabela: str,
    caminho_duckdb: str,
    tamanho_lote: int = TAMANHO_LOTE_PADRAO,
    limite: int = None,
) -> tuple[int, list[str]]:
    """
    Extrai `schema.tabela` de um Oracle (fonte) pra uma tabela de mesmo nome
    num arquivo DuckDB local, lendo em lotes via `cursor.fetchmany` (o
    cursor do python-oracledb já busca sob demanda, sem o problema do
    cursor client-side que Postgres/MySQL têm sem cursor nomeado/SSCursor).

    Espelha `postgres_to_duckdb.extrair_para_duckdb_postgres`, trocando
    "schema" pelo dono do objeto (`schema.tabela` = `OWNER.TABLE_NAME`).

    Oracle dobra identificador não citado pra MAIÚSCULO ao criar objetos
    (oposto do Postgres). Como a query usa identificador citado, `schema`/
    `tabela` são colocados em maiúsculo só pra montar a query; o nome no
    DuckDB continua usando o `tabela` original, que o resto da pipeline
    espera (ex. pra casar com `dim_base`/`TABLES=`).

    Returns:
        (qtd_linhas_extraidas, nomes_das_colunas)
    """
    schema_oracle = schema.upper()
    tabela_oracle = tabela.upper()

    cur = conn.cursor()
    cur.arraysize = tamanho_lote
    query = f'SELECT * FROM "{schema_oracle}"."{tabela_oracle}"'
    if limite is not None:
        query += f" FETCH FIRST {limite} ROWS ONLY"
    cur.execute(query)

    # `.description` já vem populado logo após `execute()`, antes do fetch:
    # igual MySQL/Trino, diferente do cursor nomeado do psycopg2.
    colunas = [d[0] for d in cur.description]
    tipos_duckdb = [_tipo_duckdb_oracle(d) for d in cur.description]

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
                [[None if v is None else _valor_para_str_oracle(v) for v in linha] for linha in lote],
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
