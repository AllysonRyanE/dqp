"""
Extração de um Postgres arbitrário (fonte a perfilar, não o Postgres do
data mart) pra um arquivo DuckDB local, em lotes: mesma lógica de
`extraction/trino_to_duckdb.py`, trocando o client pra psycopg2.

A tabela criada usa o tipo nativo da coluna, lido do OID de `pg_type` em
`cursor.description`, em vez de forçar tudo pra VARCHAR, sem precisar
examinar os dados. Qualquer OID sem mapeamento claro (enum, array, json,
uuid etc.) cai em VARCHAR, nunca um CAST agressivo que possa quebrar o INSERT.

Carga de cada lote via DataFrame pandas + "INSERT ... SELECT * FROM df",
igual ao caminho Trino. O `INSERT ... SELECT` faz o CAST implícito de
VARCHAR pro tipo nativo da coluna de destino.
"""

import duckdb
import pandas as pd
from loguru import logger

TAMANHO_LOTE_PADRAO = 10_000

# Mapeamento de OID (pg_type, estável entre versões) -> tipo DuckDB. Cobre
# só os tipos built-in mais comuns; o resto cai em VARCHAR.
_POSTGRES_OID_PARA_DUCKDB = {
    16: "BOOLEAN",  # bool
    21: "BIGINT",  # int2 / smallint
    23: "BIGINT",  # int4 / integer
    20: "BIGINT",  # int8 / bigint
    700: "DOUBLE",  # float4 / real
    701: "DOUBLE",  # float8 / double precision
    1082: "DATE",  # date
    1083: "TIME",  # time without time zone
    1114: "TIMESTAMP",  # timestamp without time zone
    1184: "TIMESTAMPTZ",  # timestamp with time zone
    25: "VARCHAR",  # text
    1043: "VARCHAR",  # varchar
    1042: "VARCHAR",  # bpchar / char(n)
    18: "VARCHAR",  # "char" (tipo interno de 1 byte)
}

_OID_NUMERIC = 1700  # numeric/decimal: tratado à parte, pode ter precisão/escala exatas
_DECIMAL_PRECISAO_MAXIMA_DUCKDB = 38


def _tipo_duckdb_postgres(col) -> str:
    """Resolve o tipo DuckDB a partir do `cursor.description` do psycopg2.
    NUMERIC/DECIMAL com precisão/escala exatas vira DECIMAL(p,s), pra evitar
    arredondamento em colunas monetárias; sem precisão declarada ou acima
    do limite do DuckDB (38), cai pra DOUBLE."""
    if col.type_code == _OID_NUMERIC:
        if col.precision is not None and col.scale is not None and col.precision <= _DECIMAL_PRECISAO_MAXIMA_DUCKDB:
            return f"DECIMAL({col.precision},{col.scale})"
        return "DOUBLE"
    return _POSTGRES_OID_PARA_DUCKDB.get(col.type_code, "VARCHAR")


def extrair_para_duckdb_postgres(
    pg_conn,
    schema: str,
    tabela: str,
    caminho_duckdb: str,
    tamanho_lote: int = TAMANHO_LOTE_PADRAO,
    limite: int = None,
) -> tuple[int, list[str]]:
    """
    Extrai `schema.tabela` de um Postgres (fonte) pra uma tabela de mesmo
    nome num arquivo DuckDB local, lendo em lotes via `cursor.fetchmany`
    (server-side cursor com `name=`, Postgres também não materializa tudo).

    Espelha `trino_to_duckdb.extrair_para_duckdb`, sem `catalogo`: em
    Postgres o "catálogo" ANSI já é o banco escolhido na conexão.

    Returns:
        (qtd_linhas_extraidas, nomes_das_colunas)
    """
    # Cursor nomeado ("server-side cursor"): sem isso, o psycopg2 buscaria o
    # resultado inteiro do servidor pro cliente antes do primeiro fetchmany,
    # o que reintroduziria o problema de memória que essa extração evita.
    cur = pg_conn.cursor(name=f"dqp_extrai_{tabela}")
    cur.itersize = tamanho_lote
    query = f'SELECT * FROM "{schema}"."{tabela}"'
    if limite is not None:
        query += f" LIMIT {limite}"
    cur.execute(query)

    # Cursor nomeado: `.description` só é populado após o primeiro fetch,
    # diferente do Trino. Por isso o primeiro lote é buscado antes de criar
    # a tabela no DuckDB.
    primeiro_lote = cur.fetchmany(tamanho_lote)
    colunas = [d[0] for d in cur.description]
    tipos_duckdb = [_tipo_duckdb_postgres(d) for d in cur.description]

    # Garante fechar a conexão DuckDB mesmo se um lote falhar, senão ela
    # fica presa no arquivo e bloqueia as próximas tabelas em TABLES=TODAS.
    con = duckdb.connect(caminho_duckdb)
    try:
        con.execute(f'DROP TABLE IF EXISTS "{tabela}"')
        colunas_ddl = ", ".join(f'"{c}" {t}' for c, t in zip(colunas, tipos_duckdb))
        con.execute(f'CREATE TABLE "{tabela}" ({colunas_ddl})')

        total = 0
        lote = primeiro_lote
        while lote:
            df_lote = pd.DataFrame(
                [[None if v is None else str(v) for v in linha] for linha in lote],
                columns=colunas,
                dtype="string",
            )
            con.execute(f'INSERT INTO "{tabela}" SELECT * FROM df_lote')
            total += len(lote)
            logger.info(f"Extraído {total} linhas de {tabela} até agora...")
            lote = cur.fetchmany(tamanho_lote)
    finally:
        con.close()

    cur.close()
    pg_conn.commit()  # cursor nomeado abre uma transação implícita, encerra aqui
    logger.success(f"Extração de {tabela} concluída: {total} linhas em {caminho_duckdb}")
    return total, colunas
