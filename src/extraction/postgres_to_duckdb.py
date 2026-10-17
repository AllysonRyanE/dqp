"""
Extração de um Postgres arbitrário (a FONTE de dados a perfilar: não
confundir com o Postgres do data mart, que é DESTINO dos resultados) pra um
arquivo DuckDB local, em lotes: mesma lógica de `extraction/trino_to_duckdb.py`,
só trocando o client de conexão (psycopg2 em vez de trino.dbapi). Nunca
materializa o resultado inteiro em memória Python de uma vez: é o mesmo
argumento central do TCC sobre volume, agora valendo também pra fontes
Postgres.

A tabela criada no DuckDB usa o tipo NATIVO da coluna, lido do metadado do
Postgres (`cursor.description[i].type_code`, o OID de `pg_type`: já
disponível sem examinar uma única linha, ver nota mais abaixo sobre timing
do cursor nomeado), em vez de forçar tudo pra VARCHAR. Isso é O(1) por
coluna (um dicionário fixo de OID->tipo DuckDB), não uma varredura dos
dados, então não reintroduz o problema de memória que a inferência de tipo
via pandas causava antes: a inferência de tipo (Numeric/Text/Categorical/
DateTime) continua sendo responsabilidade só do profiler, essa mudança aqui
é só sobre a tipagem FÍSICA da coluna no DuckDB, que afeta a compressão.
Qualquer OID sem mapeamento claro (enum, array, json, uuid, tipos de
extensão etc.) cai em VARCHAR: mesmo comportamento de antes pra esses
casos, nunca arriscamos um CAST agressivo que possa quebrar o INSERT.

Carga de cada lote via DataFrame pandas pequeno + "INSERT ... SELECT * FROM
df" (DuckDB lê o DataFrame local direto), igual ao caminho Trino. Os valores
continuam sendo convertidos pra string em Python antes de entrar no
DataFrame (mesma lógica de sempre): é o `INSERT ... SELECT` que faz o CAST
implícito de VARCHAR pro tipo nativo da coluna de destino (testado: DuckDB
aceita esse cast implícito pra BIGINT/DOUBLE/DECIMAL/DATE/TIMESTAMP/
TIMESTAMPTZ/BOOLEAN a partir da representação string do valor Python).
"""

import duckdb
import pandas as pd
from loguru import logger

TAMANHO_LOTE_PADRAO = 10_000

# Mapeamento de tipo Postgres (OID de pg_type, estável: são os tipos
# embutidos do protocolo, não mudam entre versões) -> tipo DuckDB.
# Conservador: só cobre os tipos built-in mais comuns; qualquer OID fora
# daqui cai em VARCHAR (ver `_tipo_duckdb_postgres`).
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
    """
    Resolve o tipo DuckDB pra uma coluna a partir do `cursor.description`
    do psycopg2 (namedtuple com `type_code`, `precision`, `scale`).

    NUMERIC/DECIMAL é o único caso especial: quando o Postgres informa
    precisão e escala exatas (coluna declarada como `numeric(p,s)`), usamos
    `DECIMAL(p,s)` no DuckDB pra preservar o valor exato (sem o risco de
    arredondamento de ponto flutuante que DOUBLE teria, relevante pra
    colunas monetárias). Quando não há precisão declarada (`numeric` sem
    parênteses) ou ela excede o limite do DuckDB (38), cai pra DOUBLE,
    ainda tipado nativamente (ganha compressão), só sem a garantia de
    precisão exata.
    """
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
    nome num arquivo DuckDB local, lendo em lotes de `tamanho_lote` linhas
    via `cursor.fetchmany` (server-side cursor com `name=`, então o Postgres
    também não materializa o resultado inteiro do lado dele).

    Espelha a assinatura/comportamento de `trino_to_duckdb.extrair_para_duckdb`,
    trocando `catalogo` (Trino) por nada: em Postgres o "catálogo" ANSI é o
    próprio banco de dados já escolhido na conexão (`dbname`), então só
    `schema.tabela` é necessário aqui.

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

    # Cursor nomeado: o psycopg2 só popula `.description` depois do primeiro
    # fetch (é só um DECLARE CURSOR até lá): diferente do cursor normal do
    # Trino, que já tem description logo após o execute. Por isso o primeiro
    # lote é buscado antes de criar a tabela no DuckDB, e processado dentro
    # do mesmo loop dos demais (nada de especial acontece com ele).
    primeiro_lote = cur.fetchmany(tamanho_lote)
    colunas = [d[0] for d in cur.description]
    tipos_duckdb = [_tipo_duckdb_postgres(d) for d in cur.description]

    # try/finally em volta de todo o uso de `con`: garante fechar a conexão
    # DuckDB mesmo se um lote falhar no meio (ex. erro de CAST num
    # `INSERT ... SELECT`): sem isso, a conexão fica aberta pro arquivo
    # `caminho_duckdb` e bloqueia o profiler de abrir esse mesmo arquivo
    # (read_only) pra QUALQUER tabela seguinte no modo "base inteira"
    # (TABLES=TODAS, ver persistir_datamart_duckdb.py): bug real reproduzido
    # e corrigido na tarefa que introduziu esse modo (ver mesma nota em
    # extraction/parquet_to_duckdb.py, onde foi encontrado primeiro).
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
    pg_conn.commit()  # cursor nomeado abre uma transação implícita; encerra aqui
    logger.success(f"Extração de {tabela} concluída: {total} linhas em {caminho_duckdb}")
    return total, colunas
