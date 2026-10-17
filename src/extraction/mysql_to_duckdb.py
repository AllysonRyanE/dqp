"""
Extração de um MySQL arbitrário (a FONTE de dados a perfilar: não confundir
com o Postgres do data mart, que é DESTINO dos resultados) pra um arquivo
DuckDB local, em lotes: mesma lógica de `extraction/postgres_to_duckdb.py`,
só trocando o client de conexão (PyMySQL em vez de psycopg2). Nunca
materializa o resultado inteiro em memória Python de uma vez: é o mesmo
argumento central do TCC sobre volume, agora valendo também pra fontes MySQL.

A tabela criada no DuckDB usa o tipo NATIVO da coluna, lido do metadado do
MySQL (`cursor.description[i].type_code`, o `FIELD_TYPE` do protocolo MySQL)
em vez de forçar tudo pra VARCHAR: mesma ideia de postgres_to_duckdb.py,
O(1) por coluna, não uma varredura dos dados. Diferença de timing importante
em relação ao Postgres: com `SSCursor` (server-side) do PyMySQL, o
`cursor.description` já vem populado logo após `execute()` (confirmado
empiricamente): o protocolo MySQL manda o pacote de metadados das colunas
antes de qualquer linha, então não existe aqui o mesmo problema que o cursor
nomeado do psycopg2 tem (lá, `.description` só fica pronto depois do
primeiro fetch). Por isso a tabela é criada antes do loop de fetch, igual ao
Trino, sem precisar do truque de "buscar o primeiro lote antes". Qualquer
tipo sem mapeamento claro (JSON, BLOB, ENUM, SET, BIT, GEOMETRY etc.) cai em
VARCHAR: nunca arriscamos um CAST agressivo que possa quebrar o INSERT.

Carga de cada lote via DataFrame pandas pequeno + "INSERT ... SELECT * FROM
df" (DuckDB lê o DataFrame local direto), igual ao caminho Trino/Postgres. Os
valores continuam sendo convertidos pra string em Python antes de entrar no
DataFrame: é o `INSERT ... SELECT` que faz o CAST implícito de VARCHAR pro
tipo nativo da coluna de destino (mesmo mecanismo testado em postgres_to_duckdb.py).
"""

import duckdb
import pandas as pd
from loguru import logger
from pymysql.constants import FIELD_TYPE
from pymysql.cursors import SSCursor

TAMANHO_LOTE_PADRAO = 10_000

# Mapeamento de tipo MySQL (constantes `FIELD_TYPE` do protocolo, expostas em
# `cursor.description[i].type_code`) -> tipo DuckDB. Conservador: qualquer
# código fora daqui cai em VARCHAR (ver `_tipo_duckdb_mysql`).
#
# MySQL não tem um tipo BOOLEAN nativo de verdade: `BOOLEAN`/`BOOL` são só
# apelidos de `TINYINT(1)`, indistinguíveis no protocolo de um `TINYINT`
# comum usado como inteiro pequeno (o `type_code` é o mesmo `FIELD_TYPE.TINY`
# nos dois casos). Por isso TINY entra como BIGINT, não BOOLEAN: é a escolha
# segura, já que um valor como 5 num TINYINT numérico quebraria um CAST pra
# BOOLEAN, e um valor 0/1 continua perfeitamente representável (e comprimido)
# como BIGINT.
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
    """
    Resolve o tipo DuckDB pra uma coluna a partir de um item de
    `cursor.description` do PyMySQL. Diferente do psycopg2 (que expõe
    `Column`, um namedtuple com atributos), o PyMySQL devolve uma tupla
    Python "crua" de 7 posições (confirmado empiricamente): mesmas posições
    do padrão DB-API: (name, type_code, display_size, internal_size,
    precision, scale, null_ok), daí o acesso por índice aqui.

    DECIMAL/NEWDECIMAL é tratado à parte, igual à NUMERIC do Postgres: usa
    `DECIMAL(precisao,escala)` quando o MySQL informa precisão/escala exatas
    (preserva o valor exato, sem arredondamento de DOUBLE: relevante pra
    colunas monetárias como `saldo`), caindo pra DOUBLE só quando a precisão
    não vem ou excede o limite do DuckDB (38).
    """
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
    num arquivo DuckDB local, lendo em lotes de `tamanho_lote` linhas via
    `cursor.fetchmany` (cursor "server-side" `SSCursor` do PyMySQL: sem ele,
    o cursor client-side padrão do PyMySQL/mysql-connector-python baixaria o
    resultado inteiro do servidor pro cliente antes do primeiro fetchmany,
    exatamente a mesma armadilha do cursor comum do psycopg2 no Postgres).

    Espelha a assinatura/comportamento de
    `postgres_to_duckdb.extrair_para_duckdb_postgres`, trocando o "catálogo"
    (Trino) por nada: em MySQL o "catálogo" ANSI também é o próprio banco
    de dados/schema já escolhido na conexão, então só `schema.tabela` é
    necessário aqui (mesmo raciocínio do Postgres).

    Returns:
        (qtd_linhas_extraidas, nomes_das_colunas)
    """
    # SSCursor ("server-side cursor"): sem isso, o PyMySQL usaria o cursor
    # padrão (client-side), que baixa TODO o result set do servidor pro
    # cliente já no `execute()`, reintroduzindo o problema de memória que
    # essa extração evita. Com SSCursor, cada `fetchmany` busca só o próximo
    # lote direto do socket MySQL.
    cur = mysql_conn.cursor(SSCursor)
    query = f"SELECT * FROM `{schema}`.`{tabela}`"
    if limite is not None:
        query += f" LIMIT {limite}"
    cur.execute(query)
    colunas = [d[0] for d in cur.description]
    tipos_duckdb = [_tipo_duckdb_mysql(d) for d in cur.description]

    # try/finally em volta de todo o uso de `con` (ver nota igual em
    # extraction/parquet_to_duckdb.py, onde o bug foi encontrado primeiro):
    # garante fechar a conexão DuckDB mesmo se um lote falhar no meio, senão
    # ela fica aberta pro arquivo `caminho_duckdb` e bloqueia o profiler de
    # abrir esse mesmo arquivo (read_only) pra QUALQUER tabela seguinte no
    # modo "base inteira" (TABLES=TODAS).
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
