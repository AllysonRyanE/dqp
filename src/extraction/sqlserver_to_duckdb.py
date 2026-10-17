"""
Extração de um SQL Server arbitrário (a FONTE de dados a perfilar: não
confundir com o Postgres do data mart, que é DESTINO dos resultados) pra um
arquivo DuckDB local, em lotes: mesma lógica de
`extraction/postgres_to_duckdb.py`/`extraction/mysql_to_duckdb.py`, só
trocando o client de conexão (pymssql em vez de psycopg2/PyMySQL). Nunca
materializa o resultado inteiro em memória Python de uma vez.

Driver escolhido: pymssql, não pyodbc. Decisão tomada empiricamente nesta
sandbox (mesmo espírito da escolha do PyMySQL: preferir o que instala sem
dor, sem abrir mão de confiabilidade): pymssql instala como wheel manylinux
autocontido (bundla o FreeTDS estaticamente, testado: `pip install pymssql`
funcionou de primeira, sem nenhuma dependência de sistema). pyodbc também
instala como wheel puro, mas SÓ FUNCIONA com o driver ODBC da Microsoft
(`msodbcsql17`/`msodbcsql18`) instalado a nível de SO: que exige
`apt-get install` com aceite de licença, e que por sua vez exige privilégio
de root; esta sandbox não tem sudo sem senha, então pyodbc não é utilizável
aqui sem intervenção manual fora do alcance desta automação. Resultado:
pymssql é o que "instala sem dor" nesta e em outras sandboxes equivalentes
(container Docker de CI, por exemplo), então foi o escolhido.

ATENÇÃO: limitação empírica importante do pymssql que MUDA a estratégia de
tipagem em relação às outras fontes: examinando o código-fonte do pymssql
(`_mssql.pyx`, função `get_api_coltype`) e confirmado empiricamente contra um
SQL Server real (ver testes desta mudança), o `cursor.description` do
pymssql NÃO expõe o tipo nativo da coluna: expõe só uma de 5 categorias
DB-API genéricas (NUMBER, DECIMAL, DATETIME, STRING, e um fallback BINARY
pra tudo mais), e a posição de precisão/escala do DB-API (`description[i]
.precision`/`.scale`) vem sempre `None` (o próprio código do driver as
preenche com `None` fixo, não são lidas do servidor). Pior: BIT cai no
mesmo bucket NUMBER que INT/BIGINT (indistinguível), e DATE/DATETIME2/
DATETIMEOFFSET (tipos mais novos do SQL Server, que o driver DB-Library
subjacente do FreeTDS não mapeia no `get_api_coltype`) caem todos no
fallback BINARY: o oposto do que precisamos (queremos DATE/TIMESTAMP/
TIMESTAMPTZ nativos, não BLOB). Isso é bem diferente do Postgres
(cursor.description expõe o OID exato do tipo, granular) e do MySQL
(expõe o FIELD_TYPE exato via protocolo, granular).

Por isso, diferente de postgres_to_duckdb.py/mysql_to_duckdb.py, esta
extração NÃO usa `cursor.description` pra resolver o tipo DuckDB. Em vez
disso, consulta `INFORMATION_SCHEMA.COLUMNS` (metadado do catálogo do
próprio SQL Server, disponível em qualquer versão suportada, incluindo
Managed Instance/Azure SQL) pra pegar `DATA_TYPE`/`NUMERIC_PRECISION`/
`NUMERIC_SCALE` exatos de cada coluna, casados por nome com as colunas que
`cursor.description` devolve (esse continua confiável pra NOME e ORDEM das
colunas, só não pro tipo). Column ausente no INFORMATION_SCHEMA (não deveria
acontecer pra uma tabela normal, mas ex.: coluna computada sem tipo
resolvível) cai em VARCHAR, mesmo fallback seguro das outras fontes.

Timing do `cursor.description` do pymssql: verificado tanto no código-fonte
(`Cursor.execute()` chama `get_header()` imediatamente após
`execute_query()`, antes de qualquer fetch) quanto empiricamente contra um
SQL Server real nesta sandbox: fica disponível logo após `execute()`, sem
precisar buscar linha nenhuma antes (igual Trino/MySQL, diferente do cursor
nomeado do psycopg2/Postgres).

Streaming: `cursor.fetchmany()` do pymssql busca linha a linha do socket via
DB-Library (`get_iterator`/`dbnextrow` por baixo), sem pré-carregar o result
set inteiro no cliente: confirmado no código-fonte (`_pymssql.pyx`,
`Cursor.fetchmany`chama `getrow()` em loop, sem um `fetchall` disfarçado por
trás). Streaming real, mesmo sem um cursor nomeado/SSCursor explícito (o
pymssql não expõe esse conceito: não precisa, o DB-Library já é
incrementalmente lido do socket por natureza).

Carga de cada lote via DataFrame pandas pequeno + "INSERT ... SELECT * FROM
df" (DuckDB lê o DataFrame local direto), igual às outras fontes. Os valores
continuam sendo convertidos pra string em Python antes de entrar no
DataFrame: é o `INSERT ... SELECT` que faz o CAST implícito de VARCHAR pro
tipo nativo da coluna de destino (mesmo mecanismo testado em
postgres_to_duckdb.py).
"""

import duckdb
import pandas as pd
from loguru import logger

TAMANHO_LOTE_PADRAO = 10_000

_DECIMAL_PRECISAO_MAXIMA_DUCKDB = 38

# Mapeamento de tipo SQL Server (`INFORMATION_SCHEMA.COLUMNS.DATA_TYPE`,
# sempre em minúsculo nessa view) -> tipo DuckDB. Conservador: qualquer
# DATA_TYPE fora daqui cai em VARCHAR (ver `_tipo_duckdb_sqlserver`): nunca
# arriscamos um CAST agressivo que possa quebrar o INSERT (ex.:
# UNIQUEIDENTIFIER, XML, GEOGRAPHY, GEOMETRY, VARBINARY, IMAGE, SQL_VARIANT,
# HIERARCHYID caem todos aqui).
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
    # money/smallmoney são decimais de precisão/escala fixas e conhecidas no
    # próprio padrão T-SQL (money = decimal(19,4), smallmoney = decimal(10,4))
    #: não estavam na lista pedida originalmente, mas mapear pra DECIMAL em
    # vez de VARCHAR é uma extensão segura e na mesma linha de raciocínio das
    # colunas monetárias já tratadas nas outras fontes (ganha compressão e
    # exatidão, sem risco de CAST quebrar).
    "money": "DECIMAL(19,4)",
    "smallmoney": "DECIMAL(10,4)",
}

_SQLSERVER_TIPOS_DECIMAL = ("decimal", "numeric")


def _tipo_duckdb_sqlserver(data_type: str, precision, scale) -> str:
    """
    Resolve o tipo DuckDB a partir de uma linha de `INFORMATION_SCHEMA.COLUMNS`
    (`DATA_TYPE`, `NUMERIC_PRECISION`, `NUMERIC_SCALE`).

    DECIMAL/NUMERIC é tratado à parte, igual às outras fontes: usa
    `DECIMAL(precisao,escala)` quando o SQL Server informa precisão/escala
    exatas (preserva o valor exato, sem arredondamento de DOUBLE: relevante
    pra colunas monetárias), caindo pra DOUBLE só quando a precisão não vem
    ou excede o limite do DuckDB (38).
    """
    data_type = (data_type or "").strip().lower()
    if data_type in _SQLSERVER_TIPOS_DECIMAL:
        if precision is not None and scale is not None and precision <= _DECIMAL_PRECISAO_MAXIMA_DUCKDB:
            return f"DECIMAL({precision},{scale})"
        return "DOUBLE"
    return _SQLSERVER_TIPO_PARA_DUCKDB.get(data_type, "VARCHAR")


def _metadado_colunas_information_schema(conn, schema: str, tabela: str) -> dict:
    """
    Busca DATA_TYPE/NUMERIC_PRECISION/NUMERIC_SCALE de cada coluna de
    `schema.tabela` via `INFORMATION_SCHEMA.COLUMNS`: a fonte de tipo real
    usada por essa extração (ver docstring do módulo pra motivo: o
    `cursor.description` do pymssql é granular demais só na aparência, mas
    devolve categorias DB-API genéricas que perdem BIT/DATE/DATETIME2/
    DATETIMEOFFSET e nunca tem precisão/escala).

    Returns:
        dict {nome_coluna: (data_type, precision, scale)}, chaves em
        lower-case (SQL Server é case-insensitive por padrão de collation,
        então casar por nome em lower-case é seguro e evita depender da
        collation exata da instância).
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
    nome num arquivo DuckDB local, lendo em lotes de `tamanho_lote` linhas
    via `cursor.fetchmany` (pymssql lê linha a linha do socket via
    DB-Library/FreeTDS por baixo: streaming real, sem pré-carregar o result
    set inteiro no cliente; ver docstring do módulo).

    Espelha a assinatura/comportamento de
    `postgres_to_duckdb.extrair_para_duckdb_postgres`, trocando `catalogo`
    (Trino) por nada: em SQL Server o "catálogo" ANSI é o próprio banco de
    dados já escolhido na conexão (`database=`), então só `schema.tabela` é
    necessário aqui (mesmo raciocínio do Postgres/MySQL).

    Returns:
        (qtd_linhas_extraidas, nomes_das_colunas)
    """
    metadado_colunas = _metadado_colunas_information_schema(conn, schema, tabela)

    cur = conn.cursor()
    query = f"SELECT {'TOP ' + str(limite) + ' ' if limite is not None else ''}* FROM [{schema}].[{tabela}]"
    cur.execute(query)

    # pymssql popula `.description` logo após `execute()` (confirmado no
    # código-fonte e empiricamente contra um SQL Server real, ver docstring
    # do módulo): diferente do cursor nomeado do psycopg2/Postgres, que só
    # populava depois do primeiro fetch.
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
