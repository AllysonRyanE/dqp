"""
Extração de um Oracle arbitrário (a FONTE de dados a perfilar: não confundir
com o Postgres do data mart, que é DESTINO dos resultados) pra um arquivo
DuckDB local, em lotes: mesma lógica de `extraction/postgres_to_duckdb.py`,
só trocando o client de conexão (python-oracledb em vez de psycopg2). Nunca
materializa o resultado inteiro em memória Python de uma vez: é o mesmo
argumento central do TCC sobre volume, agora valendo também pra fontes
Oracle.

Driver: `python-oracledb` (sucessor oficial do `cx_Oracle`, mantido pela
própria Oracle), usado em modo THIN: sem precisar do Oracle Instant Client
instalado no sistema operacional, mesmo benefício de instalação limpa que
motivou a escolha do PyMySQL em vez do mysqlclient. Confirmado empiricamente
(`oracledb.is_thin_mode()` retorna True logo após `oracledb.connect(...)`,
sem nenhuma chamada a `oracledb.init_oracle_client()`) que o modo thin
funciona pra essa conexão, contra um Oracle XE 18c/21c local via Docker
(`gvenzl/oracle-xe`): ver seção de validação no relatório final da tarefa
pra detalhes exatos de versão testada.

Timing de `cursor.description`: confirmado empiricamente (não assumido a
partir de Trino/MySQL/Postgres) que o python-oracledb já popula
`cursor.description` logo após `execute()`, antes de qualquer `fetch`.
Mesmo comportamento do Trino/PyMySQL, diferente do cursor nomeado do
psycopg2 (que só popula depois do primeiro fetch). Por isso a tabela DuckDB
é criada antes do loop de fetch, igual ao MySQL/Trino, sem o truque de
"buscar o primeiro lote antes" que o Postgres precisa.

A tabela criada no DuckDB usa o tipo NATIVO da coluna, lido do metadado do
Oracle (`cursor.description[i].type_code`, um objeto `oracledb.DbType`
comparável/hasheável: confirmado empiricamente que funciona como chave de
dict), em vez de forçar tudo pra VARCHAR. Mesma ideia O(1) por coluna dos
outros extratores.

## Particularidades do sistema de tipos do Oracle (genuinamente diferentes
## de Postgres/MySQL: não é copy-paste do mapeamento de lá)

1. NUMBER único tipo numérico: Oracle não separa INTEGER de DECIMAL: tudo
   numérico declarado é `NUMBER(precisao, escala)`. A heurística usada aqui
   (`_tipo_duckdb_oracle_number`):
     - `NUMBER` SEM precisão/escala declarada (`NUMBER` puro, tamanho
       arbitrário): o driver expõe isso com o par sentinela
       `precision=0, scale=-127` (confirmado empiricamente com
       python-oracledb 26.0.0 contra Oracle 21c XE: não documentado
       oficialmente com todas as letras, mas reproduzível e é o mesmo
       sentinel usado por cx_Oracle historicamente). Nesse caso cai pra
       `DOUBLE`: não há teto de precisão razoável pra decidir um DECIMAL(p,s)
       com segurança (o valor pode ter mais de 38 dígitos, teto do
       DECIMAL do DuckDB), e forçar um teto arbitrário arriscaria erro de
       overflow no INSERT pra valores legítimos maiores.
     - `NUMBER(p, s)` com `s <= 0` (escala zero ou negativa: Oracle permite
       escala negativa, ex. `NUMBER(10,-2)`, incomum mas válido) e `p <= 18`:
       `BIGINT` (cabe com folga em int64, evita o overhead de DECIMAL pra
       coluna claramente inteira).
     - `NUMBER(p, s)` com `s <= 0` e `p` entre 19 e 38 (o teto do DECIMAL do
       DuckDB): `DECIMAL(p, 0)`: inteiro grande demais pra BIGINT, mas
       ainda exato.
     - `NUMBER(p, s)` com `s > 0`: `DECIMAL(p, s)` preservando a
       precisão/escala exatas declaradas (mesmo raciocínio do NUMERIC do
       Postgres/DECIMAL do MySQL: evita erro de arredondamento de ponto
       flutuante em colunas monetárias).
     - Qualquer `p > 38` (não deveria acontecer pra NUMBER declarado: o
       teto do próprio Oracle é 38 dígitos de precisão; só apareceria via
       o sentinel acima, já tratado): fallback `DOUBLE`.

2. `DATE` do Oracle TEM componente de hora (diferente do `DATE` ANSI do
   Postgres/MySQL, que é só data): confirmado empiricamente: uma coluna
   Oracle `DATE` populada com `TO_DATE('2024-03-15 14:30:00', ...)` volta do
   driver como `datetime.datetime(2024, 3, 15, 14, 30, 0)`, hora incluída.
   Mapear isso pra `DATE` do DuckDB TRUNCARIA a hora silenciosamente na
   conversão implícita do INSERT (perda de dado, bug sutil e fácil de não
   perceber num teste superficial só com meia-noite). Por isso `DATE` do
   Oracle mapeia pra `TIMESTAMP` no DuckDB aqui, nunca pra `DATE`: ver
   `_ORACLE_DBTYPE_PARA_DUCKDB[oracledb.DB_TYPE_DATE]` abaixo. Validado
   ponta a ponta (ver relatório da tarefa): valor de teste com hora
   diferente de meia-noite (14:30:00) sobrevive intacto na tabela DuckDB
   resultante.

3. `TIMESTAMP` -> `TIMESTAMP`; `TIMESTAMP WITH TIME ZONE` e
   `TIMESTAMP WITH LOCAL TIME ZONE` -> `TIMESTAMPTZ` (ambos os DbTypes
   distintos do driver, `DB_TYPE_TIMESTAMP_TZ` e `DB_TYPE_TIMESTAMP_LTZ`,
   mapeiam pro mesmo tipo DuckDB: a distinção entre os dois é sobre COMO o
   Oracle normaliza/exibe o fuso horário internamente, não muda a
   necessidade de um tipo "com fuso" no destino).

4. `VARCHAR2`/`NVARCHAR2`/`CHAR`/`NCHAR` -> `VARCHAR`.

5. `CLOB`/`NCLOB` -> fallback `VARCHAR`. Funciona (o driver expõe um valor
   que vira string via `str(v)`, testado), mas tem custo de leitura maior
   que um VARCHAR2 comum (LOB é buscado sob demanda do servidor, mesmo
   dentro do mesmo fetch): aceitável pro volume de teste desta tarefa, mas
   vale nota pra quem for perfilar tabelas com CLOBs grandes/numerosos em
   produção.

6. `BLOB`/RAW/binários -> fallback pra `VARCHAR` na DDL (mesmo "não força
   cast exótico" dos outros extratores), mas com uma ressalva importante:
   dado binário não tem uma representação textual óbvia, e `str(v)` num
   objeto BLOB do driver **lança `TypeError`** (confirmado empiricamente:
   `LOB.__str__` de um BLOB tenta decodificar como texto e falha): differente
   do CLOB, onde `str(v)` funciona liso. Decisão: quando o valor é um LOB
   binário, convertê-lo pra uma string hexadecimal (`v.read().hex()`) em vez
   de deixar o `str(v)` genérico estourar a extração inteira. Isso preserva
   presença/nulidade/tamanho relativo do dado (útil pro profiler, ainda que
   sem sentido semântico nenhum sobre o conteúdo binário em si) sem arriscar
   um CAST exótico nem quebrar o pipeline. Documentado aqui e no código de
   `_valor_para_str_oracle` abaixo.

7. Qualquer tipo Oracle sem mapeamento explícito (ROWID, tipos BINARY_FLOAT/
   BINARY_DOUBLE de precisão simples/dupla nativa, LONG, tipos de coleção
   etc.) cai em `VARCHAR`: mesmo comportamento conservador dos outros
   extratores: nunca um CAST agressivo que possa quebrar o INSERT.

Carga de cada lote via DataFrame pandas pequeno + "INSERT ... SELECT * FROM
df" (DuckDB lê o DataFrame local direto), igual aos outros caminhos. Os
valores continuam sendo convertidos pra string em Python antes de entrar no
DataFrame: é o `INSERT ... SELECT` que faz o CAST implícito de VARCHAR pro
tipo nativo da coluna de destino (mesmo mecanismo testado nos outros
extratores, incluindo TIMESTAMPTZ/DECIMAL/BIGINT/DOUBLE).
"""

import oracledb
import duckdb
import pandas as pd
from loguru import logger

TAMANHO_LOTE_PADRAO = 10_000

_DECIMAL_PRECISAO_MAXIMA_DUCKDB = 38
_NUMBER_PRECISAO_MAXIMA_BIGINT = 18  # int64 com folga (max ~9.2e18, 19 dígitos)

# Mapeamento de tipo Oracle (objeto `oracledb.DbType`, exposto em
# `cursor.description[i].type_code`) -> tipo DuckDB. NUMBER é tratado à
# parte (ver `_tipo_duckdb_oracle_number`) por precisar de precisão/escala.
# Conservador: qualquer DbType fora daqui cai em VARCHAR
# (ver `_tipo_duckdb_oracle`).
_ORACLE_DBTYPE_PARA_DUCKDB = {
    oracledb.DB_TYPE_BINARY_FLOAT: "DOUBLE",
    oracledb.DB_TYPE_BINARY_DOUBLE: "DOUBLE",
    # Oracle DATE inclui hora (ver nota 2 no docstring do módulo): mapear
    # pra TIMESTAMP no DuckDB, NUNCA pra DATE (truncaria a hora em silêncio).
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
    """
    Heurística de NUMBER -> tipo DuckDB (ver nota 1 no docstring do módulo).

    O par sentinela `precision=0, scale=-127` identifica um `NUMBER` puro,
    sem precisão/escala declarada (tamanho arbitrário): confirmado
    empiricamente com python-oracledb 26.0.0 contra Oracle 21c XE.
    """
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
    """
    Resolve o tipo DuckDB pra uma coluna a partir do `cursor.description` do
    python-oracledb (tupla de 7 posições, padrão DB-API: name, type_code,
    display_size, internal_size, precision, scale, null_ok: `type_code` é
    um objeto `oracledb.DbType`, não um int cru como no MySQL).
    """
    type_code, precision, scale = col[1], col[4], col[5]
    if type_code == oracledb.DB_TYPE_NUMBER:
        return _tipo_duckdb_oracle_number(precision, scale)
    return _ORACLE_DBTYPE_PARA_DUCKDB.get(type_code, "VARCHAR")


def _valor_para_str_oracle(v) -> str:
    """
    Conversão valor -> string antes de entrar no DataFrame do lote. Igual
    aos outros extratores (`str(v)`) pro caso comum, com uma exceção: um
    LOB binário (BLOB) do python-oracledb lança `TypeError` em `str(v)`
    (confirmado empiricamente: `LOB.__str__` tenta decodificar como texto
    e falha pra conteúdo binário). Um LOB de texto (CLOB/NCLOB) funciona
    normalmente com `str(v)` (testado, devolve o texto). Ver nota 6 no
    docstring do módulo pra justificativa de usar hex em vez de pular a
    coluna ou arriscar outro cast.
    """
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
    num arquivo DuckDB local, lendo em lotes de `tamanho_lote` linhas via
    `cursor.fetchmany`: o cursor padrão do python-oracledb já busca do
    servidor sob demanda (não existe aqui o problema do cursor client-side
    "baixa tudo de uma vez" que Postgres/MySQL têm sem cursor nomeado/
    SSCursor; confirmado que o `arraysize` padrão do driver, usado
    internamente por `fetchmany`, controla isso sem configuração extra).

    Espelha a assinatura/comportamento de
    `postgres_to_duckdb.extrair_para_duckdb_postgres`, trocando "schema" pra
    o dono do objeto (`schema.tabela` = `OWNER.TABLE_NAME` em termos
    Oracle).

    NOTA sobre maiúsculas (particularidade encontrada na validação real, não
    prevista de antemão): Oracle dobra identificador NÃO citado pra
    MAIÚSCULO por padrão ao criar objetos (oposto do Postgres, que dobra pra
    minúsculo): uma tabela criada com `CREATE TABLE tipos_teste (...)` fica
    de fato armazenada como `TIPOS_TESTE` no catálogo. Como este módulo
    monta a query com identificador CITADO (`"schema"."tabela"`, pra permitir
    nomes com caracteres especiais), citar `schema`/`tabela` exatamente como
    vieram (tipicamente minúsculo, mesma convenção de `TABLES=` no .env
    usada pelas outras fontes) faria o Oracle procurar um objeto minúsculo
    que geralmente não existe, e falhar com `ORA-00942` (reproduzido durante
    a validação desta tarefa). Por isso `schema`/`tabela` são colocados em
    maiúsculo só pra montar a query Oracle; o nome da tabela no DuckDB
    continua usando o `tabela` ORIGINAL (o `table_name` que o resto da
    pipeline espera, ex. pra casar com `dim_base`/`TABLES=`), não o
    maiusculizado.

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

    # Confirmado empiricamente: python-oracledb já popula `.description`
    # logo após `execute()`, antes de qualquer fetch (ver nota no topo do
    # módulo): igual MySQL/Trino, diferente do cursor nomeado do psycopg2.
    colunas = [d[0] for d in cur.description]
    tipos_duckdb = [_tipo_duckdb_oracle(d) for d in cur.description]

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
