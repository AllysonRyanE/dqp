"""
Descoberta automática de tabelas por fonte: usada só quando
`persistir_datamart_duckdb.py` roda em modo "base inteira" (TABLES=TODAS, ver
docstring desse módulo pro sentinela). Esta camada só descobre NOMES de
tabela existentes num schema/diretório; a extração/profiling em si continua
inteiramente a cargo dos módulos `extraction/*_to_duckdb.py` já existentes,
chamados um de cada vez no loop principal: nada aqui extrai dado nenhum.

Postgres/MySQL/SQL Server: os três suportam `information_schema.tables`
(padrão ANSI) com a MESMA query: testado empiricamente contra os três nesta
tarefa (containers Docker de teste), não só assumido por um funcionar.

Oracle: não expõe `information_schema` por padrão: usa `ALL_TABLES`
(catálogo proprietário Oracle). Também tem a particularidade já conhecida
deste projeto (documentada em `extraction/oracle_to_duckdb.py`): Oracle
dobra identificador não citado pra MAIÚSCULO, então o "schema" (owner)
comparado em `ALL_TABLES.OWNER` precisa estar em maiúsculo: ver
`listar_tabelas_oracle`.

Trino: também expõe `information_schema.tables` (parte do padrão ANSI que o
conector do Trino implementa): implementado por ANALOGIA ao mesmo padrão
que funcionou nos três bancos acima, mas **não validado ponta a ponta contra
um Trino real** nesta sandbox (sem rede pro BigDataPE a partir daqui). Ver
aviso de honestidade também na docstring de `listar_tabelas_trino` e no
relatório final da tarefa que introduziu este módulo.

Parquet: não tem "schema" no sentido de banco: "base inteira" aqui vira um
DIRETÓRIO com múltiplos arquivos `.parquet`, cada um sendo uma tabela
distinta (diferente do glob particionado que `extrair_para_duckdb_parquet` já
suporta, onde vários arquivos formam UMA tabela só). `listar_arquivos_parquet`
enumera os arquivos `.parquet` diretamente dentro do diretório, SEM descer
recursivamente em subpastas: decisão deliberada: descer recursivamente
misturaria com o caso de particionamento Hive já suportado (ex.
`dados/ano=2024/*.parquet` são pedaços de UMA tabela, não tabelas distintas),
criando ambiguidade real sobre se uma subpasta é "mais uma tabela" ou "mais
um pedaço da tabela de cima". Quem precisar de descoberta recursiva/agrupada
por partição pode compor isso manualmente listando os diretórios e chamando
`persistir_datamart_duckdb.py` uma vez por subpasta com TABLES explícito.
"""

import os
from typing import List

_QUERY_INFORMATION_SCHEMA = (
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_schema = {marcador} AND table_type = 'BASE TABLE' "
    "ORDER BY table_name"
)


def listar_tabelas_postgres(conn, schema: str) -> List[str]:
    """Lista tabelas base (exclui views) do schema Postgres informado, via
    `information_schema.tables`. `conn` é a MESMA conexão psycopg2 já aberta
    pra extração (get_postgres_source_connection); a listagem roda numa
    transação própria, fechada com `conn.commit()` no final pra não deixar
    uma transação implícita pendurada antes do cursor nomeado da extração."""
    cur = conn.cursor()
    cur.execute(_QUERY_INFORMATION_SCHEMA.format(marcador="%s"), (schema,))
    nomes = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.commit()
    return nomes


def listar_tabelas_mysql(conn, schema: str) -> List[str]:
    """Lista tabelas base do "schema" MySQL informado (schema == database em
    MySQL, sem separação entre os dois conceitos) via `information_schema.tables`,
    mesma query do Postgres, confirmado empiricamente que funciona igual."""
    cur = conn.cursor()
    cur.execute(_QUERY_INFORMATION_SCHEMA.format(marcador="%s"), (schema,))
    nomes = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.commit()
    return nomes


def listar_tabelas_sqlserver(conn, schema: str) -> List[str]:
    """Lista tabelas base do schema SQL Server informado (ex. "dbo") via
    `information_schema.tables`: mesma query do Postgres/MySQL, confirmado
    empiricamente que funciona igual (pymssql aceita o mesmo placeholder
    pyformat `%s` que psycopg2/pymysql)."""
    cur = conn.cursor()
    cur.execute(_QUERY_INFORMATION_SCHEMA.format(marcador="%s"), (schema,))
    nomes = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.commit()
    return nomes


def listar_tabelas_oracle(conn, schema: str) -> List[str]:
    """Lista tabelas do schema (owner) Oracle informado via `ALL_TABLES`
    (Oracle não expõe `information_schema` por padrão, diferente de
    Postgres/MySQL/SQL Server). `schema` é colocado em MAIÚSCULO antes de
    comparar com `ALL_TABLES.OWNER`: Oracle dobra identificador não citado
    pra maiúsculo ao criar objetos, então um owner gravado como "dqp_user" no
    .env não bate com nada se comparado literalmente contra o catálogo (que
    guarda "DQP_USER"); mesma particularidade já documentada em
    `extraction/oracle_to_duckdb.py` pras tabelas em si. oracledb usa
    paramstyle 'named' (`:schema`), diferente do `%s` usado pelos outros
    três drivers."""
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name FROM all_tables WHERE owner = :schema ORDER BY table_name",
        {"schema": schema.upper()},
    )
    nomes = [r[0] for r in cur.fetchall()]
    cur.close()
    return nomes


def listar_tabelas_trino(conn, schema: str) -> List[str]:
    """Lista tabelas do schema Trino informado via `information_schema.tables`
    (roda dentro do catálogo já selecionado na conexão, ver `get_trino_connection`).

    AVISO DE HONESTIDADE: implementado por analogia ao mesmo padrão ANSI que
    foi de fato testado contra Postgres/MySQL/SQL Server nesta tarefa: o
    Trino documenta suporte a `information_schema.tables`, mas isso NÃO foi
    validado ponta a ponta contra um Trino real nesta sandbox (sem rede pro
    BigDataPE a partir daqui). `trino.dbapi.paramstyle` é 'qmark' (`?`),
    diferente do `%s`/`:nome` dos outros drivers: confirmado via
    inspeção do client, não por execução real."""
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
