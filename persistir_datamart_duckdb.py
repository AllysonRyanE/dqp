"""
Mesma orquestração de persistir_datamart.py, mas usando o DuckDBProfiler
(via extração em lotes Trino/Postgres -> DuckDB local) em vez do ydata-profiling.
Persiste na MESMA base (mesmo dim_base): dim_execucao guarda qual
`ferramenta` gerou cada execução, então dá pra comparar lado a lado sem
misturar os números.

A extração é configurada em DOIS lugares diferentes, de propósito:

  - FONTE=<qualquer nome> (variável de ambiente, obrigatória, sem default,
    .env ou linha de comando) escolhe QUAL fonte usar nesta execução, ex.
    "postgres_bigdata", "mysql_teste", "oracle_producao". É só um nome.

  - fontes.ini (arquivo próprio, ao lado deste script: ver
    fontes.ini.example) é onde cada fonte de fato mora: uma SEÇÃO por
    FONTE, contendo o campo `tipo` (trino, postgres, mysql, sqlserver,
    oracle ou parquet: decide qual conector roda) e os campos de conexão
    daquela fonte especificamente (host, port, user, password, database/
    catalog/service_name, schema, ou path pra parquet).

Por que um arquivo à parte, com seções, em vez de mais variáveis no .env:
um .env é só uma lista chata de chave=valor, então duas fontes do MESMO
tipo (ex. dois "postgres") disputariam a MESMA chave (ex.
PG_SOURCE_HOST): só a última escrita no arquivo sobreviveria, mesmo que
as duas tivessem seus próprios "FONTE=" por perto. Uma seção do fontes.ini
isola completamente uma fonte da outra, então os MESMOS nomes de campo
(host, port, user...) podem se repetir em quantas seções quiser, cada uma
com seu próprio valor, sem nenhum conflito: permitindo várias fontes do
mesmo tipo convivendo no mesmo arquivo. O `.env` continua existindo, só
com o que é comum a todas as execuções: TABLES e PG_DATAMART_* (destino
dos resultados, nunca confundir com a fonte).

MODO "BASE INTEIRA" (perfilar TODAS as tabelas de um schema/diretório, sem
listar uma por uma): defina TABLES=TODAS (env ou argv, mesmo lugar de sempre). É uma
sentinela case-insensitive (ver `MODO_TODAS_TABELAS` abaixo). Quando
ativado, a lista de tabelas não vem mais de TABLES, e sim é DESCOBERTA em
tempo de execução via `src/extraction/table_discovery.py`, usando o schema
(ou diretório, pra parquet) configurado na seção da FONTE em uso dentro de
fontes.ini:
  - Postgres/MySQL/SQL Server: `information_schema.tables` do schema.
  - Oracle: `ALL_TABLES` do owner (uppercased antes de comparar: ver
    docstring de `listar_tabelas_oracle`).
  - Trino: `information_schema.tables` do schema: por analogia, não
    validado ponta a ponta contra Trino real (ver docstring de
    `listar_tabelas_trino`).
  - Parquet: enumera arquivos `.parquet` diretamente dentro do diretório
    (não recursivo: ver docstring de `listar_arquivos_parquet`); cada
    arquivo é uma tabela distinta.
Cada tabela descoberta é processada isoladamente (try/except por tabela): uma
tabela falhar (permissão, tipo exótico, tabela sem colunas etc.) fica
registrada no log e NÃO interrompe as demais: resumo final impresso ao fim
do loop com quantas tiveram sucesso/falha (ver `main()`).

TIPO=oracle: faixa homologada: testado de ponta a ponta (extração +
profiling + persistência no data mart real) contra Oracle 18c XE e 21c XE
via Docker (`gvenzl/oracle-xe:18-slim` e `:21-slim`). "Deveria funcionar"
também em outras versões (12c+/19c/23c) por compatibilidade de protocolo
(mesmo driver `python-oracledb` em modo thin), mas isso NÃO foi verificado
empiricamente: só as duas versões citadas acima foram de fato rodadas. Ver
`src/extraction/oracle_to_duckdb.py` pras particularidades de tipo (NUMBER
sem INTEGER/DECIMAL separados, DATE com componente de hora, CLOB/BLOB).

TIPO=parquet é diferente das outras três: não tem conexão de banco nenhuma
(sem cursor, sem credenciais): só um caminho de arquivo/diretório local,
configurado no campo `path` da seção da fonte em fontes.ini. Por isso não
existe uma `origem_conn` de verdade pra essa fonte (ver `origem_conn = None`
no switch abaixo, e o `if origem_conn:` antes do `.close()` no fim do
arquivo).

TIPO=parquet com o campo `path` apontando pra um DIRETÓRIO tem DOIS
comportamentos totalmente diferentes, que dependem só de TABLES=TODAS estar
ou não definido: fácil de confundir os dois, leia com atenção:
  - Modo TABELA ÚNICA (padrão, sem TABLES=TODAS): o diretório é tratado como
    UMA base unificada: todos os `.parquet` soltos diretamente dentro dele
    (não recursivo, pra não colidir com particionamento Hive em subpastas)
    são mesclados numa ÚNICA tabela/execução, com o total de linhas somado.
    Detecção automática em `_resolver_caminho_parquet_unico`: monta o glob
    "<diretorio>/*.parquet" sozinho, sem o usuário precisar digitar glob
    nenhum: motivado por data lakes reais serem, com frequência, N arquivos
    parquet de mesmo schema numa pasta (exportações em lote).
  - Modo BASE INTEIRA (TABLES=TODAS, ver abaixo): o MESMO diretório vira um
    catálogo de tabelas DISTINTAS: cada `.parquet` dentro dele é UMA tabela
    separada, com perfilamento/execução própria (via `listar_arquivos_parquet`).
Ou seja: mesmo diretório, resultado oposto (1 base vs. N tabelas), decidido
inteiramente por TABLES=TODAS estar presente ou não.

Uso: FONTE=<nome> .venv/bin/python persistir_datamart_duckdb.py [tabela1,tabela2,...]
FONTE é sempre obrigatória (.env ou linha de comando, sem valor padrão) e
precisa ter uma seção [<nome>] correspondente em fontes.ini. Sem o
argumento de tabelas, roda pra todas as tabelas listadas em TABLES (.env ou
linha de comando) ou, na ausência disso, no campo "tables" da seção da
FONTE em fontes.ini. Se nem isso existir (TIPO != parquet, que deriva a
tabela do próprio `path`), cai automaticamente no modo "base inteira"
(TABLES=TODAS): uma fonte sem `tables` configurado é tratada como "perfila
tudo", nunca como erro de configuração.
"""

import configparser
import os
import sys
import time

import duckdb
import oracledb
import psycopg2
import pymysql
import pymssql
import trino
from dotenv import load_dotenv
from loguru import logger

sys.path.insert(0, "src")

from data_mart.loader import DataMartLoader
from extraction.mysql_to_duckdb import extrair_para_duckdb_mysql
from extraction.oracle_to_duckdb import extrair_para_duckdb_oracle
from extraction.parquet_to_duckdb import extrair_para_duckdb_parquet
from extraction.postgres_to_duckdb import extrair_para_duckdb_postgres
from extraction.sqlserver_to_duckdb import extrair_para_duckdb_sqlserver
from extraction.table_discovery import (
    listar_arquivos_parquet,
    listar_tabelas_mysql,
    listar_tabelas_oracle,
    listar_tabelas_postgres,
    listar_tabelas_sqlserver,
    listar_tabelas_trino,
)
from extraction.trino_to_duckdb import extrair_para_duckdb
from profilers.duckdb_profiler import DuckDBProfiler
from quality.quality_scorer import QualityScorer
from rules.user_rules_engine import avaliar_regras

load_dotenv()

CAMINHO_FONTES_INI = "fontes.ini"


def _carregar_secao_fonte(fonte: str) -> dict:
    """Lê a seção [<fonte>] de fontes.ini. Cada fonte mora na sua própria
    seção, usando sempre os MESMOS nomes de campo (host, port, user,
    password, database/catalog/service_name, schema, path) não importa o
    tipo: é a seção em si que isola uma fonte da outra, então duas fontes
    do mesmo tipo (ex. duas seções "mysql_teste" e "mysql") convivem no
    mesmo arquivo sem nenhum conflito, cada uma com seus próprios valores.
    Ver docstring do módulo pro motivo de não usar só o .env pra isso."""
    if not os.path.exists(CAMINHO_FONTES_INI):
        raise ValueError(
            f"Arquivo '{CAMINHO_FONTES_INI}' não encontrado: copie "
            f"'{CAMINHO_FONTES_INI}.example' para '{CAMINHO_FONTES_INI}' e preencha "
            "as seções das fontes que for usar."
        )
    config = configparser.ConfigParser()
    config.read(CAMINHO_FONTES_INI)
    if fonte not in config:
        secoes = ", ".join(config.sections()) or "(nenhuma)"
        raise ValueError(
            f"FONTE='{fonte}' não tem seção correspondente em '{CAMINHO_FONTES_INI}' "
            f"(seções encontradas: {secoes}). Adicione uma seção [{fonte}] nesse "
            "arquivo, com pelo menos o campo 'tipo'."
        )
    secao = dict(config[fonte])
    if "tipo" not in secao:
        raise ValueError(f"Seção [{fonte}] em '{CAMINHO_FONTES_INI}' não tem o campo 'tipo'.")
    return secao


# Rótulo livre, escolhido por quem roda a pipeline: decide QUAL seção de
# fontes.ini é lida. Obrigatório, sem default (ver docstring do módulo).
if not os.environ.get("FONTE"):
    raise ValueError(
        "FONTE não definida: obrigatório especificar um nome pra identificar esta "
        "fonte (ex.: FONTE=postgres_bigdata), tanto no .env quanto na linha de comando."
    )
FONTE = os.environ["FONTE"].strip()

# A seção em si (host, port, user, password, ... conforme o tipo) e o TIPO
# (trino/postgres/mysql/sqlserver/oracle/parquet) vêm de dentro dela: ver
# `_carregar_secao_fonte` acima.
SECAO_FONTE = _carregar_secao_fonte(FONTE)
TIPO = SECAO_FONTE["tipo"].strip().lower()


def _segmento_diretorio_significativo(caminho: str) -> str:
    """
    Segmento de diretório "significativo" mais próximo de `caminho`
    (arquivo ou glob): olha só pro DIRETÓRIO que contém `caminho` (ignora o
    nome do arquivo/glob final) e caminha os segmentos da direita pra
    esquerda até achar um que não seja glob (`*`, `?`, `[`) nem partição
    Hive (`chave=valor`, ex. "ano=2024"): ex. "/dados/*.parquet" -> "dados";
    "/dados/particionado/ano=2024/*.parquet" -> "particionado". Usado tanto
    pra `catalogo_origem` (identifica de onde o dataset veio no filesystem)
    quanto, quando o nome do arquivo também é glob, como base do nome de
    tabela padrão (ver `_nome_tabela_padrao_parquet`).
    """
    diretorio = os.path.dirname(caminho.rstrip("/"))
    partes = [p for p in diretorio.split("/") if p and not any(c in p for c in "*?[") and "=" not in p]
    return partes[-1] if partes else (diretorio or caminho)


def _nome_tabela_padrao_parquet(caminho: str) -> str:
    """
    Deriva um nome de tabela a partir do campo `path` (fontes.ini) quando TABLES
    não foi especificado explicitamente (nem via argv, nem via .env),
    parquet não tem um catálogo de tabelas pra listar, então sem TABLES só
    sobra o próprio caminho pra nomear a única "tabela" que essa execução
    processa. Regra: nome do arquivo sem extensão (ex.
    "/dados/vendas.parquet" -> "vendas"). Quando o caminho é um glob sem
    nome de arquivo fixo (ex. "/dados/*.parquet" ou
    "/dados/ano=2024/*.parquet", o caso particionado), usa
    `_segmento_diretorio_significativo` (ex. "/dados/*.parquet" -> "dados").
    """
    caminho = caminho.rstrip("/")
    base = os.path.basename(caminho)
    if not base or any(c in base for c in "*?["):
        base = _segmento_diretorio_significativo(caminho) or "dados_parquet"
    nome, _ext = os.path.splitext(base)
    return nome or base


def _resolver_caminho_parquet_unico(caminho: str) -> str:
    """
    Resolve o campo `path` (fontes.ini) pro caminho/glob final passado pra
    `extrair_para_duckdb_parquet` no modo TABELA ÚNICA (fora de
    TABLES=TODAS: esse modo tem sua própria descoberta multi-tabela em
    `listar_arquivos_parquet`, não tocada por esta função). Três casos:

      1. Já é um arquivo `.parquet` específico (ex. "/dados/vendas.parquet"):
         devolve como está: comportamento de sempre, sem mudança.
      2. Já contém caractere de glob (`*`, `?`, `[`), ex. "/dados/*.parquet"
         ou "/dados/ano=2024/*.parquet" (particionamento Hive): devolve como
         está: o usuário já escreveu o glob manualmente, sabe o que está
         fazendo, não há nada a detectar.
      3. É um DIRETÓRIO comum existente, sem glob nenhum (ex. "/dados"):
         monta automaticamente o glob não recursivo "/dados/*.parquet",
         MESMA regra "não desce em subpastas" de `listar_arquivos_parquet`
         (evita colidir com particionamento Hive em subpastas, ex.
         "/dados/ano=2024/"). Motivação (pedido explícito do usuário):
         tantos data lakes reais são formados por N arquivos parquet de
         mesmo schema soltos numa pasta (múltiplas exportações em lote, sem
         necessariamente seguir convenção Hive): nesse caso o usuário só
         quer apontar pra pasta e perfilar tudo como SE FOSSE uma tabela só,
         sem precisar saber de antemão que precisaria digitar "/*.parquet"
         no final. Quem já quer esse resultado hoje escrevendo o glob à mão
         continua podendo (caso 2 acima é idêntico ao resultado deste caso).

    A mesclagem de fato continua sendo feita pelo DuckDB nativamente dentro
    de `read_parquet` (ver `extrair_para_duckdb_parquet`): esta função só
    decide QUE caminho/glob passar pra ele; não abre, lê nem valida arquivo
    nenhum. Se os arquivos dentro do diretório tiverem schema incompatível
    entre si, o erro do DuckDB continua propagando normalmente (mesma
    filosofia de sempre: sem union_by_name/CAST forçado pra "salvar" schema
    divergente).

    IMPORTANTE: não confundir com TABLES=TODAS: aqui (modo tabela única) um
    diretório vira UMA base unificada (N arquivos -> 1 tabela, linhas
    somadas). Em TABLES=TODAS, o MESMO diretório é tratado como catálogo de
    tabelas DISTINTAS (N arquivos -> N tabelas, perfilamentos separados),
    ver `listar_arquivos_parquet` e o bloco `if TIPO == "parquet" and
    MODO_TODAS_TABELAS` em `main()`. É fácil confundir os dois: a distinção
    inteira está em TABLES=TODAS estar ou não definido, não no caminho em si.
    """
    caminho_sem_barra = caminho.rstrip("/")
    if any(c in caminho for c in "*?["):
        return caminho  # já é um glob explícito: nada a fazer
    if os.path.isdir(caminho_sem_barra):
        return os.path.join(caminho_sem_barra, "*.parquet")
    return caminho  # arquivo específico (ou caminho inexistente: deixa o erro aparecer no read_parquet, não aqui)


# Lista de tabelas, em ordem de prioridade: argumento de linha de comando
# (tabela1,tabela2,...) > TABLES do .env/linha de comando (sobrescreve
# pontualmente, pra uma execução avulsa, sem mexer em arquivo nenhum) >
# campo "tables" da seção da FONTE em fontes.ini (o padrão de cada fonte,
# já que bases diferentes normalmente têm tabelas diferentes: por isso
# TABLES não mora no .env, que é compartilhado por todas as fontes).
# Sempre tabela inteira, sem amostragem: o DuckDB não tem o teto de RAM que
# o pandas/ydata tinham, testado com tabelas completas de 300k+ linhas.
#
# TIPO=parquet é o único caso em que tudo isso pode ficar ausente: sem
# conexão de banco nenhuma pra listar tabelas, o campo `path` já é o único
# arquivo/glob processado nessa execução, então o nome da tabela é derivado
# dele (ver `_nome_tabela_padrao_parquet`) quando não há TABLES em lugar
# nenhum. Pra qualquer outro tipo, a ausência total de TABLES (argv, .env e
# fontes.ini) cai no modo "base inteira" (TABLES=TODAS) por padrão, em vez de
# erro: uma fonte sem `tables` preenchido é tratada como "perfila tudo que
# encontrar", não como configuração inválida.
if len(sys.argv) > 1:
    fonte_tabelas = sys.argv[1]
elif os.environ.get("TABLES"):
    fonte_tabelas = os.environ["TABLES"]
elif SECAO_FONTE.get("tables"):
    fonte_tabelas = SECAO_FONTE["tables"]
elif TIPO == "parquet":
    fonte_tabelas = _nome_tabela_padrao_parquet(SECAO_FONTE["path"])
else:
    fonte_tabelas = "TODAS"
    print(
        f"FONTE='{FONTE}' não tem TABLES definido (nem argv, nem .env, nem o campo "
        "'tables' em fontes.ini), assumindo TABLES=TODAS: vai descobrir e perfilar "
        "todas as tabelas encontradas no schema/diretório configurado."
    )

# Sentinela do modo "base inteira" (item 3 do backlog): TABLES=TODAS (ou
# argv[1]="TODAS", case-insensitive) pula a lista explícita e descobre TODAS
# as tabelas do schema/diretório configurado pra fonte em uso, em tempo de
# execução dentro de main() (só ali existe conexão/caminho já resolvido pra
# listar): ver `table_discovery.py` e o bloco "MODO BASE INTEIRA" no
# docstring do módulo. "TODAS" (não "*" ou "ALL") pra ficar no mesmo idioma
# do resto da ferramenta (nomes de coluna, mensagens, etc. já são em
# português) e evitar colisão com glob de shell (`*` sem aspas se expande
# antes mesmo de chegar no Python, se alguém esquecer de citar o argumento).
MODO_TODAS_TABELAS = fonte_tabelas.strip().upper() == "TODAS"

if MODO_TODAS_TABELAS:
    nomes_tabelas = []  # preenchido dentro de main(), após a conexão/caminho da fonte estar pronto
else:
    nomes_tabelas = [n.strip() for n in fonte_tabelas.split(",") if n.strip()]
TABLES = {nome: None for nome in nomes_tabelas}

CAMINHO_DUCKDB = "/tmp/dqp_extrato.duckdb"


def get_trino_connection(secao):
    return trino.dbapi.connect(
        host=secao["host"],
        port=int(secao["port"]),
        user=secao["user"],
        catalog=secao["catalog"],
        http_scheme="https",
        auth=trino.auth.BasicAuthentication(secao["user"], secao["password"]),
    )


def get_postgres_source_connection(secao):
    """Conexão com o Postgres FONTE (dados a perfilar): campos lidos da
    seção da fonte em fontes.ini, distinto do Postgres do data mart
    (PG_DATAMART_*, no .env, destino)."""
    return psycopg2.connect(
        host=secao["host"],
        port=int(secao["port"]),
        user=secao["user"],
        password=secao["password"],
        dbname=secao["database"],
    )


def get_mysql_source_connection(secao):
    """Conexão com o MySQL FONTE (dados a perfilar): campos lidos da seção
    da fonte em fontes.ini, distinto do Postgres do data mart (PG_DATAMART_*,
    no .env, destino)."""
    return pymysql.connect(
        host=secao["host"],
        port=int(secao["port"]),
        user=secao["user"],
        password=secao["password"],
        database=secao["database"],
    )


def get_sqlserver_source_connection(secao):
    """Conexão com o SQL Server FONTE (dados a perfilar): campos lidos da
    seção da fonte em fontes.ini, distinto do Postgres do data mart
    (PG_DATAMART_*, no .env, destino). Usa pymssql (não pyodbc: ver
    docstring de extraction/sqlserver_to_duckdb.py pra motivo da escolha de
    driver)."""
    return pymssql.connect(
        server=secao["host"],
        port=int(secao["port"]),
        user=secao["user"],
        password=secao["password"],
        database=secao["database"],
    )


def get_oracle_source_connection(secao):
    """Conexão com o Oracle FONTE (dados a perfilar): campos lidos da seção
    da fonte em fontes.ini, distinto do Postgres do data mart (PG_DATAMART_*,
    no .env, destino). Usa python-oracledb em modo thin (default do driver,
    sem `oracledb.init_oracle_client()`): não exige Oracle Instant Client
    instalado no sistema, confirmado empiricamente contra Oracle 18c/21c XE
    (ver docstring de extraction/oracle_to_duckdb.py). `dsn` monta o
    `host:port/service_name` esperado pelo driver: Oracle usa "service
    name" (ou SID, legado) em vez do "database" simples do Postgres/MySQL;
    a imagem `gvenzl/oracle-xe` usada na validação expõe o pluggable
    database como `XEPDB1` por padrão."""
    dsn = f"{secao['host']}:{secao['port']}/{secao['service_name']}"
    return oracledb.connect(
        user=secao["user"],
        password=secao["password"],
        dsn=dsn,
    )


def datamart_connection_string() -> str:
    return (
        f"postgresql+psycopg2://{os.environ['PG_DATAMART_USER']}:{os.environ['PG_DATAMART_PASSWORD']}"
        f"@{os.environ['PG_DATAMART_HOST']}:{os.environ['PG_DATAMART_PORT']}/{os.environ['PG_DATAMART_DATABASE']}"
    )


def main():
    global TABLES  # reatribuído abaixo quando MODO_TODAS_TABELAS descobre a lista em tempo de execução

    # Conector de extração: TIPO decide a conexão e o bloco de variáveis lido;
    # FONTE (rótulo livre) vira sistema_origem, identificando esta fonte
    # especificamente no data mart: ver docstring do módulo.
    if TIPO == "postgres":
        origem_conn = get_postgres_source_connection(SECAO_FONTE)
        sistema_origem = FONTE
        catalogo_origem = SECAO_FONTE["database"]
        schema_origem = SECAO_FONTE["schema"]
    elif TIPO == "mysql":
        origem_conn = get_mysql_source_connection(SECAO_FONTE)
        sistema_origem = FONTE
        catalogo_origem = SECAO_FONTE["database"]
        schema_origem = SECAO_FONTE["database"]
    elif TIPO == "sqlserver":
        origem_conn = get_sqlserver_source_connection(SECAO_FONTE)
        sistema_origem = FONTE
        catalogo_origem = SECAO_FONTE["database"]
        schema_origem = SECAO_FONTE.get("schema", "dbo")
    elif TIPO == "oracle":
        origem_conn = get_oracle_source_connection(SECAO_FONTE)
        sistema_origem = FONTE
        catalogo_origem = SECAO_FONTE["service_name"]
        schema_origem = SECAO_FONTE["schema"]
    elif TIPO == "trino":
        origem_conn = get_trino_connection(SECAO_FONTE)
        sistema_origem = FONTE
        catalogo_origem = SECAO_FONTE["catalog"]
        schema_origem = SECAO_FONTE["schema"]
    elif TIPO == "parquet":
        # Sem conexão de banco nenhuma pra essa fonte: só um arquivo/glob
        # local (ver docstring do módulo). `catalogo_origem`/`schema_origem`
        # não têm um equivalente natural pra parquet (não existe "banco" nem
        # "schema" num arquivo solto), então usamos o segmento de diretório
        # "significativo" mais próximo do arquivo/glob (ver
        # `_segmento_diretorio_significativo`: pula segmentos glob tipo
        # "*" e partições Hive tipo "ano=2024", pra não gravar algo inútil
        # como catalogo="*" quando o campo `path` é um glob particionado)
        # como `catalogo_origem`. Identifica de onde o dataset veio no
        # filesystem, análogo ao papel do catálogo/database nas outras
        # fontes, mas curto: testado: o caminho absoluto inteiro estourou
        # `dim_base.catalogo`, que é `VARCHAR(100)` no data mart (caminhos de
        # diretório temporário/scratch passam de 100 caracteres fácil;
        # um único segmento de diretório não). `schema_origem` fica vazio:
        # não há um segundo nível de namespace pra representar. Truncado em
        # 100 chars como cinto de segurança pro mesmo limite.
        origem_conn = None
        sistema_origem = FONTE
        caminho_parquet_origem = SECAO_FONTE["path"]
        catalogo_origem = _segmento_diretorio_significativo(caminho_parquet_origem)[:100]
        schema_origem = ""
    else:
        raise ValueError(
            f"TIPO inválido: '{TIPO}' "
            "(valores aceitos: 'trino', 'postgres', 'mysql', 'sqlserver', 'oracle', 'parquet')"
        )

    # Modo "base inteira" (TABLES=TODAS): a lista de tabelas só pode ser
    # descoberta AQUI, depois que origem_conn/schema_origem (ou
    # caminho_parquet_origem, pra parquet) já estão resolvidos pelo switch
    # de TIPO acima: ver `table_discovery.py` pra implementação por fonte.
    if MODO_TODAS_TABELAS:
        if TIPO == "postgres":
            nomes_descobertos = listar_tabelas_postgres(origem_conn, schema_origem)
        elif TIPO == "mysql":
            nomes_descobertos = listar_tabelas_mysql(origem_conn, schema_origem)
        elif TIPO == "sqlserver":
            nomes_descobertos = listar_tabelas_sqlserver(origem_conn, schema_origem)
        elif TIPO == "oracle":
            nomes_descobertos = listar_tabelas_oracle(origem_conn, schema_origem)
        elif TIPO == "trino":
            nomes_descobertos = listar_tabelas_trino(origem_conn, schema_origem)
        else:  # TIPO == "parquet"
            nomes_descobertos = listar_arquivos_parquet(caminho_parquet_origem)

        if not nomes_descobertos:
            raise RuntimeError(
                f"TABLES=TODAS não encontrou nenhuma tabela pra FONTE={FONTE} (TIPO={TIPO}) "
                f"(schema/diretório: {schema_origem or caminho_parquet_origem!r}): "
                "confira se o schema/caminho configurado está correto e se o "
                "usuário de conexão tem permissão de leitura no catálogo."
            )
        print(
            f"TABLES=TODAS: {len(nomes_descobertos)} tabela(s) descoberta(s) em "
            f"{FONTE} ({TIPO})/{schema_origem or caminho_parquet_origem}: {', '.join(nomes_descobertos)}"
        )
        TABLES = {nome: None for nome in nomes_descobertos}

    scorer = QualityScorer()
    loader = DataMartLoader(datamart_connection_string())

    tabelas_ok = []
    tabelas_falhas = []  # lista de (nome, mensagem de erro): isolamento por tabela (ver bloco try/except abaixo)

    for table_name, limite in TABLES.items():
        print(f"\n{'=' * 80}\nTABELA: {table_name} (via DuckDB, fonte={FONTE}, tipo={TIPO})\n{'=' * 80}")
        # No modo "base inteira" (TABLES=TODAS) com TIPO=parquet,
        # caminho_parquet_origem é o DIRETÓRIO inteiro (ver listar_arquivos_parquet)
        #: cada tabela descoberta precisa apontar pro SEU PRÓPRIO arquivo
        # dentro dele, senão `read_parquet` acaba lendo o diretório inteiro
        # de novo a cada iteração (bug real encontrado na validação desta
        # tarefa: as 3 "tabelas" liam o glob do diretório inteiro e
        # colidiam por schema incompatível entre arquivos). Fora do modo
        # TODAS (modo tabela única), caminho_parquet_origem pode ser um
        # arquivo específico, um glob já escrito à mão, OU um diretório
        # comum: nesse último caso `_resolver_caminho_parquet_unico`
        # detecta e monta o glob não recursivo automaticamente, tratando
        # todos os .parquet soltos na pasta como UMA base só (ver docstring
        # da função: NÃO confundir com o modo TODAS logo acima: mesmo
        # diretório, comportamento oposto, dependendo só de TABLES=TODAS
        # estar ou não definido).
        if TIPO == "parquet" and MODO_TODAS_TABELAS:
            caminho_parquet_tabela = os.path.join(caminho_parquet_origem, f"{table_name}.parquet")
        elif TIPO == "parquet":
            caminho_parquet_tabela = _resolver_caminho_parquet_unico(caminho_parquet_origem)
        else:
            caminho_parquet_tabela = None
        try:
            _processar_uma_tabela(
                table_name,
                limite,
                origem_conn=origem_conn,
                schema_origem=schema_origem,
                catalogo_origem=catalogo_origem,
                caminho_parquet_origem=caminho_parquet_tabela,
                sistema_origem=sistema_origem,
                scorer=scorer,
                loader=loader,
            )
        except Exception as exc:  # isolamento: uma tabela ruim não pode abortar as demais
            logger.exception(f"Falha ao processar a tabela '{table_name}': seguindo pras próximas.")
            tabelas_falhas.append((table_name, f"{type(exc).__name__}: {exc}"))
            if origem_conn is not None:
                # `origem_conn` é REUTILIZADA pra todas as tabelas do loop,
                # se a falha veio de dentro de uma query no meio de uma
                # transação (ex. erro de permissão/tipo no meio do SELECT do
                # Postgres), a conexão pode ficar numa transação "abortada"
                # que rejeitaria qualquer comando seguinte até um ROLLBACK
                # explícito. Nem todo driver expõe rollback() com o mesmo
                # comportamento (psycopg2 sim; pymysql/pymssql/oracledb
                # tratam como no-op fora de transação; trino.dbapi pode nem
                # ter o método): daí o try/except amplo aqui, é só uma
                # rede de segurança pra não deixar UMA tabela ruim
                # contaminar a conexão compartilhada pras próximas.
                try:
                    origem_conn.rollback()
                except Exception:
                    pass
            continue
        else:
            tabelas_ok.append(table_name)

    print(f"\n{'=' * 80}\nRESUMO: {len(tabelas_ok)} tabela(s) processada(s) com sucesso, "
          f"{len(tabelas_falhas)} falharam (de {len(TABLES)} no total)\n{'=' * 80}")
    if tabelas_ok:
        print(f"OK: {', '.join(tabelas_ok)}")
    if tabelas_falhas:
        for nome, erro in tabelas_falhas:
            print(f"FALHA: {nome}: {erro}")

    if origem_conn is not None:  # TIPO=parquet não abre conexão nenhuma (ver switch acima)
        origem_conn.close()

    if tabelas_falhas:
        sys.exit(1)  # sinaliza falha parcial pro shell/CI, mesmo com o resumo já impresso


def _processar_uma_tabela(
    table_name,
    limite,
    *,
    origem_conn,
    schema_origem,
    catalogo_origem,
    caminho_parquet_origem,
    sistema_origem,
    scorer,
    loader,
):
    """Extração + profiling + scoring + persistência de UMA tabela: extraído
    de `main()` pra poder ser chamado dentro do try/except por tabela do
    modo "base inteira" sem duplicar o corpo do loop. Levanta a exceção
    original em caso de erro (quem decide o que fazer com isso é o chamador,
    em `main()`)."""
    inicio_extracao = time.time()
    if TIPO == "postgres":
        total_linhas, _colunas = extrair_para_duckdb_postgres(
            origem_conn,
            schema_origem,
            table_name,
            CAMINHO_DUCKDB,
            limite=limite,
        )
    elif TIPO == "mysql":
        total_linhas, _colunas = extrair_para_duckdb_mysql(
            origem_conn,
            schema_origem,
            table_name,
            CAMINHO_DUCKDB,
            limite=limite,
        )
    elif TIPO == "sqlserver":
        total_linhas, _colunas = extrair_para_duckdb_sqlserver(
            origem_conn,
            schema_origem,
            table_name,
            CAMINHO_DUCKDB,
            limite=limite,
        )
    elif TIPO == "oracle":
        total_linhas, _colunas = extrair_para_duckdb_oracle(
            origem_conn,
            schema_origem,
            table_name,
            CAMINHO_DUCKDB,
            limite=limite,
        )
    elif TIPO == "parquet":
        total_linhas, _colunas = extrair_para_duckdb_parquet(
            caminho_parquet_origem,
            table_name,
            CAMINHO_DUCKDB,
            limite=limite,
        )
    else:
        total_linhas, _colunas = extrair_para_duckdb(
            origem_conn,
            catalogo_origem,
            schema_origem,
            table_name,
            CAMINHO_DUCKDB,
            limite=limite,
        )
    duracao_extracao = round(time.time() - inicio_extracao, 2)
    print(f"Extração: {total_linhas} linhas em {duracao_extracao}s")

    profiler = DuckDBProfiler(CAMINHO_DUCKDB)
    inicio_profiling = time.time()
    full = profiler.profile_full(table_name)
    duracao_profiling = round(time.time() - inicio_profiling, 2)
    print(f"Profiling: {duracao_profiling}s")

    regras_ativas = loader.buscar_regras_usuario_ativas(
        nome_base=table_name,
        sistema_origem=sistema_origem,
        catalogo=catalogo_origem,
        schema_origem=schema_origem,
    )
    resultados_regras_usuario = avaliar_regras(CAMINHO_DUCKDB, table_name, regras_ativas)
    if resultados_regras_usuario:
        qtd_falhas = sum(1 for r in resultados_regras_usuario if not r["passou"])
        print(f"Regras de usuário: {len(resultados_regras_usuario)} avaliadas, {qtd_falhas} falharam")

    # 'nulidade_aceitavel' não é só uma checagem: muda a fórmula de
    # completude do QualityScorer pra essa coluna (ver quality_scorer.py).
    tolerancia_nulidade_por_coluna = {
        r["parametros"]["coluna"]: r["parametros"]["tolerancia_percentual"]
        for r in regras_ativas
        if r["codigo"] == "nulidade_aceitavel"
    }

    scores_por_coluna = {
        coluna: scorer.score(
            metrics,
            nome_coluna=coluna,
            resultados_regras_usuario=resultados_regras_usuario,
            tolerancia_nulidade=tolerancia_nulidade_por_coluna.get(coluna),
        )
        for coluna, metrics in full["colunas"].items()
    }
    if not scores_por_coluna:
        # Tabela sem colunas (ex. `CREATE TABLE t ()`, válido em Postgres),
        # caso real encontrado ao validar o modo "base inteira" com uma
        # tabela propositalmente problemática: sem isso, a divisão abaixo
        # por len(scores_por_coluna) levantaria ZeroDivisionError. Isolado
        # aqui como um erro claro em vez de deixar a exceção genérica subir
        # (o chamador em main() trata qualquer exceção do mesmo jeito, mas
        # esta mensagem é bem mais útil no log/resumo final).
        raise ValueError(f"Tabela '{table_name}' não tem nenhuma coluna perfilável (0 colunas)")
    media_score_colunas = sum(s.score_geral for s in scores_por_coluna.values()) / len(scores_por_coluna)
    percentual_linhas_duplicadas = (full["tabela"].get("p_duplicates") or 0) * 100
    score_final_base = scorer.calculate_final_base_score(media_score_colunas, percentual_linhas_duplicadas)
    classificacao_final_base = scorer.classify(score_final_base)

    modo_coleta = "completo" if limite is None else "amostra"

    sk_execucao = loader.salvar_perfilamento(
        nome_base=table_name,
        sistema_origem=sistema_origem,
        catalogo=catalogo_origem,
        schema_origem=schema_origem,
        full=full,
        scores_por_coluna=scores_por_coluna,
        score_final_base=score_final_base,
        classificacao_final_base=classificacao_final_base,
        ferramenta="duckdb",
        versao_ferramenta=duckdb.__version__,
        modo_coleta=modo_coleta,
        tamanho_amostra=total_linhas,
        duracao_segundos=duracao_profiling,
        config={"extracao_segundos": duracao_extracao},
        resultados_regras_usuario=resultados_regras_usuario,
    )

    print(f"Score final da base: {score_final_base} ({classificacao_final_base})")
    print(f"Persistido no data mart: sk_execucao={sk_execucao}")


if __name__ == "__main__":
    main()
