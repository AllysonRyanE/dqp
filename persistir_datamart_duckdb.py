"""
Mesma orquestração de persistir_datamart.py, mas usando o DuckDBProfiler
(via extração em lotes Trino/Postgres -> DuckDB local) em vez do
ydata-profiling. Persiste na mesma base: dim_execucao guarda qual
`ferramenta` gerou cada execução, pra comparar lado a lado.

A extração é configurada em dois lugares: FONTE=<nome> (variável de
ambiente, obrigatória) escolhe qual fonte usar; fontes.ini (ao lado deste
script) é onde cada fonte mora, uma seção por FONTE com `tipo` (trino,
postgres, mysql, sqlserver, oracle ou parquet) e os campos de conexão. Um
arquivo à parte evita que duas fontes do mesmo tipo disputem a mesma
chave no .env, que guarda só o que é comum a todas as execuções: TABLES e
PG_DATAMART_* (destino, nunca a fonte).

MODO "BASE INTEIRA" (TABLES=TODAS, ver `MODO_TODAS_TABELAS`): a lista de
tabelas é descoberta em tempo de execução via
`src/extraction/table_discovery.py`, a partir do schema (ou diretório,
pra parquet) da fonte em uso. Cada tabela é processada isoladamente
(try/except por tabela): uma falhar não interrompe as demais (resumo
final em `main()`).

TIPO=oracle homologado contra Oracle 18c/21c XE via Docker; outras
versões devem funcionar por compatibilidade de protocolo, mas não
verificado empiricamente (ver `src/extraction/oracle_to_duckdb.py`).

TIPO=parquet não tem conexão de banco, só um caminho em `path`. Um
diretório em `path` se comporta diferente conforme TABLES=TODAS: sem
ele, todos os `.parquet` da pasta mesclam numa única base
(`_resolver_caminho_parquet_unico` monta o glob sozinho); com ele, cada
`.parquet` vira uma tabela distinta (`listar_arquivos_parquet`).

Uso: FONTE=<nome> .venv/bin/python persistir_datamart_duckdb.py [tabela1,tabela2,...]
FONTE é sempre obrigatória, com seção [<nome>] em fontes.ini. Sem
argumento de tabelas, roda as de TABLES (.env/linha de comando) ou do
campo "tables" da fonte em fontes.ini; se nenhum existir, cai no modo
"base inteira" (perfila tudo, não é erro de configuração).
"""

import configparser
import os
import sys
import time
from datetime import datetime

import duckdb
import oracledb
import psycopg2
import pymysql
import pymssql
import trino
from dotenv import load_dotenv
from loguru import logger

# Precisa rodar antes de qualquer import de src/: duckdb_profiler.py lê os
# tetos DQP_TETO_* do ambiente na importação, como constante de módulo.
load_dotenv()

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

CAMINHO_FONTES_INI = "fontes.ini"


def _carregar_secao_fonte(fonte: str) -> dict:
    """Lê a seção [<fonte>] de fontes.ini. Cada fonte mora na sua própria
    seção (host, port, user, password, database/catalog/service_name,
    schema, path), o que isola fontes do mesmo tipo sem conflito de nomes."""
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

# Grava um log por execução em logs/, além do terminal: cobre tanto as
# linhas do loguru quanto os print() soltos (via Tee do stdout).
os.makedirs("logs", exist_ok=True)
_timestamp_log = datetime.now().strftime("%Y%m%d_%H%M%S")
_caminho_log = f"logs/persistir_datamart_duckdb_{FONTE}_{_timestamp_log}.log"
logger.add(_caminho_log, level="DEBUG", encoding="utf-8")


class _TeeStdout:
    def __init__(self, destino_original, caminho_arquivo):
        self._original = destino_original
        self._arquivo = open(caminho_arquivo, "a", encoding="utf-8")

    def write(self, texto):
        self._original.write(texto)
        self._arquivo.write(texto)
        return len(texto)

    def flush(self):
        self._original.flush()
        self._arquivo.flush()


sys.stdout = _TeeStdout(sys.stdout, _caminho_log)

# A seção em si (host, port, user, password, ... conforme o tipo) e o TIPO
# (trino/postgres/mysql/sqlserver/oracle/parquet) vêm de dentro dela: ver
# `_carregar_secao_fonte` acima.
SECAO_FONTE = _carregar_secao_fonte(FONTE)
TIPO = SECAO_FONTE["tipo"].strip().lower()


def _segmento_diretorio_significativo(caminho: str) -> str:
    """Segmento de diretório "significativo" mais próximo de `caminho`:
    caminha da direita pra esquerda até achar um que não seja glob nem
    partição Hive. Ex: "/dados/ano=2024/*.parquet" -> "dados". Usado em
    `catalogo_origem` e em `_nome_tabela_padrao_parquet`."""
    diretorio = os.path.dirname(caminho.rstrip("/"))
    partes = [p for p in diretorio.split("/") if p and not any(c in p for c in "*?[") and "=" not in p]
    return partes[-1] if partes else (diretorio or caminho)


def _nome_tabela_padrao_parquet(caminho: str) -> str:
    """Deriva um nome de tabela a partir de `path` quando TABLES não foi
    especificado: nome do arquivo sem extensão, ou, se `path` é um glob
    sem nome fixo, `_segmento_diretorio_significativo`."""
    caminho = caminho.rstrip("/")
    base = os.path.basename(caminho)
    if not base or any(c in base for c in "*?["):
        base = _segmento_diretorio_significativo(caminho) or "dados_parquet"
    nome, _ext = os.path.splitext(base)
    return nome or base


def _resolver_caminho_parquet_unico(caminho: str) -> str:
    """
    Resolve `path` (fontes.ini) pro caminho/glob final passado pra
    `extrair_para_duckdb_parquet` no modo TABELA ÚNICA (fora de
    TABLES=TODAS). Três casos: arquivo específico devolve como está; glob
    explícito (`*`, `?`, `[`) devolve como está; diretório comum sem glob
    monta automaticamente "<diretorio>/*.parquet" (não recursivo, pra não
    colidir com particionamento Hive), pensado pra data lakes com vários
    parquet soltos numa pasta.

    A mesclagem em si é feita pelo DuckDB em `read_parquet`; esta função só
    decide qual caminho/glob passar.

    Não confundir com TABLES=TODAS: aqui um diretório vira uma base
    unificada (N arquivos -> 1 tabela); em TABLES=TODAS, o mesmo diretório
    vira um catálogo de tabelas distintas (ver `listar_arquivos_parquet`).
    """
    caminho_sem_barra = caminho.rstrip("/")
    if any(c in caminho for c in "*?["):
        return caminho  # já é um glob explícito: nada a fazer
    if os.path.isdir(caminho_sem_barra):
        return os.path.join(caminho_sem_barra, "*.parquet")
    return caminho  # arquivo específico (ou caminho inexistente: deixa o erro aparecer no read_parquet, não aqui)


# Lista de tabelas, em ordem de prioridade: argumento de linha de comando
# > TABLES do .env/linha de comando > campo "tables" da fonte em
# fontes.ini. TIPO=parquet sem nenhum dos três deriva o nome de `path`
# (`_nome_tabela_padrao_parquet`); qualquer outro tipo cai no modo "base
# inteira" (TABLES=TODAS) por padrão, não como erro de configuração.
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

# TABLES=TODAS (case-insensitive) pula a lista explícita e descobre todas
# as tabelas da fonte em main() (ver `table_discovery.py`). "TODAS" em vez
# de "*" pra evitar colisão com glob de shell sem aspas.
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
    """Conexão com o SQL Server FONTE (dados a perfilar, distinto do
    Postgres do data mart). Usa pymssql, não pyodbc (ver docstring de
    extraction/sqlserver_to_duckdb.py)."""
    return pymssql.connect(
        server=secao["host"],
        port=int(secao["port"]),
        user=secao["user"],
        password=secao["password"],
        database=secao["database"],
    )


def get_oracle_source_connection(secao):
    """Conexão com o Oracle FONTE (dados a perfilar, distinto do Postgres
    do data mart). Usa python-oracledb em modo thin, sem exigir Oracle
    Instant Client instalado (ver extraction/oracle_to_duckdb.py). `dsn`
    monta `host:port/service_name`, já que Oracle usa "service name" em
    vez do "database" simples do Postgres/MySQL."""
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

    # Conector de extração: TIPO decide a conexão e o bloco de variáveis lido.
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
        # Parquet não tem conexão nem "banco"/"schema" próprios, então
        # `catalogo_origem` usa o segmento de diretório significativo mais
        # próximo (`_segmento_diretorio_significativo`), truncado em 100
        # chars pro limite de `dim_base.catalogo` (VARCHAR(100)).
        # `schema_origem` fica vazio: não há namespace a representar.
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
        # Em TABLES=TODAS com parquet, caminho_parquet_origem é o diretório
        # inteiro: cada tabela precisa apontar pro seu próprio arquivo,
        # senão `read_parquet` relê o diretório inteiro a cada iteração.
        # Fora desse modo, `_resolver_caminho_parquet_unico` já resolveu o
        # caminho (arquivo, glob ou diretório mesclado numa base só).
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
                # `origem_conn` é reutilizada entre tabelas: uma falha pode
                # deixá-la numa transação abortada, daí o rollback de
                # segurança pra não contaminar as próximas tabelas do loop.
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
