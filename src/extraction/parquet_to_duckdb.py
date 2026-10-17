"""
Extração de arquivo(s) `.parquet` (a FONTE de dados a perfilar) pra um
arquivo DuckDB local. Fundamentalmente mais simples que
`postgres_to_duckdb.py`/`mysql_to_duckdb.py`/`trino_to_duckdb.py`: parquet já
é colunar e já tem schema tipado embutido no próprio arquivo (não precisa
inferir nada, nem do dado nem de metadado de conexão via `cursor.description`,
aqui não existe cursor/conexão de banco nenhuma, só o arquivo), e o DuckDB
lê parquet nativamente via SQL (`read_parquet`), com pushdown/streaming
interno no próprio motor C++. Por isso a extração inteira é UMA instrução SQL
(`CREATE TABLE ... AS SELECT * FROM read_parquet(...)`), sem o laço de
`fetchmany`/lote em Python que as outras três fontes precisam: não há
"cliente" Python fazendo streaming aqui, é o próprio DuckDB (embedded, mesmo
processo) lendo o arquivo direto.

Por que isso é seguro em memória (mesmo argumento central do TCC sobre
volume, agora pra parquet): `CREATE TABLE AS SELECT` no DuckDB roda no motor
de execução vetorizada nativo dele, que processa o parquet em "row groups"/
batches de ~2048 linhas por vez (streaming out-of-core), igual a qualquer
outra consulta DuckDB: em NENHUM momento os dados passam por uma estrutura
Python (nada de pandas.DataFrame aqui, ao contrário das outras três fontes,
que precisam de um DataFrame pequeno por lote só pra ponte cursor->DuckDB).
Essa afirmação foi validada empiricamente (não assumida): ver
`docs/validacao_parquet.md` (ou o relatório da tarefa) pro RSS medido durante
a extração de um parquet de milhões de linhas: o consumo de memória do
processo NÃO cresce linearmente com o tamanho do arquivo.

Suporta também um diretório com múltiplos arquivos parquet particionados
(padrão comum de exportação em lotes, ex. `dados/*.parquet` ou
`dados/ano=2024/*.parquet`): basta passar um glob pattern pra
`caminho_parquet`, que o `read_parquet` do DuckDB aceita nativamente (lista
de arquivos, glob com `*`, ou até lista Python de caminhos: usamos só o
glob/string aqui, é o suficiente pro caso de uso). Todos os arquivos
casados pelo glob precisam ter schema compatível (mesmas colunas/tipos, ou
tipos promovíveis); se não tiverem, o DuckDB levanta erro na hora do
`read_parquet` e deixamos esse erro propagar como está (sem tentar
"consertar" schema divergente com union_by_name ou cast forçado: ver
docstring de `extrair_para_duckdb_parquet` abaixo).

Este módulo em si NUNCA decide sozinho se `caminho_parquet` é um arquivo, um
glob ou um diretório comum: ele só passa o que recebeu pro `read_parquet`.
Quem chama esta função no modo "tabela única" de `persistir_datamart_duckdb.py`
é quem detecta um diretório comum (sem glob) e monta o glob não recursivo
automaticamente antes de chamar aqui (ver `_resolver_caminho_parquet_unico`
nesse módulo): assim um usuário apontando pra uma pasta cheia de `.parquet`
de mesmo schema (data lake real, exportações em lote sem convenção Hive) não
precisa saber que precisaria digitar `/*.parquet` no final; só apontar pra
pasta já mescla tudo numa base só. Isso é diferente do modo TABLES=TODAS
(mesmo script), onde o MESMO diretório vira um catálogo de tabelas
distintas (uma por arquivo): ver docstring de `persistir_datamart_duckdb.py`
e de `extraction/table_discovery.py` pra essa distinção completa.
"""

import duckdb
from loguru import logger


def extrair_para_duckdb_parquet(
    caminho_parquet: str,
    tabela: str,
    caminho_duckdb: str,
    limite: int = None,
) -> tuple[int, list[str]]:
    """
    Extrai um arquivo (ou glob de arquivos) `.parquet` pra uma tabela de
    mesmo nome num arquivo DuckDB local.

    `caminho_parquet` pode ser:
      - um único arquivo: "/dados/tabela.parquet"
      - um glob de múltiplos arquivos particionados: "/dados/*.parquet" ou
        "/dados/ano=2024/*.parquet" (o DuckDB expande o glob nativamente
        dentro de `read_parquet`, sem precisar listar os arquivos em Python)

    Esta função nunca recebe um diretório comum "cru" no modo tabela única
    de `persistir_datamart_duckdb.py`: o glob acima já vem pronto de
    `_resolver_caminho_parquet_unico` nesse módulo, que detecta diretório
    sem glob e monta "<diretorio>/*.parquet" antes de chegar aqui.

    Diferente das fontes de banco vivo (Trino/Postgres/MySQL), não há lote
    nem cursor: uma única instrução `CREATE TABLE ... AS SELECT * FROM
    read_parquet(...)` entrega streaming out-of-core de graça, cortesia do
    motor de execução vetorizada do próprio DuckDB (ver docstring do módulo
    pra a explicação completa e a validação empírica de memória).

    O schema/tipos da tabela resultante vêm DIRETO do parquet (colunas já
    tipadas nativamente no arquivo: DATE, TIMESTAMP, BOOLEAN, DECIMAL,
    BIGINT etc. continuam exatamente esses tipos no DuckDB, sem qualquer
    inferência ou mapeamento manual como nas outras três fontes), então não
    existe aqui um dicionário `_TIPO_PARA_DUCKDB` equivalente ao das outras
    três: não tem o que mapear.

    `limite`: quando informado, aplica `LIMIT` na consulta (útil pra
    amostragem/teste rápido, mesmo padrão das outras três fontes). Sem ele,
    lê o arquivo inteiro.

    Fallback seguro: se `read_parquet` falhar (ex. arquivo corrompido,
    ausente, ou schema incompatível entre arquivos de um glob), o erro do
    DuckDB propaga como está: não tentamos nenhum CAST/coerção exótica pra
    "salvar" um schema divergente.

    Returns:
        (qtd_linhas_extraidas, nomes_das_colunas)
    """
    # try/finally em volta de todo o uso de `con`: sem isso, uma falha no
    # meio (ex. parquet corrompido/schema incompatível) deixa a conexão
    # DuckDB aberta pro arquivo `caminho_duckdb`: inofensivo rodando uma
    # tabela isolada (processo termina, SO libera tudo), mas um bug real e
    # reproduzível no modo "base inteira" (TABLES=TODAS, ver
    # persistir_datamart_duckdb.py): a conexão vazada de UMA tabela que falhou
    # bloqueia o `duckdb.connect(..., read_only=True)` do profiler pra
    # QUALQUER tabela seguinte no mesmo arquivo ("Connection Error: Can't
    # open a connection to same database file with a different
    # configuration"), quebrando o isolamento de erro por tabela que é o
    # requisito central desse modo. Reproduzido e corrigido durante a
    # validação da tarefa que introduziu TABLES=TODAS.
    con = duckdb.connect(caminho_duckdb)
    try:
        con.execute(f'DROP TABLE IF EXISTS "{tabela}"')

        query = f"SELECT * FROM read_parquet('{caminho_parquet}')"
        if limite is not None:
            query += f" LIMIT {limite}"

        logger.info(f"Extraindo {caminho_parquet} -> tabela '{tabela}' em {caminho_duckdb} (via read_parquet)...")
        con.execute(f'CREATE TABLE "{tabela}" AS {query}')

        total = con.execute(f'SELECT COUNT(*) FROM "{tabela}"').fetchone()[0]
        colunas = [r[0] for r in con.execute(f'DESCRIBE "{tabela}"').fetchall()]
    finally:
        con.close()
    logger.success(f"Extração de {tabela} concluída: {total} linhas em {caminho_duckdb}")
    return total, colunas
