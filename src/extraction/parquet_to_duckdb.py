"""
Extração de arquivo(s) `.parquet` pra um arquivo DuckDB local. Mais simples
que os outros extratores: parquet já é colunar e tipado, e o DuckDB lê
nativamente via SQL (`read_parquet`), então a extração inteira é uma única
instrução `CREATE TABLE ... AS SELECT * FROM read_parquet(...)`, sem laço
de `fetchmany` em Python nem DataFrame intermediário.

Suporta também um diretório com múltiplos arquivos particionados: basta
passar um glob (`dados/*.parquet`, `dados/ano=2024/*.parquet`) pra
`caminho_parquet`. Todos os arquivos casados pelo glob precisam ter schema
compatível; se não tiverem, o erro do DuckDB propaga normalmente, sem
tentar "consertar" com union_by_name ou cast forçado.

Este módulo nunca decide sozinho se `caminho_parquet` é arquivo, glob ou
diretório: só passa o que recebeu pro `read_parquet`. Quem chama esta
função no modo "tabela única" de `persistir_datamart_duckdb.py` é quem
monta o glob não recursivo quando recebe um diretório comum (ver
`_resolver_caminho_parquet_unico`), diferente do modo TABLES=TODAS, onde o
mesmo diretório vira um catálogo de tabelas distintas (uma por arquivo).
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
    Extrai um arquivo (ou glob) `.parquet` pra uma tabela de mesmo nome num
    arquivo DuckDB local. `caminho_parquet` pode ser um único arquivo ou um
    glob ("/dados/*.parquet"); diretório comum já chega aqui resolvido como
    glob por `_resolver_caminho_parquet_unico`.

    O schema/tipos vêm direto do parquet, sem inferência ou mapeamento
    manual como nas outras fontes. `limite` aplica `LIMIT` na consulta.

    Returns:
        (qtd_linhas_extraidas, nomes_das_colunas)
    """
    # Garante fechar a conexão DuckDB mesmo se a extração falhar, senão ela
    # fica presa no arquivo e bloqueia as próximas tabelas em TABLES=TODAS.
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
