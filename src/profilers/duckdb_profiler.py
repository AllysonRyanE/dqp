"""
Profiler baseado em DuckDB: mesma forma de saída que YDataProfiler.profile_full(),
mas calculado via SQL sobre um arquivo DuckDB local (sem limite de RAM do
pandas, sem depender do ydata-profiling). Pensado pra tabelas que não cabem
confortavelmente em memória.

Convenção de chaves no dict de métricas por coluna: segue a MESMA convenção
mista que o YDataProfiler já usava (compatibilidade com data_mart/loader.py
sem precisar alterá-lo):
  - chaves "percentual_*" já vêm multiplicadas por 100
  - chaves "p_*" (estilo ydata) vêm como fração 0-1 (o loader multiplica por 100)
  - "min"/"max" servem tanto pra numérico quanto pra data (o loader decide
    qual campo final usar olhando tipo_dado_inferido)
"""

import math
from typing import Any, Dict, List, Optional

import duckdb
from loguru import logger

CARDINALIDADE_MAXIMA_CATEGORIA_TEXTO = 50
PROPORCAO_MAXIMA_CATEGORIA_TEXTO = 0.5
CARDINALIDADE_MAXIMA_CATEGORIA_NUMERICA = 5
LIMIAR_PARSE_NUMERICO_OU_DATA = 0.9
CARDINALIDADE_MAXIMA_PARA_CONTAGEM_EXATA_DUPLICATAS = 40  # colunas; acima disso, pula duplicatas de linha


class DuckDBProfiler:
    def __init__(self, caminho_duckdb: str):
        self.caminho_duckdb = caminho_duckdb

    # ------------------------------------------------------------------ #
    # Orquestração
    # ------------------------------------------------------------------ #

    def profile_full(self, tabela: str) -> Dict[str, Any]:
        con = duckdb.connect(self.caminho_duckdb, read_only=True)
        try:
            descricao_colunas = con.execute(f'DESCRIBE "{tabela}"').fetchall()
            colunas = [r[0] for r in descricao_colunas]
            tipos_fisicos = {r[0]: r[1] for r in descricao_colunas}
            logger.info(f"Iniciando profiling (DuckDB) para {len(colunas)} colunas em '{tabela}'")

            metricas_colunas = {}
            for coluna in colunas:
                metricas_colunas[coluna] = self._metricas_coluna(con, tabela, coluna, tipos_fisicos[coluna])
                logger.info(f"Profiling DuckDB concluído para {coluna}: {len(metricas_colunas[coluna])} métricas")

            tabela_info = self._metricas_tabela(con, tabela, colunas, metricas_colunas)
            correlacoes = self._correlacoes(con, tabela, colunas, metricas_colunas)
            padrao_ausencia = self._padrao_ausencia(con, tabela, colunas)
            duplicatas = self._duplicatas(con, tabela, colunas)

            return {
                "colunas": metricas_colunas,
                "tabela": tabela_info,
                "alertas": [],  # gerado por etapa separada (alertas automáticos, ver tarefa dedicada)
                "correlacoes": correlacoes,
                "padrao_ausencia": padrao_ausencia,
                "duplicatas": duplicatas,
            }
        finally:
            con.close()

    # ------------------------------------------------------------------ #
    # Inferência de tipo
    # ------------------------------------------------------------------ #

    def _inferir_tipo(self, n_nao_nulo: int, n_distinto: int, n_parse_num: int, n_parse_data: int) -> str:
        if n_nao_nulo == 0:
            return "Unsupported"

        pct_num = n_parse_num / n_nao_nulo
        pct_data = n_parse_data / n_nao_nulo
        pct_distinto = n_distinto / n_nao_nulo

        if pct_data >= LIMIAR_PARSE_NUMERICO_OU_DATA and pct_num < LIMIAR_PARSE_NUMERICO_OU_DATA:
            return "DateTime"

        if pct_num >= LIMIAR_PARSE_NUMERICO_OU_DATA:
            if 1 <= n_distinto <= CARDINALIDADE_MAXIMA_CATEGORIA_NUMERICA:
                return "Categorical"
            return "Numeric"

        if 1 <= n_distinto <= CARDINALIDADE_MAXIMA_CATEGORIA_TEXTO and pct_distinto < PROPORCAO_MAXIMA_CATEGORIA_TEXTO:
            return "Categorical"

        return "Text"

    # ------------------------------------------------------------------ #
    # Métricas por coluna
    # ------------------------------------------------------------------ #

    def _metricas_coluna(self, con, tabela: str, coluna: str, tipo_fisico: str) -> Dict[str, Any]:
        n, n_nao_nulo, n_distinto, n_parse_num, n_parse_data = con.execute(
            f"""
            SELECT
                COUNT(*),
                COUNT("{coluna}"),
                COUNT(DISTINCT "{coluna}"),
                COUNT(TRY_CAST("{coluna}" AS DOUBLE)),
                COUNT(TRY_CAST("{coluna}" AS TIMESTAMP))
            FROM "{tabela}"
            """
        ).fetchone()

        tipo = self._inferir_tipo(n_nao_nulo, n_distinto, n_parse_num, n_parse_data)

        metrics: Dict[str, Any] = {
            "qtd_linhas_total": n,
            "qtd_nulos": n - n_nao_nulo,
            "percentual_nulos": round(100 * (n - n_nao_nulo) / n, 2) if n else 0.0,
            "qtd_valores_distintos": n_distinto,
            "qtd_duplicados": n - n_distinto,
            "seletividade": round(n_distinto / n, 4) if n else 0.0,
            "tipo_dado_inferido": tipo,
            "tipo_fisico_duckdb": tipo_fisico,
        }

        # Calculado pra qualquer tipo (não só Text/Categorical) porque
        # system_alerts.py precisa comparar esse percentual com o tipo físico
        # da coluna, mesmo quando ela já foi classificada como Numeric/DateTime.
        metrics["percentual_parse_numerico"] = round(100 * n_parse_num / n_nao_nulo, 2) if n_nao_nulo else 0.0
        metrics["percentual_parse_data"] = round(100 * n_parse_data / n_nao_nulo, 2) if n_nao_nulo else 0.0

        if n_nao_nulo > 0:
            metrics.update(self._moda_e_concentracao(con, tabela, coluna, n_nao_nulo))
        else:
            metrics.update(
                {"moda": None, "frequencia_moda": 0, "percentual_valor_mais_frequente": 0.0, "indice_concentracao_categorica": 0.0}
            )

        if tipo == "Numeric":
            metrics.update(self._estatistica_numerica(con, tabela, coluna))
        elif tipo == "DateTime":
            metrics.update(self._estatistica_data(con, tabela, coluna, n_nao_nulo))
        elif tipo in ("Text", "Categorical"):
            metrics.update(self._estatistica_texto(con, tabela, coluna))

        metrics["memory_size"] = None  # conceito de footprint em memória pandas não se aplica aqui

        return metrics

    def _moda_e_concentracao(self, con, tabela: str, coluna: str, n_nao_nulo: int) -> Dict[str, Any]:
        moda, freq = con.execute(
            f'SELECT "{coluna}", COUNT(*) AS freq FROM "{tabela}" WHERE "{coluna}" IS NOT NULL '
            f'GROUP BY "{coluna}" ORDER BY freq DESC LIMIT 1'
        ).fetchone()

        (herfindahl,) = con.execute(
            f"""
            WITH props AS (
                SELECT COUNT(*)::DOUBLE / {n_nao_nulo} AS p
                FROM "{tabela}" WHERE "{coluna}" IS NOT NULL GROUP BY "{coluna}"
            )
            SELECT SUM(p * p) FROM props
            """
        ).fetchone()

        return {
            "moda": moda,
            "frequencia_moda": int(freq),
            "percentual_valor_mais_frequente": round(100 * freq / n_nao_nulo, 2),
            "indice_concentracao_categorica": round(float(herfindahl), 4),
        }

    def _estatistica_numerica(self, con, tabela: str, coluna: str) -> Dict[str, Any]:
        (
            minimo, maximo, media, mediana, desvio, variancia, skew, kurt, p25, p75,
            n_zero, n_neg, n_inf, n_finitos,
        ) = con.execute(
            f"""
            WITH v AS (SELECT TRY_CAST("{coluna}" AS DOUBLE) AS x FROM "{tabela}" WHERE "{coluna}" IS NOT NULL)
            SELECT
                MIN(x), MAX(x), AVG(x), MEDIAN(x), STDDEV(x), VARIANCE(x), SKEWNESS(x), KURTOSIS(x),
                QUANTILE_CONT(x, 0.25), QUANTILE_CONT(x, 0.75),
                COUNT(*) FILTER (WHERE x = 0),
                COUNT(*) FILTER (WHERE x < 0),
                COUNT(*) FILTER (WHERE isinf(x)),
                COUNT(*) FILTER (WHERE NOT isinf(x))
            FROM v WHERE x IS NOT NULL
            """
        ).fetchone()

        resultado = {
            "min": minimo,
            "max": maximo,
            "mean": media,
            "50%": mediana,
            "std": desvio,
            "variance": variancia,
            "skewness": skew,
            "kurtosis": kurt,
            "25%": p25,
            "75%": p75,
            "cv": round(desvio / media, 6) if media else None,
            "n_zeros": n_zero,
            "p_zeros": round(n_zero / n_finitos, 6) if n_finitos else 0.0,
            "n_negative": n_neg,
            "p_negative": round(n_neg / n_finitos, 6) if n_finitos else 0.0,
            "n_infinite": n_inf,
        }

        if p25 is not None and p75 is not None:
            iqr = p75 - p25
            limite_inf, limite_sup = p25 - 1.5 * iqr, p75 + 1.5 * iqr
            (qtd_outliers,) = con.execute(
                f"""
                SELECT COUNT(*) FROM (SELECT TRY_CAST("{coluna}" AS DOUBLE) AS x FROM "{tabela}" WHERE "{coluna}" IS NOT NULL) v
                WHERE x < {limite_inf} OR x > {limite_sup}
                """
            ).fetchone()
            resultado["qtd_outliers"] = qtd_outliers
            resultado["percentual_outliers"] = round(100 * qtd_outliers / n_finitos, 2) if n_finitos else 0.0
        else:
            resultado["qtd_outliers"] = 0
            resultado["percentual_outliers"] = 0.0

        return resultado

    def _estatistica_data(self, con, tabela: str, coluna: str, n_nao_nulo: int) -> Dict[str, Any]:
        minimo, maximo, n_invalidas = con.execute(
            f"""
            SELECT
                MIN(TRY_CAST("{coluna}" AS TIMESTAMP)),
                MAX(TRY_CAST("{coluna}" AS TIMESTAMP)),
                COUNT(*) FILTER (WHERE "{coluna}" IS NOT NULL AND TRY_CAST("{coluna}" AS TIMESTAMP) IS NULL)
            FROM "{tabela}"
            """
        ).fetchone()

        return {
            "min": minimo,
            "max": maximo,
            "n_invalid_dates": n_invalidas,
            "p_invalid_dates": round(n_invalidas / n_nao_nulo, 6) if n_nao_nulo else 0.0,
        }

    def _estatistica_texto(self, con, tabela: str, coluna: str) -> Dict[str, Any]:
        # "Texto"/"Categorical" agora pode cair sobre uma coluna que não é
        # VARCHAR de fato (ex.: BOOLEAN ou BIGINT de baixa cardinalidade,
        # possível desde que a extração passou a preservar o tipo da fonte
        # em vez de forçar VARCHAR): LENGTH()/regexp_extract_all() só aceitam
        # VARCHAR nativamente no DuckDB (erro de binder em BOOLEAN/BIGINT/
        # DATE/etc., testado). CAST(... AS VARCHAR) normaliza pra string
        # antes, preservando o mesmo cálculo que já era feito quando tudo
        # entrava como VARCHAR.
        media, mediana, maximo, minimo = con.execute(
            f"""
            SELECT AVG(LENGTH(CAST("{coluna}" AS VARCHAR))), MEDIAN(LENGTH(CAST("{coluna}" AS VARCHAR))),
                   MAX(LENGTH(CAST("{coluna}" AS VARCHAR))), MIN(LENGTH(CAST("{coluna}" AS VARCHAR)))
            FROM "{tabela}" WHERE "{coluna}" IS NOT NULL
            """
        ).fetchone()

        # Caracteres distintos: aproximação sobre até 500 valores distintos
        # (evita explodir string->caracteres numa coluna de texto livre gigante).
        (n_chars_distintos,) = con.execute(
            f"""
            WITH amostra AS (
                SELECT DISTINCT CAST("{coluna}" AS VARCHAR) AS v FROM "{tabela}" WHERE "{coluna}" IS NOT NULL LIMIT 500
            ),
            chars AS (
                SELECT UNNEST(regexp_extract_all(v, '.')) AS c FROM amostra
            )
            SELECT COUNT(DISTINCT c) FROM chars
            """
        ).fetchone()

        return {
            "mean_length": media,
            "median_length": mediana,
            "max_length": maximo,
            "min_length": minimo,
            "n_characters_distinct": n_chars_distintos,
        }

    # ------------------------------------------------------------------ #
    # Métricas da tabela inteira
    # ------------------------------------------------------------------ #

    def _metricas_tabela(self, con, tabela: str, colunas: List[str], metricas_colunas: Dict[str, Any]) -> Dict[str, Any]:
        (n,) = con.execute(f'SELECT COUNT(*) FROM "{tabela}"').fetchone()
        n_var = len(colunas)

        n_cells_missing = sum(m["qtd_nulos"] for m in metricas_colunas.values())
        n_vars_with_missing = sum(1 for m in metricas_colunas.values() if m["qtd_nulos"] > 0)
        tipos = {}
        for m in metricas_colunas.values():
            tipos[m["tipo_dado_inferido"]] = tipos.get(m["tipo_dado_inferido"], 0) + 1

        n_duplicates = None
        if n_var <= CARDINALIDADE_MAXIMA_PARA_CONTAGEM_EXATA_DUPLICATAS:
            colunas_sql = ", ".join(f'"{c}"' for c in colunas)
            (n_distintas,) = con.execute(f'SELECT COUNT(*) FROM (SELECT DISTINCT {colunas_sql} FROM "{tabela}") t').fetchone()
            n_duplicates = n - n_distintas

        return {
            "n": n,
            "n_var": n_var,
            "n_cells_missing": n_cells_missing,
            "p_cells_missing": round(n_cells_missing / (n * n_var), 6) if n and n_var else 0.0,
            "n_vars_with_missing": n_vars_with_missing,
            "n_duplicates": n_duplicates if n_duplicates is not None else 0,
            "p_duplicates": round(n_duplicates / n, 6) if n_duplicates and n else 0.0,
            "memory_size": None,
            "record_size": None,
            "types": tipos,
        }

    # ------------------------------------------------------------------ #
    # Correlação: Pearson (numérico x numérico) + Cramér's V (categórico x categórico)
    # ------------------------------------------------------------------ #

    # Cramér's V entre 2 colunas quase-únicas não diz nada de útil (a
    # correlação de duas colunas de texto livre é sempre ~indefinida) e o
    # tamanho da tabela de contingência cresce com o produto das
    # cardinalidades: acima disso, nem calculamos.
    CARDINALIDADE_MAXIMA_PARA_CRAMERS_V = 200

    def _correlacoes(self, con, tabela: str, colunas: List[str], metricas_colunas: Dict[str, Any]) -> Dict[str, Any]:
        numericas = [c for c in colunas if metricas_colunas[c]["tipo_dado_inferido"] == "Numeric"]
        categoricas = [
            c
            for c in colunas
            if metricas_colunas[c]["tipo_dado_inferido"] in ("Categorical", "Text")
            and metricas_colunas[c]["qtd_valores_distintos"] <= self.CARDINALIDADE_MAXIMA_PARA_CRAMERS_V
        ]

        registros = []
        colunas_ordem = numericas + categoricas

        for a in colunas_ordem:
            linha = {}
            for b in colunas_ordem:
                if a == b:
                    linha[b] = 1.0
                    continue
                if a in numericas and b in numericas:
                    (valor,) = con.execute(
                        f'SELECT CORR(TRY_CAST("{a}" AS DOUBLE), TRY_CAST("{b}" AS DOUBLE)) FROM "{tabela}"'
                    ).fetchone()
                    linha[b] = round(float(valor), 4) if valor is not None else None
                elif a in categoricas and b in categoricas:
                    linha[b] = self._cramers_v(con, tabela, a, b)
                else:
                    linha[b] = None  # par misto (numérico x categórico): fora do escopo desta primeira versão
            registros.append(linha)

        if not registros:
            return {}
        return {"auto": registros}

    def _cramers_v(self, con, tabela: str, a: str, b: str) -> Optional[float]:
        """Qui-quadrado calculado inteiramente em SQL (sem laço em Python: era
        isso que tornava o cálculo O(cardinalidade²) e lento de verdade)."""
        con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW contingencia AS
            SELECT "{a}" AS va, "{b}" AS vb, COUNT(*) AS observado
            FROM "{tabela}" WHERE "{a}" IS NOT NULL AND "{b}" IS NOT NULL
            GROUP BY "{a}", "{b}"
            """
        )

        n, qtd_linhas, qtd_colunas = con.execute(
            """
            SELECT
                (SELECT SUM(observado) FROM contingencia),
                (SELECT COUNT(DISTINCT va) FROM contingencia),
                (SELECT COUNT(DISTINCT vb) FROM contingencia)
            """
        ).fetchone()

        if not n or qtd_linhas < 2 or qtd_colunas < 2:
            return None

        (qui_quadrado,) = con.execute(
            f"""
            WITH totais_linha AS (SELECT va, SUM(observado) AS total_linha FROM contingencia GROUP BY va),
            totais_coluna AS (SELECT vb, SUM(observado) AS total_coluna FROM contingencia GROUP BY vb)
            SELECT SUM(POWER(c.observado - (tl.total_linha * tc.total_coluna / {n}), 2) / (tl.total_linha * tc.total_coluna / {n}))
            FROM contingencia c
            JOIN totais_linha tl ON tl.va = c.va
            JOIN totais_coluna tc ON tc.vb = c.vb
            """
        ).fetchone()

        if qui_quadrado is None:
            return None

        k = min(qtd_linhas, qtd_colunas)
        return round(math.sqrt(qui_quadrado / (n * (k - 1))), 4)

    # ------------------------------------------------------------------ #
    # Padrão de ausência: correlação entre indicadores de nulo (0/1)
    # ------------------------------------------------------------------ #

    def _padrao_ausencia(self, con, tabela: str, colunas: List[str]) -> Dict[str, Any]:
        com_nulo = []
        for c in colunas:
            (tem_nulo,) = con.execute(f'SELECT COUNT(*) > 0 FROM "{tabela}" WHERE "{c}" IS NULL').fetchone()
            if tem_nulo:
                com_nulo.append(c)

        if len(com_nulo) < 2:
            return {}

        registros = []
        for a in com_nulo:
            linha = {}
            for b in com_nulo:
                if a == b:
                    linha[b] = 1.0
                    continue
                (valor,) = con.execute(
                    f"""
                    SELECT CORR(CASE WHEN "{a}" IS NULL THEN 1.0 ELSE 0.0 END, CASE WHEN "{b}" IS NULL THEN 1.0 ELSE 0.0 END)
                    FROM "{tabela}"
                    """
                ).fetchone()
                linha[b] = round(float(valor), 4) if valor is not None else None
            registros.append(linha)

        return registros

    # ------------------------------------------------------------------ #
    # Amostra de linhas duplicadas
    # ------------------------------------------------------------------ #

    def _duplicatas(self, con, tabela: str, colunas: List[str], limite: int = 10) -> List[Dict[str, Any]]:
        if len(colunas) > CARDINALIDADE_MAXIMA_PARA_CONTAGEM_EXATA_DUPLICATAS:
            return []

        colunas_sql = ", ".join(f'"{c}"' for c in colunas)
        linhas = con.execute(
            f"""
            SELECT {colunas_sql} FROM "{tabela}"
            GROUP BY {colunas_sql} HAVING COUNT(*) > 1 LIMIT {limite}
            """
        ).fetchall()

        return [dict(zip(colunas, linha)) for linha in linhas]
