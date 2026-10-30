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
import os
from typing import Any, Dict, List, Optional

import duckdb
from loguru import logger


def _teto_env(nome_env: str, padrao):
    """Lê um teto de custo computacional do ambiente, com fallback pro padrão
    testado neste trabalho. Só os tetos ligados a CUSTO (tamanho de tabela de
    contingência, quantidade de pares testados etc.) são configuráveis assim:
    eles existem por causa da infraestrutura de teste atual, não por uma regra
    de negócio ou de qualidade de dados, então variam de ambiente pra ambiente
    sem precisar de uma nova versão do código. Os limiares que definem
    SEMÂNTICA (o que conta como Identifier, Categorical etc.) continuam fixos
    no código: não são sobre custo, são sobre o que a métrica significa."""
    valor = os.getenv(nome_env)
    if valor is None:
        return padrao
    return type(padrao)(valor)


CARDINALIDADE_MAXIMA_CATEGORIA_TEXTO = 50
PROPORCAO_MAXIMA_CATEGORIA_TEXTO = 0.5
CARDINALIDADE_MAXIMA_CATEGORIA_NUMERICA = 5
LIMIAR_PARSE_NUMERICO_OU_DATA = 0.9
CARDINALIDADE_MAXIMA_PARA_CONTAGEM_EXATA_DUPLICATAS = _teto_env("DQP_TETO_COLUNAS_DUPLICATAS", 40)  # colunas, acima disso pula duplicatas de linha

# Identificador "tratado como número" (ex.: CPF, matrícula): mesmo passando no
# corte de LIMIAR_PARSE_NUMERICO_OU_DATA, uma coluna só vira "Identifier" (em
# vez de "Numeric") se, além disso, tiver formato de código. Dois sinais,
# qualquer um basta: (a) prova direta, algum valor já perdeu zero à esquerda
# no cast pra número; (b) prova por formato, largura praticamente fixa e
# quase todo valor distinto (cara de chave, não de métrica a somar/mediar).
TAMANHOS_DISTINTOS_MAXIMO_IDENTIFICADOR = 2
SELETIVIDADE_MINIMA_IDENTIFICADOR = 0.9
# Sinal (b) sozinho (largura quase fixa + seletividade alta) dispara em
# qualquer coluna de tabela pequena, onde praticamente todo valor já é
# distinto só pelo tamanho da amostra: exige também um mínimo de linhas e,
# quando a coluna não é texto, uma largura mínima de dígitos. Senão, um
# inteiro curto (ex.: idade) vira Identifier à toa.
QTD_MINIMA_LINHAS_PARA_SINAL_FORMATO_IDENTIFICADOR = 50
LARGURA_MINIMA_IDENTIFICADOR_NUMERICO = 6

# Coluna que só varia a HORA, com a data sempre igual a um valor sentinela
# (comum em exportação SAP, ex.: "1970-01-01T05:54:14Z"): dia da semana e
# intervalo entre datas não fariam sentido aqui, porque não há de fato uma
# data variando, só um horário dentro de um timestamp.
MAPA_DIAS_SEMANA_PT = {
    "Monday": "Segunda-feira",
    "Tuesday": "Terça-feira",
    "Wednesday": "Quarta-feira",
    "Thursday": "Quinta-feira",
    "Friday": "Sexta-feira",
    "Saturday": "Sábado",
    "Sunday": "Domingo",
}
ORDEM_DIAS_SEMANA_PT = list(MAPA_DIAS_SEMANA_PT.values())

# Dependência funcional (origem -> destino): descoberta automática, não regra
# declarada pelo usuário. Três tetos pra não deixar o custo crescer com o
# quadrado da quantidade de colunas (ver docs/achados_tese ou changelog):
#   1) QTD_MAXIMA_COLUNAS_PARA_ANALISE_DE_PARES corta a raiz do problema e
#      protege também a matriz de correlação (_correlacoes), que já era
#      O(colunas²) antes desta mudança;
#   2) LIMIAR_CRAMERS_V_PARA_TESTAR_FD só testa FD nos pares que a correlação
#      já calculada apontou como fortemente associados, reaproveita o
#      trabalho já feito em vez de rodar um novo exame de cada par;
#   3) QTD_MAXIMA_PARES_TESTADOS_FD é o teto fixo final, independente de
#      quantos pares passarem no limiar acima.
# SELETIVIDADE_MAXIMA_ORIGEM_PARA_FD descarta a origem quase-única: o caso
# mais caro de agrupar (quase um grupo por linha) e o menos informativo como
# achado (uma coluna quase-chave "determina" qualquer coisa trivialmente).
QTD_MAXIMA_COLUNAS_PARA_ANALISE_DE_PARES = _teto_env("DQP_TETO_COLUNAS_CORRELACAO", 60)
LIMIAR_CRAMERS_V_PARA_TESTAR_FD = _teto_env("DQP_LIMIAR_CRAMERS_V_FD", 0.6)
SELETIVIDADE_MAXIMA_ORIGEM_PARA_FD = _teto_env("DQP_TETO_SELETIVIDADE_ORIGEM_FD", 0.5)
QTD_MAXIMA_PARES_TESTADOS_FD = _teto_env("DQP_TETO_PARES_TESTADOS_FD", 20)
LIMIAR_ADERENCIA_PARA_REPORTAR_FD = 0.95  # % dos grupos de origem sem nenhuma exceção, pra virar achado: critério de qualidade do achado, não de custo, fica fixo
MINIMO_GRUPOS_ORIGEM_PARA_REPORTAR_FD = 3  # com só 2 grupos de origem a "regra" é quase trivial e pouco informativa como achado


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
            dependencias_funcionais = self._dependencias_funcionais(con, tabela, metricas_colunas, correlacoes)
            padrao_ausencia = self._padrao_ausencia(con, tabela, colunas, metricas_colunas)
            duplicatas = self._duplicatas(con, tabela, colunas)

            return {
                "colunas": metricas_colunas,
                "tabela": tabela_info,
                "correlacoes": correlacoes,
                "dependencias_funcionais": dependencias_funcionais,
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

        expressao_data_numerica = None
        if tipo == "Numeric":
            # Data codificada como inteiro (AAAAMMDD) tem precedência sobre
            # Identifier: a largura fixa de 8 dígitos é incidental à data, não
            # uma cara de código (B6/achado 9 de docs/achados_tese.sql).
            expressao_data_numerica = self._parece_data_numerica(con, tabela, coluna)
            if expressao_data_numerica is not None:
                tipo = "DateTime"
            elif self._parece_identificador(con, tabela, coluna, tipo_fisico, n_nao_nulo, n_distinto):
                tipo = "Identifier"

        if tipo == "DateTime" and expressao_data_numerica is None and self._parece_apenas_hora(con, tabela, coluna, n_distinto):
            tipo = "Time"

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
            metrics.update(
                self._estatistica_data(con, tabela, coluna, n_nao_nulo, expressao_timestamp=expressao_data_numerica)
            )
        elif tipo == "Time":
            metrics.update(self._estatistica_data(con, tabela, coluna, n_nao_nulo, calcular_ciclo=False))
        elif tipo in ("Text", "Categorical", "Identifier"):
            metrics.update(self._estatistica_texto(con, tabela, coluna))

        metrics["memory_size"] = None  # conceito de footprint em memória pandas não se aplica aqui

        return metrics

    def _parece_identificador(self, con, tabela: str, coluna: str, tipo_fisico: str, n_nao_nulo: int, n_distinto: int) -> bool:
        """Só é chamada quando a coluna já passaria no corte de Numeric (pct_num
        alto): não desperdiça esse exame de formato em texto livre nem em data."""
        if n_nao_nulo == 0:
            return False

        eh_fisicamente_texto = (tipo_fisico or "").upper().startswith("VARCHAR")

        n_tamanhos, largura_maxima, n_com_zero_a_esquerda = con.execute(
            f"""
            SELECT
                COUNT(DISTINCT LENGTH(CAST("{coluna}" AS VARCHAR))),
                MAX(LENGTH(CAST("{coluna}" AS VARCHAR))),
                COUNT(*) FILTER (
                    WHERE regexp_matches(CAST("{coluna}" AS VARCHAR), '^0[0-9]+$')
                )
            FROM "{tabela}" WHERE "{coluna}" IS NOT NULL
            """
        ).fetchone()

        # Prova direta (zero à esquerda perdido no cast) só vale quando a
        # coluna já era texto na origem: um DOUBLE como 0.72 ou 0.0 também
        # casa com "começa em 0 e tem mais de 1 dígito", mas nunca perdeu
        # zero à esquerda nenhum, porque nunca foi texto pra começo de conversa (A2).
        if eh_fisicamente_texto and n_com_zero_a_esquerda:
            return True

        seletividade = n_distinto / n_nao_nulo
        if n_tamanhos > TAMANHOS_DISTINTOS_MAXIMO_IDENTIFICADOR or seletividade < SELETIVIDADE_MINIMA_IDENTIFICADOR:
            return False

        # Sinal (b), formato: exige um mínimo de linhas (tabela pequena faz
        # quase qualquer coluna parecer "quase distinta") e, se a coluna não
        # for texto, uma largura mínima de dígitos (senão um inteiro curto
        # tipo idade vira Identifier só por coincidência de largura) (A3).
        if n_nao_nulo < QTD_MINIMA_LINHAS_PARA_SINAL_FORMATO_IDENTIFICADOR:
            return False

        return eh_fisicamente_texto or (largura_maxima is not None and largura_maxima >= LARGURA_MINIMA_IDENTIFICADOR_NUMERICO)

    def _parece_data_numerica(self, con, tabela: str, coluna: str) -> Optional[str]:
        """Detecta coluna NUMÉRICA que na verdade guarda uma data no formato
        AAAAMMDD (comum em exportações SAP, ex.: 20261021): sem isso, vira
        Numeric e entra num Pearson/média sem sentido, tipo 'média 2026-07-55',
        um dia que não existe (B6/achado 9 de docs/achados_tese.sql).
        Exige largura sempre igual a 8 dígitos e, decodificado como AAAAMMDD,
        parse bem-sucedido (ano/mês/dia plausíveis, STRPTIME já valida isso)
        em pelo menos LIMIAR_PARSE_NUMERICO_OU_DATA dos valores. Retorna a
        expressão SQL de cast pronta pra reusar em _estatistica_data, ou None."""
        n_nao_nulo, n_largura_8, n_parse_data_numerica = con.execute(
            f"""
            SELECT
                COUNT(*),
                COUNT(*) FILTER (WHERE LENGTH(CAST("{coluna}" AS VARCHAR)) = 8),
                COUNT(TRY_STRPTIME(CAST("{coluna}" AS VARCHAR), '%Y%m%d'))
            FROM "{tabela}" WHERE "{coluna}" IS NOT NULL
            """
        ).fetchone()

        if not n_nao_nulo or n_largura_8 != n_nao_nulo:
            return None  # alguma largura diferente de 8 dígitos: não é esse padrão

        if n_parse_data_numerica / n_nao_nulo < LIMIAR_PARSE_NUMERICO_OU_DATA:
            return None

        return f"TRY_STRPTIME(CAST(\"{coluna}\" AS VARCHAR), '%Y%m%d')"

    def _parece_apenas_hora(self, con, tabela: str, coluna: str, n_distinto: int) -> bool:
        """Coluna cujo componente de DATA é constante (ex.: sempre 1970-01-01,
        prática comum em exportação SAP pra campo HHMMSS guardado como
        timestamp): só a hora varia de verdade, então dia da semana e
        intervalo entre datas não fariam sentido (A6). Exige mais de um valor
        distinto no total, senão uma coluna de valor único trivialmente teria
        uma única data só por ter um único timestamp."""
        if n_distinto <= 1:
            return False

        (n_datas_distintas,) = con.execute(
            f"""
            SELECT COUNT(DISTINCT CAST(TRY_CAST("{coluna}" AS TIMESTAMP) AS DATE))
            FROM "{tabela}"
            WHERE "{coluna}" IS NOT NULL AND TRY_CAST("{coluna}" AS TIMESTAMP) IS NOT NULL
            """
        ).fetchone()

        return n_datas_distintas == 1

    def _moda_e_concentracao(self, con, tabela: str, coluna: str, n_nao_nulo: int) -> Dict[str, Any]:
        moda, freq = con.execute(
            f'SELECT "{coluna}", COUNT(*) AS freq FROM "{tabela}" WHERE "{coluna}" IS NOT NULL '
            f'GROUP BY "{coluna}" ORDER BY freq DESC LIMIT 1'
        ).fetchone()

        # Mesma CTE de contagem por valor que já pagávamos pelo Herfindahl: a
        # distribuição de frequências (min/mediana) sai de graça junto, sem
        # scan a mais. SUM((freq/n)²) é algebricamente igual ao SUM(p*p) de antes.
        (herfindahl, freq_minima, freq_mediana) = con.execute(
            f"""
            WITH contagens AS (
                SELECT COUNT(*) AS freq
                FROM "{tabela}" WHERE "{coluna}" IS NOT NULL GROUP BY "{coluna}"
            )
            SELECT
                SUM((freq::DOUBLE / {n_nao_nulo}) * (freq::DOUBLE / {n_nao_nulo})),
                MIN(freq),
                MEDIAN(freq)
            FROM contagens
            """
        ).fetchone()

        return {
            "moda": moda,
            "frequencia_moda": int(freq),
            "percentual_valor_mais_frequente": round(100 * freq / n_nao_nulo, 2),
            "indice_concentracao_categorica": round(float(herfindahl), 4),
            "frequencia_minima_valor": int(freq_minima),
            "frequencia_mediana_valor": float(freq_mediana),
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

    def _estatistica_data(
        self,
        con,
        tabela: str,
        coluna: str,
        n_nao_nulo: int,
        calcular_ciclo: bool = True,
        expressao_timestamp: Optional[str] = None,
    ) -> Dict[str, Any]:
        # Normalmente a própria coluna castada pra TIMESTAMP. Quando
        # _parece_data_numerica() detectou um AAAAMMDD (B6), vem uma expressão
        # de STRPTIME pronta pra decodificar o inteiro antes de qualquer conta.
        expr = expressao_timestamp or f'TRY_CAST("{coluna}" AS TIMESTAMP)'

        minimo, maximo, n_invalidas = con.execute(
            f"""
            SELECT
                MIN({expr}),
                MAX({expr}),
                COUNT(*) FILTER (WHERE "{coluna}" IS NOT NULL AND {expr} IS NULL)
            FROM "{tabela}"
            """
        ).fetchone()

        distribuicao_dia_semana: Dict[str, int] = {}
        intervalo_minimo = intervalo_mediano = intervalo_maximo = None

        if calcular_ciclo:
            # Distribuição por dia da semana: passada única, igual em custo às
            # outras métricas de coluna. GROUP BY 1 (posição), não pelo apelido
            # "dia": se a própria coluna analisada já se chamar "dia", o DuckDB
            # resolve o GROUP BY pela coluna original (a data) em vez do
            # apelido, e o dict final acaba só com a última data de cada dia
            # da semana (A1). Nomes traduzidos e reordenados em ISO
            # (segunda -> domingo) na hora de montar o dict, não na ordem
            # arbitrária que o GROUP BY devolveu (B11).
            contagem_por_dia_em_ingles = dict(
                con.execute(
                    f"""
                    SELECT dayname({expr}), COUNT(*)
                    FROM "{tabela}"
                    WHERE "{coluna}" IS NOT NULL AND {expr} IS NOT NULL
                    GROUP BY 1
                    """
                ).fetchall()
            )
            distribuicao_dia_semana = {
                dia_pt: int(contagem_por_dia_em_ingles[dia_en])
                for dia_en, dia_pt in MAPA_DIAS_SEMANA_PT.items()
                if dia_en in contagem_por_dia_em_ingles
            }
            distribuicao_dia_semana = {
                dia: distribuicao_dia_semana[dia] for dia in ORDEM_DIAS_SEMANA_PT if dia in distribuicao_dia_semana
            }

            # Intervalo entre datas DISTINTAS consecutivas: roda sobre dias
            # distintos (CAST ... AS DATE), não sobre timestamps distintos.
            # Senão, numa coluna com hora, dois instantes do mesmo dia já
            # contam como "datas distintas" com intervalo 0, zerando mínimo e
            # mediano (A7). Mais barato do que parece: anos de dados diários
            # ainda são só milhares de valores aqui, não milhões.
            intervalo_minimo, intervalo_mediano, intervalo_maximo = con.execute(
                f"""
                WITH datas AS (
                    SELECT DISTINCT CAST({expr} AS DATE) AS d
                    FROM "{tabela}" WHERE "{coluna}" IS NOT NULL AND {expr} IS NOT NULL
                ),
                ordenadas AS (
                    SELECT d, LAG(d) OVER (ORDER BY d) AS anterior FROM datas
                )
                SELECT
                    MIN(date_diff('day', anterior, d)),
                    MEDIAN(date_diff('day', anterior, d)),
                    MAX(date_diff('day', anterior, d))
                FROM ordenadas WHERE anterior IS NOT NULL
                """
            ).fetchone()

        return {
            "min": minimo,
            "max": maximo,
            "n_invalid_dates": n_invalidas,
            "p_invalid_dates": round(n_invalidas / n_nao_nulo, 6) if n_nao_nulo else 0.0,
            "distribuicao_dia_semana": distribuicao_dia_semana,
            "intervalo_minimo_dias": intervalo_minimo,
            "intervalo_mediano_dias": float(intervalo_mediano) if intervalo_mediano is not None else None,
            "intervalo_maximo_dias": intervalo_maximo,
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
        # Colunas 100% nulas (comum em exportação SAP) não geram alerta nem
        # aparecem em nenhum resumo hoje: essa contagem por base torna isso
        # visível sem precisar abrir coluna por coluna (B7).
        n_vars_totalmente_nulas = sum(
            1 for m in metricas_colunas.values() if m["qtd_linhas_total"] and m["qtd_nulos"] == m["qtd_linhas_total"]
        )
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
            "n_vars_totalmente_nulas": n_vars_totalmente_nulas,
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
    CARDINALIDADE_MAXIMA_PARA_CRAMERS_V = _teto_env("DQP_TETO_CARDINALIDADE_CRAMERS_V", 200)

    def _correlacoes(self, con, tabela: str, colunas: List[str], metricas_colunas: Dict[str, Any]) -> Dict[str, Any]:
        numericas = [c for c in colunas if metricas_colunas[c]["tipo_dado_inferido"] == "Numeric"]
        categoricas = [
            c
            for c in colunas
            # Identifier entra aqui também (B2): sem isso, uma coluna como
            # raiz_cnpj nunca aparece no Cramér's V, e por consequência nunca
            # é testada como candidata a dependência funcional. O teto de
            # cardinalidade abaixo já protege o custo, igual pra Categorical/Text.
            if metricas_colunas[c]["tipo_dado_inferido"] in ("Categorical", "Text", "Identifier")
            and metricas_colunas[c]["qtd_valores_distintos"] <= self.CARDINALIDADE_MAXIMA_PARA_CRAMERS_V
        ]

        colunas_ordem = numericas + categoricas
        if len(colunas_ordem) > QTD_MAXIMA_COLUNAS_PARA_ANALISE_DE_PARES:
            logger.warning(
                f"'{tabela}' tem {len(colunas_ordem)} colunas elegíveis para correlação "
                f"(acima do teto de {QTD_MAXIMA_COLUNAS_PARA_ANALISE_DE_PARES}): pulando a "
                f"matriz inteira pra não rodar um número de pares que cresce com o quadrado "
                f"da quantidade de colunas."
            )
            return {}

        registros = []

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
                    # CORR() de uma coluna sem variância (ex.: praticamente
                    # constante) devolve NaN, não None: sem essa guarda, NaN
                    # vaza pro data mart como se fosse um valor numérico de
                    # verdade (A4, mesmo cuidado de _padrao_ausencia).
                    linha[b] = round(float(valor), 4) if valor is not None and not math.isnan(valor) else None
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
    # Dependência funcional (origem -> destino): descoberta automática,
    # pré-filtrada pela correlação (Cramér's V) já calculada em _correlacoes.
    # ------------------------------------------------------------------ #

    def _dependencias_funcionais(
        self, con, tabela: str, metricas_colunas: Dict[str, Any], correlacoes: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        matriz = (correlacoes or {}).get("auto")
        if not matriz:
            return []

        colunas_ordem = list(matriz[0].keys())

        # Candidatos: só pares categórico x categórico (mesmo universo do
        # Cramér's V) cujo Cramér's V já calculado é alto o bastante pra
        # valer a pena o COUNT(DISTINCT) por grupo, que é a parte cara.
        # Testa as duas direções (a->b e b->a): dependência funcional é
        # direcional, o Cramér's V não distingue uma da outra.
        candidatos = []
        for i, origem in enumerate(colunas_ordem):
            if metricas_colunas[origem]["tipo_dado_inferido"] not in ("Categorical", "Text", "Identifier"):
                continue
            seletividade_origem = metricas_colunas[origem]["seletividade"]
            if seletividade_origem > SELETIVIDADE_MAXIMA_ORIGEM_PARA_FD:
                continue  # origem quase-única: FD cara de testar e trivial como achado

            for destino in colunas_ordem:
                if destino == origem or metricas_colunas[destino]["tipo_dado_inferido"] not in (
                    "Categorical", "Text", "Identifier",
                ):
                    continue
                valor_cramers_v = matriz[i].get(destino)
                if valor_cramers_v is None or valor_cramers_v < LIMIAR_CRAMERS_V_PARA_TESTAR_FD:
                    continue
                candidatos.append((valor_cramers_v, origem, destino))

        # Teto fixo final: ordena pelos pares mais fortemente associados e
        # desempata pelo número de valores distintos da origem (B4). Sem
        # isso, pares estruturais com V=1 mas só 2-3 grupos (ex.: um status
        # binário) podem tomar as vagas de pares bem mais ricos e informativos.
        candidatos.sort(
            key=lambda c: (c[0], metricas_colunas[c[1]]["qtd_valores_distintos"]),
            reverse=True,
        )
        candidatos = candidatos[:QTD_MAXIMA_PARES_TESTADOS_FD]

        achados = []
        for _, origem, destino in candidatos:
            n_grupos, n_grupos_sem_excecao, n_linhas_excecao, n_linhas_avaliadas = con.execute(
                f"""
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (WHERE qtd_distintos_destino = 1),
                    SUM(qtd_linhas) FILTER (WHERE qtd_distintos_destino > 1),
                    SUM(qtd_linhas)
                FROM (
                    SELECT "{origem}" AS v, COUNT(DISTINCT "{destino}") AS qtd_distintos_destino, COUNT(*) AS qtd_linhas
                    FROM "{tabela}" WHERE "{origem}" IS NOT NULL AND "{destino}" IS NOT NULL
                    GROUP BY "{origem}"
                ) t
                """
            ).fetchone()

            if not n_grupos or n_grupos < MINIMO_GRUPOS_ORIGEM_PARA_REPORTAR_FD:
                continue  # poucos grupos de origem: "dependência" quase trivial, pouco informativa (B3)

            aderencia = n_grupos_sem_excecao / n_grupos
            if aderencia < LIMIAR_ADERENCIA_PARA_REPORTAR_FD:
                continue

            achados.append({
                "origem": origem,
                "destino": destino,
                "qtd_grupos_origem": n_grupos,
                "percentual_grupos_sem_excecao": round(100 * aderencia, 2),
                "qtd_linhas_excecao": int(n_linhas_excecao or 0),
                # Quantas linhas de fato entraram na checagem (origem e
                # destino não nulos): sem isso, um achado batendo 100% em só
                # 5% das linhas da tabela parece "regra geral" sem ressalva (B3).
                "qtd_linhas_avaliadas": int(n_linhas_avaliadas or 0),
            })

        return achados

    # ------------------------------------------------------------------ #
    # Padrão de ausência: correlação entre indicadores de nulo (0/1)
    # ------------------------------------------------------------------ #

    def _padrao_ausencia(self, con, tabela: str, colunas: List[str], metricas_colunas: Dict[str, Any]) -> Dict[str, Any]:
        # Só colunas com nulo PARCIAL entram na matriz: numa coluna 100% nula
        # (ou 100% preenchida), o indicador de nulo é constante, e CORR() de
        # uma constante é NaN, não "correlação forte". NaN é um valor
        # NUMERIC válido no Postgres, maior que qualquer número de verdade, o
        # que faz um filtro tipo "WHERE correlacao_nulidade >= 0.9" devolver
        # exatamente essas linhas sem sentido como se fossem os pares mais
        # fortes (A4).
        com_nulo_parcial = [
            c
            for c in colunas
            if 0 < metricas_colunas[c]["qtd_nulos"] < metricas_colunas[c]["qtd_linhas_total"]
        ]

        if len(com_nulo_parcial) < 2:
            return {}

        registros = []
        for a in com_nulo_parcial:
            linha = {}
            for b in com_nulo_parcial:
                if a == b:
                    linha[b] = 1.0
                    continue
                (valor,) = con.execute(
                    f"""
                    SELECT CORR(CASE WHEN "{a}" IS NULL THEN 1.0 ELSE 0.0 END, CASE WHEN "{b}" IS NULL THEN 1.0 ELSE 0.0 END)
                    FROM "{tabela}"
                    """
                ).fetchone()
                linha[b] = round(float(valor), 4) if valor is not None and not math.isnan(valor) else None
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
