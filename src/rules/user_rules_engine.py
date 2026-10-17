"""
Motor de regras de negócio configuradas pelo usuário (origem='usuario' em
fato_regra_aplicada). Roda contra o mesmo arquivo DuckDB que o DuckDBProfiler
usa: não depende do ydata-profiling nem do pandas.

Combina Great Expectations (regras de coluna única/composta, onde ele é a
ferramenta certa) com SQL direto (regras entre tabelas ou cálculos
específicos, onde GE fica artificial demais). Great Expectations valida
direto contra o SQLAlchemy engine via `duckdb_engine`, sem carregar nada em
pandas: mesmo princípio de "sem amostragem" do DuckDBProfiler.

Contrato de `parametros` por código de regra (dim_regra.codigo):
    valores_distintos_entre  {"coluna": str, "minimo": int, "maximo": int}
    comprimento_maximo       {"coluna": str, "maximo": int}
    chave_composta_unica     {"colunas": [str, ...]}
    soma_colunas             {"coluna_soma": str, "colunas_parcelas": [str, ...], "tolerancia": float=0.01}
    sem_negativos            {"coluna": str}
    intervalo_numerico       {"coluna": str, "minimo": float|None, "maximo": float|None}
    intervalo_data           {"coluna": str, "minimo": str|None, "maximo": str|None}  (ISO 8601)
    valores_aceitos          {"coluna": str, "valores": [str, ...]}
    valor_unico              {"coluna": str}
    formato_regex            {"coluna": str, "regex": str}
    existe_em_outra_tabela   {"coluna": str, "tabela_referencia": str, "coluna_referencia": str}
    completude_condicional   {"coluna_condicionante": str, "coluna_dependente": str}
    nulidade_aceitavel       {"coluna": str, "tolerancia_percentual": float}: única regra que também
                             altera o cálculo de score_completude (ver quality_scorer.py); as outras 11
                             só registram passou/falhou, essa muda a fórmula da dimensão.

Cada regra de entrada é um dict {"sk_config_regra": int, "codigo": str, "parametros": dict}.
Cada resultado de saída é um dict pronto pra virar uma linha de fato_regra_aplicada:
{"sk_config_regra", "codigo", "colunas_envolvidas", "passou", "percentual_conformidade",
 "qtd_violacoes", "mensagem", "parametros"}.
"""

import gc
from typing import Any, Dict, List, Optional

import great_expectations as gx
import great_expectations.execution_engine.sqlalchemy_execution_engine as _ge_sql_engine
from loguru import logger
from sqlalchemy import text


class _DuckDBDialectMarker:
    """GE 1.19.1 fecha a conexão SQLAlchemy assim que uma query termina
    (`SqlAlchemyExecutionEngine.get_connection`): funciona pra dialetos que
    bufferizam o resultado inteiro no client (psycopg2, etc), mas o driver
    Python do DuckDB invalida o cursor quando a conexão-mãe fecha, então
    `resolve_metric_bundle` recebe uma lista vazia e explode com
    `IndexError: list index out of range` mesmo pra métricas triviais como
    `table.row_count`. GE já tem uma lista de dialetos que usam conexão
    persistida (`_PERSISTED_CONNECTION_DIALECTS`, ex: sqlite, mssql): só que
    "duckdb" nunca foi adicionado lá (não existe nem como GXSqlDialect). Esse
    marcador imita um membro do enum (precisa de `.value` e de comparar por
    igualdade com a string "duckdb") pra entrar nessa lista via monkeypatch,
    sem precisar (nem poder) estender o Enum do GE diretamente."""

    value = "duckdb"

    def __eq__(self, other):
        return other == "duckdb"

    def __hash__(self):
        return hash("duckdb")


if "duckdb" not in _ge_sql_engine._PERSISTED_CONNECTION_DIALECTS:
    _ge_sql_engine._PERSISTED_CONNECTION_DIALECTS = _ge_sql_engine._PERSISTED_CONNECTION_DIALECTS + (
        _DuckDBDialectMarker(),
    )


class _MotorGE:
    """Contexto GE efêmero + cache de batch definitions por tabela/view: evita
    re-registrar o mesmo data asset a cada regra avaliada na mesma tabela.

    Também expõe `self.engine` (a mesma engine SQLAlchemy/duckdb_engine que o
    GE usa) pra rodar SQL direto nas regras que não passam por GE: o DuckDB
    não permite abrir uma segunda conexão pro mesmo arquivo com configuração
    diferente da conexão do GE, então tudo (GE e SQL cru) passa pela mesma engine."""

    def __init__(self, caminho_duckdb: str):
        self.context = gx.get_context(mode="ephemeral")
        self.data_source = self.context.data_sources.add_sql(
            name="dqp_duckdb", connection_string=f"duckdb:///{caminho_duckdb}"
        )
        self.engine = self.data_source.get_engine()
        self._batch_defs: Dict[str, Any] = {}

    def batch(self, nome_tabela_ou_view: str):
        if nome_tabela_ou_view not in self._batch_defs:
            asset = self.data_source.add_table_asset(name=nome_tabela_ou_view, table_name=nome_tabela_ou_view)
            self._batch_defs[nome_tabela_ou_view] = asset.add_batch_definition_whole_table(name="lote_completo")
        return self._batch_defs[nome_tabela_ou_view].get_batch()

    def executar(self, sql: str, params: Optional[Dict[str, Any]] = None):
        with self.engine.begin() as conn:
            return conn.execute(text(sql), params or {}).fetchone()

    def encerrar(self):
        """Fecha a conexão persistida que o GE mantém internamente pro DuckDB
        (ver _DuckDBDialectMarker): sem isso, `engine.dispose()` sozinho não
        libera o arquivo e a próxima tabela não consegue abrir uma conexão nova."""
        execution_engine = self.data_source.get_execution_engine()
        conexao_persistida = getattr(execution_engine, "_connection", None)
        if conexao_persistida is not None:
            conexao_persistida.close()
            execution_engine._connection = None
        self.engine.dispose()


def _empacotar_ge_linha(regra: Dict[str, Any], colunas_envolvidas: str, resultado, mensagem_falha: str) -> Dict[str, Any]:
    """Pra expectations 'linha a linha' (unexpected_count/element_count no result)."""
    r = resultado.result or {}
    total = r.get("element_count") or 0
    violacoes = r.get("unexpected_count") or 0
    conformidade = round(100 * (1 - violacoes / total), 3) if total else 100.0
    return {
        "sk_config_regra": regra["sk_config_regra"],
        "codigo": regra["codigo"],
        "colunas_envolvidas": colunas_envolvidas,
        "passou": bool(resultado.success),
        "percentual_conformidade": conformidade,
        "qtd_violacoes": violacoes,
        "mensagem": mensagem_falha if not resultado.success else "OK: regra cumprida.",
        "parametros": regra["parametros"],
    }


def _empacotar_ge_agregado(regra: Dict[str, Any], colunas_envolvidas: str, resultado, mensagem_falha: str) -> Dict[str, Any]:
    """Pra expectations 'de tabela' (ex: contagem de distintos): resultado é binário,
    não tem violação linha a linha, só um observed_value."""
    valor_observado = (resultado.result or {}).get("observed_value")
    return {
        "sk_config_regra": regra["sk_config_regra"],
        "codigo": regra["codigo"],
        "colunas_envolvidas": colunas_envolvidas,
        "passou": bool(resultado.success),
        "percentual_conformidade": 100.0 if resultado.success else 0.0,
        "qtd_violacoes": 0 if resultado.success else 1,
        "mensagem": (
            f"OK: valor observado: {valor_observado}."
            if resultado.success
            else f"{mensagem_falha} (valor observado: {valor_observado})"
        ),
        "parametros": regra["parametros"],
    }


def _empacotar_sql(regra: Dict[str, Any], colunas_envolvidas: str, violacoes: int, total: int, mensagem_falha: str) -> Dict[str, Any]:
    conformidade = round(100 * (1 - violacoes / total), 3) if total else 100.0
    passou = violacoes == 0
    return {
        "sk_config_regra": regra["sk_config_regra"],
        "codigo": regra["codigo"],
        "colunas_envolvidas": colunas_envolvidas,
        "passou": passou,
        "percentual_conformidade": conformidade,
        "qtd_violacoes": violacoes,
        "mensagem": mensagem_falha if not passou else "OK: regra cumprida.",
        "parametros": regra["parametros"],
    }


def _resultado_erro(regra: Dict[str, Any], colunas_envolvidas: str, erro: str) -> Dict[str, Any]:
    return {
        "sk_config_regra": regra["sk_config_regra"],
        "codigo": regra["codigo"],
        "colunas_envolvidas": colunas_envolvidas,
        "passou": False,
        "percentual_conformidade": None,
        "qtd_violacoes": None,
        "mensagem": f"Erro ao avaliar a regra: {erro}",
        "parametros": regra["parametros"],
    }


# ------------------------------------------------------------------ #
# Regras via Great Expectations (colunas em VARCHAR, sem cast: a
# semântica da regra não depende do tipo numérico/data real).
# ------------------------------------------------------------------ #

def _valores_distintos_entre(tabela, p, regra, motor_ge):
    coluna = p["coluna"]
    batch = motor_ge.batch(tabela)
    resultado = batch.validate(
        gx.expectations.ExpectColumnUniqueValueCountToBeBetween(column=coluna, min_value=p["minimo"], max_value=p["maximo"])
    )
    return _empacotar_ge_agregado(
        regra, coluna, resultado,
        f"Quantidade de valores distintos de '{coluna}' fora do intervalo [{p['minimo']}, {p['maximo']}].",
    )


def _comprimento_maximo(tabela, p, regra, motor_ge):
    coluna = p["coluna"]
    batch = motor_ge.batch(tabela)
    resultado = batch.validate(gx.expectations.ExpectColumnValueLengthsToBeBetween(column=coluna, min_value=0, max_value=p["maximo"]))
    return _empacotar_ge_linha(regra, coluna, resultado, f"Valores de '{coluna}' passam de {p['maximo']} caracteres.")


def _chave_composta_unica(tabela, p, regra, motor_ge):
    colunas = p["colunas"]
    batch = motor_ge.batch(tabela)
    resultado = batch.validate(gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=colunas))
    return _empacotar_ge_linha(regra, ",".join(colunas), resultado, f"Combinação de {colunas} tem valores repetidos.")


def _valores_aceitos(tabela, p, regra, motor_ge):
    coluna = p["coluna"]
    batch = motor_ge.batch(tabela)
    resultado = batch.validate(gx.expectations.ExpectColumnValuesToBeInSet(column=coluna, value_set=p["valores"]))
    return _empacotar_ge_linha(regra, coluna, resultado, f"Valores fora da lista aceita em '{coluna}'.")


def _valor_unico(tabela, p, regra, motor_ge):
    coluna = p["coluna"]
    batch = motor_ge.batch(tabela)
    resultado = batch.validate(gx.expectations.ExpectColumnValuesToBeUnique(column=coluna))
    return _empacotar_ge_linha(regra, coluna, resultado, f"Valores repetidos encontrados em '{coluna}'.")


# ------------------------------------------------------------------ #
# Regras via Great Expectations sobre uma view com TRY_CAST (a
# semântica é numérica/de data: precisa comparar valores, não texto).
# ------------------------------------------------------------------ #

def _view_com_cast(motor_ge, tabela: str, coluna: str, tipo_sql: str, sufixo: str) -> str:
    """View PERSISTENTE (não TEMP): views TEMP são isoladas por conexão e o GE
    só enxergaria uma criada na mesma sessão. Fica no catálogo do arquivo,
    dropada no final de `avaliar_regras`.

    Guard: se a coluna não é do tipo certo (ex: regra numérica configurada
    numa coluna de texto), TRY_CAST vira NULL pra tudo: sem esse check, o
    GE conta `element_count=0` e a expectation "passa" por vacuidade (falso
    positivo, já visto na prática). Em vez disso, erra explicitamente."""
    nome_view = f"__cast_{tabela}_{coluna}_{sufixo}"
    motor_ge.executar(f'CREATE OR REPLACE VIEW "{nome_view}" AS SELECT *, TRY_CAST("{coluna}" AS {tipo_sql}) AS valor_cast FROM "{tabela}"')

    falhas_cast, total_nao_nulos = motor_ge.executar(
        f'SELECT COUNT(*) FILTER (WHERE "{coluna}" IS NOT NULL AND valor_cast IS NULL), '
        f'COUNT(*) FILTER (WHERE "{coluna}" IS NOT NULL) FROM "{nome_view}"'
    )
    if total_nao_nulos and falhas_cast == total_nao_nulos:
        raise ValueError(
            f"coluna '{coluna}' não converte pra {tipo_sql} em nenhuma linha não-nula: "
            f"tipo errado pra essa regra (confira se a coluna é mesmo numérica/data)"
        )

    return nome_view


def _sem_negativos(tabela, p, regra, motor_ge):
    coluna = p["coluna"]
    nome_view = _view_com_cast(motor_ge, tabela, coluna, "DOUBLE", "num")
    batch = motor_ge.batch(nome_view)
    resultado = batch.validate(gx.expectations.ExpectColumnValuesToBeBetween(column="valor_cast", min_value=0))
    return _empacotar_ge_linha(regra, coluna, resultado, f"Valores negativos encontrados em '{coluna}'.")


def _intervalo_numerico(tabela, p, regra, motor_ge):
    coluna = p["coluna"]
    nome_view = _view_com_cast(motor_ge, tabela, coluna, "DOUBLE", "num")
    batch = motor_ge.batch(nome_view)
    resultado = batch.validate(
        gx.expectations.ExpectColumnValuesToBeBetween(column="valor_cast", min_value=p.get("minimo"), max_value=p.get("maximo"))
    )
    return _empacotar_ge_linha(
        regra, coluna, resultado, f"Valores de '{coluna}' fora do intervalo [{p.get('minimo')}, {p.get('maximo')}]."
    )


def _intervalo_data(tabela, p, regra, motor_ge):
    coluna = p["coluna"]
    nome_view = _view_com_cast(motor_ge, tabela, coluna, "DATE", "data")
    batch = motor_ge.batch(nome_view)
    resultado = batch.validate(
        gx.expectations.ExpectColumnValuesToBeBetween(column="valor_cast", min_value=p.get("minimo"), max_value=p.get("maximo"))
    )
    return _empacotar_ge_linha(
        regra, coluna, resultado, f"Datas de '{coluna}' fora do intervalo [{p.get('minimo')}, {p.get('maximo')}]."
    )


# ------------------------------------------------------------------ #
# Regras via SQL direto (multi-tabela ou cálculo específico: GE
# ficaria artificial ou exigiria carregar tudo em pandas de qualquer jeito).
# ------------------------------------------------------------------ #

def _formato_regex(tabela, p, regra, motor_ge):
    """GE 1.19.1 não tem suporte de regex pro dialeto duckdb (a metric
    'column_values.match_regex.condition' nem tenta traduzir a regex pra SQL
    nesse dialeto, e o próprio aviso de 'dialeto não suportado' quebra com um
    AttributeError interno do GE): validado empiricamente, não é algo que dê
    pra contornar sem tocar no código do GE. O DuckDB tem `regexp_matches`
    nativo, então isso vira SQL direto em vez de uma expectation."""
    coluna = p["coluna"]
    violacoes, total = motor_ge.executar(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE NOT regexp_matches("{coluna}", :regex)),
            COUNT(*)
        FROM "{tabela}"
        WHERE "{coluna}" IS NOT NULL
        """,
        {"regex": p["regex"]},
    )
    return _empacotar_sql(regra, coluna, violacoes, total, f"Valores de '{coluna}' não batem com o padrão esperado.")


def _verificar_colunas_numericas(motor_ge, tabela: str, colunas: List[str]) -> None:
    """Mesma lógica do guard de _view_com_cast, mas pra colunas onde o cast é
    inline (soma_colunas usa COALESCE(...,0), que mascararia silenciosamente
    uma coluna do tipo errado como zero em toda linha)."""
    for coluna in colunas:
        falhas_cast, total_nao_nulos = motor_ge.executar(
            f'SELECT COUNT(*) FILTER (WHERE "{coluna}" IS NOT NULL AND TRY_CAST("{coluna}" AS DOUBLE) IS NULL), '
            f'COUNT(*) FILTER (WHERE "{coluna}" IS NOT NULL) FROM "{tabela}"'
        )
        if total_nao_nulos and falhas_cast == total_nao_nulos:
            raise ValueError(f"coluna '{coluna}' não converte pra número em nenhuma linha não-nula: tipo errado pra essa regra")


def _soma_colunas(tabela, p, regra, motor_ge):
    coluna_soma = p["coluna_soma"]
    parcelas = p["colunas_parcelas"]
    tolerancia = p.get("tolerancia", 0.01)
    _verificar_colunas_numericas(motor_ge, tabela, [coluna_soma] + parcelas)
    soma_sql = " + ".join(f'COALESCE(TRY_CAST("{c}" AS DOUBLE), 0)' for c in parcelas)
    violacoes, total = motor_ge.executar(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE ABS(TRY_CAST("{coluna_soma}" AS DOUBLE) - ({soma_sql})) > {tolerancia}),
            COUNT(*)
        FROM "{tabela}"
        WHERE "{coluna_soma}" IS NOT NULL
        """
    )
    colunas_envolvidas = ",".join([coluna_soma] + parcelas)
    return _empacotar_sql(
        regra, colunas_envolvidas, violacoes, total,
        f"'{coluna_soma}' não bate com a soma de {parcelas} em {violacoes} linha(s).",
    )


def _existe_em_outra_tabela(tabela, p, regra, motor_ge):
    coluna = p["coluna"]
    tabela_referencia = p["tabela_referencia"]
    coluna_referencia = p["coluna_referencia"]
    violacoes, total = motor_ge.executar(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE t."{coluna}" IS NOT NULL AND t."{coluna}" NOT IN (SELECT "{coluna_referencia}" FROM "{tabela_referencia}")),
            COUNT(*) FILTER (WHERE t."{coluna}" IS NOT NULL)
        FROM "{tabela}" t
        """
    )
    return _empacotar_sql(
        regra, coluna, violacoes, total,
        f"{violacoes} valor(es) de '{coluna}' não existem em {tabela_referencia}.{coluna_referencia}.",
    )


def _completude_condicional(tabela, p, regra, motor_ge):
    coluna_condicionante = p["coluna_condicionante"]
    coluna_dependente = p["coluna_dependente"]
    violacoes, total = motor_ge.executar(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE "{coluna_condicionante}" IS NOT NULL AND "{coluna_dependente}" IS NULL),
            COUNT(*) FILTER (WHERE "{coluna_condicionante}" IS NOT NULL)
        FROM "{tabela}"
        """
    )
    return _empacotar_sql(
        regra, f"{coluna_condicionante},{coluna_dependente}", violacoes, total,
        f"{violacoes} linha(s) com '{coluna_condicionante}' preenchida mas '{coluna_dependente}' vazia.",
    )


def _nulidade_aceitavel(tabela, p, regra, motor_ge):
    """Diferente das outras 11: além de registrar passou/falhou aqui (pra
    aparecer nos relatórios/alertas como as demais), o valor de tolerância
    também é lido direto de config_regra_usuario pelo orquestrador
    (persistir_datamart_duckdb.py) e repassado pro QualityScorer, que deixa
    de penalizar score_completude até esse limite: ver
    calculate_completeness_score em quality_scorer.py. Essa função aqui só
    cuida do registro/relato; quem muda o score é o scorer."""
    coluna = p["coluna"]
    tolerancia = p["tolerancia_percentual"]
    nulos, total = motor_ge.executar(f'SELECT COUNT(*) FILTER (WHERE "{coluna}" IS NULL), COUNT(*) FROM "{tabela}"')
    percentual_nulos = round(100 * nulos / total, 3) if total else 0.0
    excesso = max(0.0, percentual_nulos - tolerancia)
    passou = excesso == 0.0
    return {
        "sk_config_regra": regra["sk_config_regra"],
        "codigo": regra["codigo"],
        "colunas_envolvidas": coluna,
        "passou": passou,
        "percentual_conformidade": round(100 - excesso, 3),
        "qtd_violacoes": nulos if not passou else 0,
        "mensagem": (
            f"{percentual_nulos}% nulos, dentro da tolerância de {tolerancia}%."
            if passou else
            f"{percentual_nulos}% nulos, {excesso:.3f} pontos além da tolerância de {tolerancia}%."
        ),
        "parametros": regra["parametros"],
    }


_DISPATCH = {
    "valores_distintos_entre": _valores_distintos_entre,
    "comprimento_maximo": _comprimento_maximo,
    "chave_composta_unica": _chave_composta_unica,
    "soma_colunas": _soma_colunas,
    "sem_negativos": _sem_negativos,
    "intervalo_numerico": _intervalo_numerico,
    "intervalo_data": _intervalo_data,
    "valores_aceitos": _valores_aceitos,
    "valor_unico": _valor_unico,
    "formato_regex": _formato_regex,
    "existe_em_outra_tabela": _existe_em_outra_tabela,
    "completude_condicional": _completude_condicional,
    "nulidade_aceitavel": _nulidade_aceitavel,
}


def avaliar_regras(caminho_duckdb: str, tabela: str, regras: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    `regras`: lista de {"sk_config_regra": int, "codigo": str, "parametros": dict}
    (formato de config_regra_usuario, já filtrado por sk_base e ativa=TRUE).
    """
    if not regras:
        return []

    motor_ge = _MotorGE(caminho_duckdb)
    resultados = []
    views_criadas: List[str] = []

    for regra in regras:
        codigo = regra["codigo"]
        fn = _DISPATCH.get(codigo)
        if fn is None:
            logger.warning(f"Código de regra de usuário desconhecido: {codigo}")
            continue
        try:
            if codigo in ("sem_negativos", "intervalo_numerico", "intervalo_data"):
                coluna = regra["parametros"]["coluna"]
                sufixo = "data" if codigo == "intervalo_data" else "num"
                views_criadas.append(f"__cast_{tabela}_{coluna}_{sufixo}")
            resultados.append(fn(tabela, regra["parametros"], regra, motor_ge))
        except Exception as e:
            logger.warning(f"Falha ao avaliar regra '{codigo}' (sk_config_regra={regra['sk_config_regra']}): {e}")
            colunas = regra["parametros"].get("coluna") or str(regra["parametros"])
            resultados.append(_resultado_erro(regra, colunas, str(e)))

    for nome_view in views_criadas:
        motor_ge.executar(f'DROP VIEW IF EXISTS "{nome_view}"')

    motor_ge.encerrar()
    del motor_ge
    # O GE monta o SQLAlchemy Engine/Connection num ciclo de referências (engine
    # <-> pool <-> connection <-> dialect) que o refcounting normal do CPython não
    # derruba na hora: sem isso, o handle do arquivo DuckDB fica preso até o GC
    # cíclico rodar sozinho (não determinístico), e a próxima tabela não consegue
    # abrir uma conexão nova pro mesmo arquivo.
    gc.collect()
    return resultados
