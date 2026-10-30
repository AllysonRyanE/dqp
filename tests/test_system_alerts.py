"""
Testes de gerar_alertas_automaticos a partir de dicts de métricas sintéticos
(mesmo formato que DuckDBProfiler.profile_full produz), não depende de
banco nenhum.
"""

from quality.system_alerts import SEPARADOR_EVIDENCIA_CONCLUSAO, gerar_alertas_automaticos


def _full(colunas, correlacoes=None):
    return {"colunas": colunas, "correlacoes": correlacoes or {}}


def _codigos(alertas):
    return {a["codigo"] for a in alertas}


def test_cardinalidade_muito_alta_dispara_para_quase_tudo_distinto():
    full = _full({
        "id": {
            "tipo_dado_inferido": "Numeric",
            "qtd_linhas_total": 100,
            "seletividade": 0.99,
        }
    })
    assert "cardinalidade_muito_alta" in _codigos(gerar_alertas_automaticos(full))


def test_tipo_fisico_divergente_dispara_para_texto_100_por_cento_numerico():
    full = _full({
        "cpf": {
            "tipo_dado_inferido": "Numeric",
            "tipo_fisico_duckdb": "VARCHAR",
            "percentual_parse_numerico": 100.0,
            "percentual_parse_data": 0.0,
            "qtd_linhas_total": 100,
            "seletividade": 0.5,
        }
    })
    alertas = gerar_alertas_automaticos(full)
    assert "tipo_fisico_divergente" in _codigos(alertas)


def test_tipo_fisico_divergente_nao_dispara_para_categorico_real():
    full = _full({
        "uf": {
            "tipo_dado_inferido": "Categorical",
            "tipo_fisico_duckdb": "VARCHAR",
            "percentual_parse_numerico": 0.0,
            "percentual_parse_data": 0.0,
            "qtd_linhas_total": 100,
            "seletividade": 0.04,
        }
    })
    assert "tipo_fisico_divergente" not in _codigos(gerar_alertas_automaticos(full))


def test_tipo_fisico_divergente_nao_dispara_quando_ja_e_numerico_fisicamente():
    full = _full({
        "idade": {
            "tipo_dado_inferido": "Numeric",
            "tipo_fisico_duckdb": "BIGINT",
            "percentual_parse_numerico": 100.0,
            "percentual_parse_data": 0.0,
            "qtd_linhas_total": 100,
            "seletividade": 0.3,
        }
    })
    assert "tipo_fisico_divergente" not in _codigos(gerar_alertas_automaticos(full))


def test_distorcao_estatistica_dispara_acima_do_limiar():
    full = _full({
        "valor": {
            "tipo_dado_inferido": "Numeric",
            "skewness": 3.5,
            "qtd_linhas_total": 100,
            "seletividade": 0.5,
        }
    })
    assert "distorcao_estatistica" in _codigos(gerar_alertas_automaticos(full))


def test_correlacao_alta_dispara_entre_duas_colunas_numericas():
    colunas = {
        "a": {"tipo_dado_inferido": "Numeric", "qtd_linhas_total": 100, "seletividade": 0.5},
        "b": {"tipo_dado_inferido": "Numeric", "qtd_linhas_total": 100, "seletividade": 0.5},
    }
    correlacoes = {"auto": [{"a": 1.0, "b": 0.95}, {"a": 0.95, "b": 1.0}]}
    full = _full(colunas, correlacoes)
    alertas = gerar_alertas_automaticos(full)
    correlacao = next(a for a in alertas if a["codigo"] == "correlacao_alta")
    assert "Pearson" in correlacao["mensagem"]


def test_mensagem_separa_evidencia_e_conclusao():
    full = _full({
        "id": {"tipo_dado_inferido": "Numeric", "qtd_linhas_total": 100, "seletividade": 0.99}
    })
    alerta = gerar_alertas_automaticos(full)[0]
    assert SEPARADOR_EVIDENCIA_CONCLUSAO in alerta["mensagem"]


def test_dependencia_funcional_com_excecao_gera_alerta():
    full = _full({})
    full["dependencias_funcionais"] = [
        {"origem": "produto", "destino": "unidade", "qtd_grupos_origem": 10,
         "percentual_grupos_sem_excecao": 97.0, "qtd_linhas_excecao": 3, "qtd_linhas_avaliadas": 500},
    ]
    alertas = gerar_alertas_automaticos(full)
    assert "dependencia_funcional_com_excecoes" in _codigos(alertas)


def test_dependencia_funcional_sem_excecao_nao_gera_alerta():
    """100% de aderência (0 exceções) é o caso comum e esperado: não é achado
    que precise de alerta, só o caso com exceção de verdade é que vale a pena."""
    full = _full({})
    full["dependencias_funcionais"] = [
        {"origem": "produto", "destino": "loja", "qtd_grupos_origem": 4,
         "percentual_grupos_sem_excecao": 100.0, "qtd_linhas_excecao": 0, "qtd_linhas_avaliadas": 100},
    ]
    alertas = gerar_alertas_automaticos(full)
    assert "dependencia_funcional_com_excecoes" not in _codigos(alertas)
