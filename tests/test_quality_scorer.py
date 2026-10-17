"""
Testes puros do QualityScorer (sem DuckDB, sem banco nenhum): as regras de
pontuação em si, a partir de dicts de métricas já calculados.
"""

import pytest

from quality.quality_scorer import QualityScorer


@pytest.fixture
def scorer():
    return QualityScorer()


def test_consistencia_datetime_usa_percentual_invalidas(scorer):
    metrics = {"tipo_dado_inferido": "DateTime", "percentual_datas_invalidas": 10.0}
    assert scorer.calculate_consistency_score(metrics) == 90.0


def test_consistencia_categorical_100_por_cento_parseavel_nao_penaliza(scorer):
    # Caso CPF: texto 100% parseável como número não é "misturado", é consistente.
    metrics = {
        "tipo_dado_inferido": "Categorical",
        "percentual_parse_numerico": 100.0,
        "percentual_parse_data": 0.0,
    }
    assert scorer.calculate_consistency_score(metrics) == 100.0


def test_consistencia_categorical_mistura_50_50_e_o_pior_caso(scorer):
    metrics = {
        "tipo_dado_inferido": "Categorical",
        "percentual_parse_numerico": 50.0,
        "percentual_parse_data": 0.0,
    }
    assert scorer.calculate_consistency_score(metrics) == 0.0


def test_consistencia_nao_se_aplica_a_numeric(scorer):
    metrics = {"tipo_dado_inferido": "Numeric"}
    assert scorer.calculate_consistency_score(metrics) is None


def test_completude_com_tolerancia_de_nulidade(scorer):
    metrics = {"percentual_nulos": 30.0}
    assert scorer.calculate_completeness_score(metrics, tolerancia_nulidade=30.0) == 100.0
    assert scorer.calculate_completeness_score(metrics, tolerancia_nulidade=10.0) == 80.0


def test_overall_score_redistribui_peso_quando_dimensao_nao_se_aplica(scorer):
    # Só completude (peso 6/20) e acurácia (peso 4/20) se aplicam aqui.
    # O peso de consistência/conformidade é redistribuído, não descartado
    # como um 100 "de mentirinha".
    resultado = scorer.calculate_overall_score(
        score_completude=80.0,
        score_consistencia=None,
        score_acuracia=100.0,
        score_conformidade_regras=None,
    )
    assert resultado == pytest.approx(88.0)


def test_classificacao_por_faixa(scorer):
    assert scorer.classify(90.0) == "Ótimo"
    assert scorer.classify(75.0) == "Regular"
    assert scorer.classify(50.0) == "Crítico"
