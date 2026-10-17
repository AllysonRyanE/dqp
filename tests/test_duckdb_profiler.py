"""
Testes do DuckDBProfiler contra um arquivo DuckDB local sintético (sem
precisar de nenhuma fonte externa: Trino/Postgres/etc. não entram aqui).
Cobre principalmente a inferência de tipo (_inferir_tipo), que é a base de
praticamente toda regra de qualidade e de alerta do dqp.
"""

import duckdb
import pytest

from profilers.duckdb_profiler import DuckDBProfiler


@pytest.fixture
def caminho_duckdb(tmp_path):
    caminho = str(tmp_path / "teste.duckdb")
    con = duckdb.connect(caminho)
    con.execute(
        """
        CREATE TABLE pessoas (
            cpf VARCHAR,       -- texto, mas 100% numérico (caso real: CPF sem máscara)
            uf VARCHAR,        -- texto categórico de verdade
            nascimento VARCHAR, -- texto, mas 100% parseável como data
            idade BIGINT,      -- já numérico de verdade
            observacao VARCHAR -- maioria nula
        )
        """
    )
    ufs = ["SP", "RJ", "MG", "BA"]
    for i in range(200):
        cpf = str(10000000000 + i)
        uf = ufs[i % len(ufs)]
        nascimento = f"19{70 + i % 30}-0{1 + i % 9}-10"
        observacao = "ok" if i % 20 == 0 else None
        con.execute(
            "INSERT INTO pessoas VALUES (?, ?, ?, ?, ?)",
            [cpf, uf, nascimento, 20 + i % 50, observacao],
        )
    con.close()
    return caminho


def test_coluna_texto_100_por_cento_numerica_vira_numeric(caminho_duckdb):
    full = DuckDBProfiler(caminho_duckdb).profile_full("pessoas")
    cpf = full["colunas"]["cpf"]
    assert cpf["tipo_dado_inferido"] == "Numeric"
    assert cpf["tipo_fisico_duckdb"] == "VARCHAR"
    assert cpf["percentual_parse_numerico"] == 100.0


def test_coluna_categorica_de_verdade_nao_vira_numeric(caminho_duckdb):
    full = DuckDBProfiler(caminho_duckdb).profile_full("pessoas")
    uf = full["colunas"]["uf"]
    assert uf["tipo_dado_inferido"] == "Categorical"
    assert uf["percentual_parse_numerico"] == 0.0


def test_coluna_texto_100_por_cento_data_vira_datetime(caminho_duckdb):
    full = DuckDBProfiler(caminho_duckdb).profile_full("pessoas")
    nascimento = full["colunas"]["nascimento"]
    assert nascimento["tipo_dado_inferido"] == "DateTime"
    assert nascimento["tipo_fisico_duckdb"] == "VARCHAR"


def test_coluna_ja_numerica_fisicamente(caminho_duckdb):
    full = DuckDBProfiler(caminho_duckdb).profile_full("pessoas")
    idade = full["colunas"]["idade"]
    assert idade["tipo_dado_inferido"] == "Numeric"
    assert idade["tipo_fisico_duckdb"] == "BIGINT"


def test_coluna_majoritariamente_nula(caminho_duckdb):
    full = DuckDBProfiler(caminho_duckdb).profile_full("pessoas")
    observacao = full["colunas"]["observacao"]
    assert observacao["qtd_nulos"] == 190
    assert observacao["percentual_nulos"] == pytest.approx(95.0, abs=0.1)
