"""
ETL para persistir resultados de perfilamento no data mart dimensional local (Postgres).
"""

import json
import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from loguru import logger
from sqlalchemy import create_engine, text

from quality.system_alerts import gerar_alertas_automaticos


def _json_safe(valor: Any) -> Any:
    """NaN/Infinity não são JSON válido (o Postgres rejeita). Troca por null."""
    if isinstance(valor, float) and (math.isnan(valor) or math.isinf(valor)):
        return None
    if isinstance(valor, dict):
        return {k: _json_safe(v) for k, v in valor.items()}
    if isinstance(valor, list):
        return [_json_safe(v) for v in valor]
    return valor


def dumps(valor: Any) -> str:
    return json.dumps(_json_safe(valor), ensure_ascii=False, default=str)


class DataMartLoader:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        self._mapa_regra_tipo: Optional[Dict[str, int]] = None

    def _get_mapa_regra_tipo(self, conn) -> Dict[str, int]:
        if self._mapa_regra_tipo is None:
            linhas = conn.execute(text("SELECT codigo, sk_regra_tipo FROM dim_regra")).fetchall()
            self._mapa_regra_tipo = {codigo: sk for codigo, sk in linhas}
        return self._mapa_regra_tipo

    def buscar_regras_usuario_ativas(
        self, nome_base: str, sistema_origem: str, catalogo: str, schema_origem: str
    ) -> List[Dict[str, Any]]:
        """Regras que o usuário configurou (config_regra_usuario, ativa=TRUE) pra essa
        base: vazio se a base ainda não existe (primeira execução) ou não tem regras."""
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT sk_base FROM dim_base
                    WHERE sistema_origem = :sistema AND COALESCE(catalogo, '') = COALESCE(:catalogo, '')
                      AND COALESCE(schema_origem, '') = COALESCE(:schema, '') AND nome_base = :nome
                    """
                ),
                {"sistema": sistema_origem, "catalogo": catalogo, "schema": schema_origem, "nome": nome_base},
            ).fetchone()
            if not row:
                return []

            linhas = conn.execute(
                text(
                    """
                    SELECT cru.sk_config_regra, r.codigo, cru.parametros
                    FROM config_regra_usuario cru
                    JOIN dim_regra r ON r.sk_regra_tipo = cru.sk_regra_tipo
                    WHERE cru.sk_base = :sk_base AND cru.ativa = TRUE
                    """
                ),
                {"sk_base": row[0]},
            ).fetchall()
            return [{"sk_config_regra": sk_config, "codigo": codigo, "parametros": parametros} for sk_config, codigo, parametros in linhas]

    def registrar_base_leve(
        self, nome_base: str, sistema_origem: str, catalogo: str, schema_origem: str, colunas: List[str], dominio: str = None
    ) -> int:
        """Registra base+colunas em dim_base/dim_atributo SEM perfilar (nenhuma
        linha em dim_execucao/fato_*). Usa os mesmos get_or_create do pipeline
        completo, então é idempotente: rodar a pipeline de verdade depois
        reaproveita exatamente essas linhas, sem duplicar nem conflitar.
        Existe pra base aparecer no Streamlit (app_regras.py) antes do primeiro
        profiling, usando só a extração (ver src/extraction/extrair_apenas.py)."""
        with self.engine.begin() as conn:
            sk_base = self.get_or_create_base(conn, nome_base, sistema_origem, catalogo, schema_origem, dominio)
            for posicao, nome_coluna in enumerate(colunas):
                self.get_or_create_atributo(conn, sk_base, nome_coluna, posicao)
        return sk_base

    # ------------------------------------------------------------------ #
    # Dimensões (get-or-create)
    # ------------------------------------------------------------------ #

    def get_or_create_calendario(self, conn, data: datetime) -> int:
        sk = int(data.strftime("%Y%m%d"))
        row = conn.execute(text("SELECT sk_calendario FROM dim_calendario WHERE sk_calendario = :sk"), {"sk": sk}).fetchone()
        if row:
            return sk

        conn.execute(
            text(
                """
                INSERT INTO dim_calendario
                    (sk_calendario, data, ano, trimestre, mes, nome_mes, dia_mes,
                     dia_semana, nome_dia_semana, eh_fim_semana)
                VALUES (:sk, :data, :ano, :tri, :mes, :nome_mes, :dia_mes,
                        :dia_semana, :nome_dia_semana, :fim_semana)
                """
            ),
            {
                "sk": sk,
                "data": data.date(),
                "ano": data.year,
                "tri": (data.month - 1) // 3 + 1,
                "mes": data.month,
                "nome_mes": data.strftime("%B"),
                "dia_mes": data.day,
                "dia_semana": data.weekday(),
                "nome_dia_semana": data.strftime("%A"),
                "fim_semana": data.weekday() >= 5,
            },
        )
        return sk

    def get_or_create_base(self, conn, nome_base: str, sistema_origem: str, catalogo: str, schema_origem: str, dominio: str = None) -> int:
        row = conn.execute(
            text(
                """
                SELECT sk_base FROM dim_base
                WHERE sistema_origem = :sistema AND COALESCE(catalogo, '') = COALESCE(:catalogo, '')
                  AND COALESCE(schema_origem, '') = COALESCE(:schema, '') AND nome_base = :nome
                """
            ),
            {"sistema": sistema_origem, "catalogo": catalogo, "schema": schema_origem, "nome": nome_base},
        ).fetchone()
        if row:
            return row[0]

        row = conn.execute(
            text(
                """
                INSERT INTO dim_base (nome_base, sistema_origem, catalogo, schema_origem, dominio)
                VALUES (:nome, :sistema, :catalogo, :schema, :dominio)
                RETURNING sk_base
                """
            ),
            {"nome": nome_base, "sistema": sistema_origem, "catalogo": catalogo, "schema": schema_origem, "dominio": dominio},
        ).fetchone()
        return row[0]

    def get_or_create_atributo(self, conn, sk_base: int, nome_coluna: str, posicao: int) -> int:
        row = conn.execute(
            text("SELECT sk_atributo FROM dim_atributo WHERE sk_base = :sk_base AND nome_coluna = :nome"),
            {"sk_base": sk_base, "nome": nome_coluna},
        ).fetchone()
        if row:
            return row[0]

        row = conn.execute(
            text(
                """
                INSERT INTO dim_atributo (sk_base, nome_coluna, posicao_coluna)
                VALUES (:sk_base, :nome, :posicao)
                RETURNING sk_atributo
                """
            ),
            {"sk_base": sk_base, "nome": nome_coluna, "posicao": posicao},
        ).fetchone()
        return row[0]

    def criar_execucao(
        self,
        conn,
        data_hora_execucao: datetime,
        ferramenta: str,
        versao_ferramenta: str,
        modo_coleta: str,
        tamanho_amostra: int,
        duracao_segundos: float,
        config: Dict[str, Any],
    ) -> int:
        row = conn.execute(
            text(
                """
                INSERT INTO dim_execucao
                    (data_hora_execucao, ferramenta, versao_ferramenta, modo_coleta,
                     tamanho_amostra, duracao_segundos, config_json)
                VALUES (:dt, :ferramenta, :versao, :modo, :amostra, :duracao, CAST(:config AS jsonb))
                RETURNING sk_execucao
                """
            ),
            {
                "dt": data_hora_execucao,
                "ferramenta": ferramenta,
                "versao": versao_ferramenta,
                "modo": modo_coleta,
                "amostra": tamanho_amostra,
                "duracao": duracao_segundos,
                "config": dumps(config),
            },
        ).fetchone()
        return row[0]

    # ------------------------------------------------------------------ #
    # Histórico (grupo 2): compara com a execução anterior do mesmo atributo/base
    # ------------------------------------------------------------------ #

    def _historico_coluna(self, conn, sk_atributo: int, data_hora_execucao: datetime, qtd_linhas_atual: int, pct_valor_frequente_atual: float) -> Dict[str, Any]:
        anterior = conn.execute(
            text(
                """
                SELECT f.score_geral, f.qtd_linhas_total, f.percentual_valor_mais_frequente, e.data_hora_execucao
                FROM fato_perfilamento_coluna f
                JOIN dim_execucao e ON e.sk_execucao = f.sk_execucao
                WHERE f.sk_atributo = :sk_atributo AND e.data_hora_execucao < :agora
                ORDER BY e.data_hora_execucao DESC
                LIMIT 1
                """
            ),
            {"sk_atributo": sk_atributo, "agora": data_hora_execucao},
        ).fetchone()

        qtd_anteriores = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM fato_perfilamento_coluna f
                JOIN dim_execucao e ON e.sk_execucao = f.sk_execucao
                WHERE f.sk_atributo = :sk_atributo AND e.data_hora_execucao < :agora
                """
            ),
            {"sk_atributo": sk_atributo, "agora": data_hora_execucao},
        ).scalar()

        if not anterior:
            return {
                "score_geral_execucao_anterior": None,
                "variacao_score_geral": None,
                "percentual_variacao_qtd_linhas": None,
                "variacao_percentual_valor_mais_frequente": None,
                "dias_desde_execucao_anterior": None,
                "qtd_execucoes_anteriores": qtd_anteriores,
            }

        score_anterior, linhas_anterior, pct_freq_anterior, data_anterior = anterior
        return {
            "score_geral_execucao_anterior": float(score_anterior) if score_anterior is not None else None,
            "variacao_score_geral": None,  # calculado depois, quando soubermos o score atual
            "percentual_variacao_qtd_linhas": (
                round(100 * (qtd_linhas_atual - linhas_anterior) / linhas_anterior, 3) if linhas_anterior else None
            ),
            "variacao_percentual_valor_mais_frequente": (
                round(pct_valor_frequente_atual - float(pct_freq_anterior), 3)
                if pct_valor_frequente_atual is not None and pct_freq_anterior is not None
                else None
            ),
            "dias_desde_execucao_anterior": (data_hora_execucao - data_anterior).days,
            "qtd_execucoes_anteriores": qtd_anteriores,
        }

    def _historico_base(self, conn, sk_base: int, data_hora_execucao: datetime, qtd_linhas_atual: int) -> Dict[str, Any]:
        anterior = conn.execute(
            text(
                """
                SELECT b.score_final_base, b.qtd_linhas, e.data_hora_execucao
                FROM fato_perfilamento_base b
                JOIN dim_execucao e ON e.sk_execucao = b.sk_execucao
                WHERE b.sk_base = :sk_base AND e.data_hora_execucao < :agora
                ORDER BY e.data_hora_execucao DESC
                LIMIT 1
                """
            ),
            {"sk_base": sk_base, "agora": data_hora_execucao},
        ).fetchone()

        qtd_anteriores = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM fato_perfilamento_base b
                JOIN dim_execucao e ON e.sk_execucao = b.sk_execucao
                WHERE b.sk_base = :sk_base AND e.data_hora_execucao < :agora
                """
            ),
            {"sk_base": sk_base, "agora": data_hora_execucao},
        ).scalar()

        if not anterior:
            return {
                "score_final_execucao_anterior": None,
                "variacao_score_final": None,
                "percentual_variacao_qtd_linhas": None,
                "dias_desde_execucao_anterior": None,
                "qtd_execucoes_anteriores": qtd_anteriores,
            }

        score_anterior, linhas_anterior, data_anterior = anterior
        return {
            "score_final_execucao_anterior": float(score_anterior) if score_anterior is not None else None,
            "variacao_score_final": None,  # calculado depois
            "percentual_variacao_qtd_linhas": (
                round(100 * (qtd_linhas_atual - linhas_anterior) / linhas_anterior, 3) if linhas_anterior else None
            ),
            "dias_desde_execucao_anterior": (data_hora_execucao - data_anterior).days,
            "qtd_execucoes_anteriores": qtd_anteriores,
        }

    # ------------------------------------------------------------------ #
    # Orquestração principal
    # ------------------------------------------------------------------ #

    def salvar_perfilamento(
        self,
        nome_base: str,
        sistema_origem: str,
        catalogo: str,
        schema_origem: str,
        full: Dict[str, Any],
        scores_por_coluna: Dict[str, Any],
        score_final_base: float,
        classificacao_final_base: str,
        ferramenta: str,
        versao_ferramenta: str,
        modo_coleta: str,
        tamanho_amostra: int,
        duracao_segundos: float,
        config: Dict[str, Any],
        dominio: str = None,
        resultados_regras_usuario: Optional[List[Dict[str, Any]]] = None,
    ) -> int:
        agora = datetime.now()

        with self.engine.begin() as conn:
            sk_calendario = self.get_or_create_calendario(conn, agora)
            sk_base = self.get_or_create_base(conn, nome_base, sistema_origem, catalogo, schema_origem, dominio)
            sk_execucao = self.criar_execucao(
                conn, agora, ferramenta, versao_ferramenta, modo_coleta, tamanho_amostra, duracao_segundos, config
            )

            # ---- fato por coluna ----
            mapa_coluna_para_sk_atributo: Dict[str, int] = {}
            for posicao, (nome_coluna, metrics) in enumerate(full["colunas"].items()):
                sk_atributo = self.get_or_create_atributo(conn, sk_base, nome_coluna, posicao)
                mapa_coluna_para_sk_atributo[nome_coluna] = sk_atributo
                scores = scores_por_coluna[nome_coluna]

                historico = self._historico_coluna(
                    conn, sk_atributo, agora, metrics["qtd_linhas_total"], metrics.get("percentual_valor_mais_frequente")
                )
                if historico["score_geral_execucao_anterior"] is not None:
                    historico["variacao_score_geral"] = round(scores.score_geral - historico["score_geral_execucao_anterior"], 2)

                eh_numerico = metrics.get("tipo_dado_inferido") == "Numeric"
                eh_data = metrics.get("tipo_dado_inferido") == "DateTime"

                conn.execute(
                    text(
                        """
                        INSERT INTO fato_perfilamento_coluna (
                            sk_calendario, sk_execucao, sk_base, sk_atributo,
                            qtd_linhas_total, qtd_valores_nulos, percentual_valores_nulos,
                            qtd_valores_distintos, percentual_valores_distintos, qtd_valores_repetidos,
                            tipo_dado_inferido, percentual_valores_parseaveis_numero, percentual_valores_parseaveis_data,
                            valor_minimo, valor_maximo, data_minima, data_maxima, valor_medio, valor_mediano, desvio_padrao, variancia,
                            assimetria, curtose, percentil_25, percentil_75, coeficiente_variacao,
                            qtd_valores_zero, percentual_valores_zero, qtd_valores_negativos, percentual_valores_negativos,
                            qtd_valores_infinitos, qtd_outliers, percentual_outliers,
                            tamanho_medio_texto, tamanho_mediano_texto, tamanho_maximo_texto, tamanho_minimo_texto,
                            qtd_caracteres_distintos, valor_moda, frequencia_moda, percentual_valor_mais_frequente,
                            indice_concentracao_categorica, qtd_datas_invalidas, percentual_datas_invalidas,
                            tamanho_memoria_bytes,
                            score_completude, score_unicidade, score_consistencia, score_acuracia, score_atualidade,
                            score_conformidade_regras,
                            score_geral, classificacao,
                            score_geral_execucao_anterior, variacao_score_geral, percentual_variacao_qtd_linhas,
                            variacao_percentual_valor_mais_frequente, dias_desde_execucao_anterior, qtd_execucoes_anteriores
                        ) VALUES (
                            :sk_calendario, :sk_execucao, :sk_base, :sk_atributo,
                            :qtd_linhas_total, :qtd_valores_nulos, :percentual_valores_nulos,
                            :qtd_valores_distintos, :percentual_valores_distintos, :qtd_valores_repetidos,
                            :tipo_dado_inferido, :percentual_valores_parseaveis_numero, :percentual_valores_parseaveis_data,
                            :valor_minimo, :valor_maximo, :data_minima, :data_maxima, :valor_medio, :valor_mediano, :desvio_padrao, :variancia,
                            :assimetria, :curtose, :percentil_25, :percentil_75, :coeficiente_variacao,
                            :qtd_valores_zero, :percentual_valores_zero, :qtd_valores_negativos, :percentual_valores_negativos,
                            :qtd_valores_infinitos, :qtd_outliers, :percentual_outliers,
                            :tamanho_medio_texto, :tamanho_mediano_texto, :tamanho_maximo_texto, :tamanho_minimo_texto,
                            :qtd_caracteres_distintos, :valor_moda, :frequencia_moda, :percentual_valor_mais_frequente,
                            :indice_concentracao_categorica, :qtd_datas_invalidas, :percentual_datas_invalidas,
                            :tamanho_memoria_bytes,
                            :score_completude, :score_unicidade, :score_consistencia, :score_acuracia, :score_atualidade,
                            :score_conformidade_regras,
                            :score_geral, :classificacao,
                            :score_geral_execucao_anterior, :variacao_score_geral, :percentual_variacao_qtd_linhas,
                            :variacao_percentual_valor_mais_frequente, :dias_desde_execucao_anterior, :qtd_execucoes_anteriores
                        )
                        """
                    ),
                    {
                        "sk_calendario": sk_calendario,
                        "sk_execucao": sk_execucao,
                        "sk_base": sk_base,
                        "sk_atributo": sk_atributo,
                        "qtd_linhas_total": metrics.get("qtd_linhas_total"),
                        "qtd_valores_nulos": metrics.get("qtd_nulos"),
                        "percentual_valores_nulos": metrics.get("percentual_nulos"),
                        "qtd_valores_distintos": metrics.get("qtd_valores_distintos"),
                        "percentual_valores_distintos": (metrics.get("seletividade") or 0) * 100,
                        "qtd_valores_repetidos": metrics.get("qtd_duplicados"),
                        "tipo_dado_inferido": metrics.get("tipo_dado_inferido"),
                        "percentual_valores_parseaveis_numero": metrics.get("percentual_parse_numerico"),
                        "percentual_valores_parseaveis_data": metrics.get("percentual_parse_data"),
                        "valor_minimo": metrics.get("min") if eh_numerico else None,
                        "valor_maximo": metrics.get("max") if eh_numerico else None,
                        "data_minima": metrics.get("min") if eh_data else None,
                        "data_maxima": metrics.get("max") if eh_data else None,
                        "valor_medio": metrics.get("mean") if eh_numerico else None,
                        "valor_mediano": metrics.get("50%") if eh_numerico else None,
                        "desvio_padrao": metrics.get("std") if eh_numerico else None,
                        "variancia": metrics.get("variance") if eh_numerico else None,
                        "assimetria": metrics.get("skewness") if eh_numerico else None,
                        "curtose": metrics.get("kurtosis") if eh_numerico else None,
                        "percentil_25": metrics.get("25%") if eh_numerico else None,
                        "percentil_75": metrics.get("75%") if eh_numerico else None,
                        "coeficiente_variacao": metrics.get("cv") if eh_numerico else None,
                        "qtd_valores_zero": metrics.get("n_zeros") if eh_numerico else None,
                        "percentual_valores_zero": (metrics.get("p_zeros") * 100) if eh_numerico and metrics.get("p_zeros") is not None else None,
                        "qtd_valores_negativos": metrics.get("n_negative") if eh_numerico else None,
                        "percentual_valores_negativos": (
                            (metrics.get("p_negative") * 100) if eh_numerico and metrics.get("p_negative") is not None else None
                        ),
                        "qtd_valores_infinitos": metrics.get("n_infinite") if eh_numerico else None,
                        "qtd_outliers": metrics.get("qtd_outliers"),
                        "percentual_outliers": metrics.get("percentual_outliers"),
                        "tamanho_medio_texto": metrics.get("mean_length"),
                        "tamanho_mediano_texto": metrics.get("median_length"),
                        "tamanho_maximo_texto": metrics.get("max_length"),
                        "tamanho_minimo_texto": metrics.get("min_length"),
                        "qtd_caracteres_distintos": metrics.get("n_characters_distinct"),
                        "valor_moda": str(metrics["moda"]) if metrics.get("moda") is not None else None,
                        "frequencia_moda": metrics.get("frequencia_moda"),
                        "percentual_valor_mais_frequente": metrics.get("percentual_valor_mais_frequente"),
                        "indice_concentracao_categorica": metrics.get("indice_concentracao_categorica"),
                        "qtd_datas_invalidas": metrics.get("n_invalid_dates"),
                        "percentual_datas_invalidas": (metrics.get("p_invalid_dates") or None) and metrics.get("p_invalid_dates") * 100,
                        "tamanho_memoria_bytes": metrics.get("memory_size"),
                        "score_completude": scores.completude,
                        "score_unicidade": scores.unicidade,
                        "score_consistencia": scores.consistencia,
                        "score_acuracia": scores.acuracia,
                        "score_atualidade": scores.atualidade,
                        "score_conformidade_regras": scores.conformidade_regras,
                        "score_geral": scores.score_geral,
                        "classificacao": scores.classificacao,
                        **historico,
                    },
                )

            # ---- fato por base ----
            tabela = full["tabela"]
            historico_base = self._historico_base(conn, sk_base, agora, tabela.get("n"))
            if historico_base["score_final_execucao_anterior"] is not None:
                historico_base["variacao_score_final"] = round(score_final_base - historico_base["score_final_execucao_anterior"], 2)

            conn.execute(
                text(
                    """
                    INSERT INTO fato_perfilamento_base (
                        sk_calendario, sk_execucao, sk_base,
                        qtd_linhas, qtd_colunas, qtd_celulas_nulas, percentual_celulas_nulas,
                        qtd_colunas_com_nulo, qtd_linhas_duplicadas, percentual_linhas_duplicadas,
                        tamanho_memoria_bytes, tamanho_medio_registro, tipos_coluna, qtd_alertas, alertas,
                        score_final_base, classificacao_final_base,
                        score_final_execucao_anterior, variacao_score_final, percentual_variacao_qtd_linhas,
                        dias_desde_execucao_anterior, qtd_execucoes_anteriores
                    ) VALUES (
                        :sk_calendario, :sk_execucao, :sk_base,
                        :qtd_linhas, :qtd_colunas, :qtd_celulas_nulas, :percentual_celulas_nulas,
                        :qtd_colunas_com_nulo, :qtd_linhas_duplicadas, :percentual_linhas_duplicadas,
                        :tamanho_memoria_bytes, :tamanho_medio_registro, CAST(:tipos_coluna AS jsonb), :qtd_alertas, CAST(:alertas AS jsonb),
                        :score_final_base, :classificacao_final_base,
                        :score_final_execucao_anterior, :variacao_score_final, :percentual_variacao_qtd_linhas,
                        :dias_desde_execucao_anterior, :qtd_execucoes_anteriores
                    )
                    """
                ),
                {
                    "sk_calendario": sk_calendario,
                    "sk_execucao": sk_execucao,
                    "sk_base": sk_base,
                    "qtd_linhas": tabela.get("n"),
                    "qtd_colunas": tabela.get("n_var"),
                    "qtd_celulas_nulas": tabela.get("n_cells_missing"),
                    "percentual_celulas_nulas": (tabela.get("p_cells_missing") or 0) * 100,
                    "qtd_colunas_com_nulo": tabela.get("n_vars_with_missing"),
                    "qtd_linhas_duplicadas": tabela.get("n_duplicates"),
                    "percentual_linhas_duplicadas": (tabela.get("p_duplicates") or 0) * 100,
                    "tamanho_memoria_bytes": tabela.get("memory_size"),
                    "tamanho_medio_registro": tabela.get("record_size"),
                    "tipos_coluna": dumps(tabela.get("types", {})),
                    "qtd_alertas": len(full["alertas"]),
                    "alertas": dumps(full["alertas"]),
                    "score_final_base": score_final_base,
                    "classificacao_final_base": classificacao_final_base,
                    **historico_base,
                },
            )

            # ---- fatos satélite: correlação e padrão de ausência ----
            for metodo, matriz in full["correlacoes"].items():
                self._salvar_matriz_par_a_par(conn, "fato_correlacao", sk_execucao, sk_base, matriz, metodo=metodo)

            if full.get("padrao_ausencia"):
                self._salvar_matriz_par_a_par(conn, "fato_padrao_ausencia", sk_execucao, sk_base, full["padrao_ausencia"])

            # ---- fato satélite: duplicatas ----
            for linha in full.get("duplicatas") or []:
                conn.execute(
                    text("INSERT INTO fato_duplicatas (sk_execucao, sk_base, linha_duplicada) VALUES (:exec, :base, CAST(:linha AS jsonb))"),
                    {"exec": sk_execucao, "base": sk_base, "linha": dumps(linha)},
                )

            # ---- alertas automáticos (origem='sistema') ----
            mapa_regra_tipo = self._get_mapa_regra_tipo(conn)
            for alerta in gerar_alertas_automaticos(full):
                sk_regra_tipo = mapa_regra_tipo.get(alerta["codigo"])
                if sk_regra_tipo is None:
                    logger.warning(f"Código de regra desconhecido em dim_regra: {alerta['codigo']}")
                    continue

                sk_atributo_alerta = None
                if alerta["nome_coluna"] is not None:
                    sk_atributo_alerta = mapa_coluna_para_sk_atributo.get(alerta["nome_coluna"])

                conn.execute(
                    text(
                        """
                        INSERT INTO fato_regra_aplicada (
                            sk_execucao, sk_base, sk_atributo, sk_regra_tipo, origem,
                            colunas_envolvidas, parametros, passou, percentual_conformidade, qtd_violacoes, mensagem
                        ) VALUES (
                            :sk_execucao, :sk_base, :sk_atributo, :sk_regra_tipo, 'sistema',
                            :colunas_envolvidas, CAST(:parametros AS jsonb), FALSE, NULL, 1, :mensagem
                        )
                        """
                    ),
                    {
                        "sk_execucao": sk_execucao,
                        "sk_base": sk_base,
                        "sk_atributo": sk_atributo_alerta,
                        "sk_regra_tipo": sk_regra_tipo,
                        "colunas_envolvidas": alerta["colunas_envolvidas"],
                        "parametros": dumps(alerta["parametros"]),
                        "mensagem": alerta["mensagem"],
                    },
                )

            # ---- regras configuradas pelo usuário (origem='usuario') ----
            for resultado in resultados_regras_usuario or []:
                sk_regra_tipo = mapa_regra_tipo.get(resultado["codigo"])
                if sk_regra_tipo is None:
                    logger.warning(f"Código de regra desconhecido em dim_regra: {resultado['codigo']}")
                    continue

                parametros = resultado.get("parametros") or {}
                coluna_unica = parametros.get("coluna") if isinstance(parametros, dict) else None
                sk_atributo_regra = mapa_coluna_para_sk_atributo.get(coluna_unica) if coluna_unica else None

                conn.execute(
                    text(
                        """
                        INSERT INTO fato_regra_aplicada (
                            sk_execucao, sk_base, sk_atributo, sk_regra_tipo, sk_config_regra, origem,
                            colunas_envolvidas, parametros, passou, percentual_conformidade, qtd_violacoes, mensagem
                        ) VALUES (
                            :sk_execucao, :sk_base, :sk_atributo, :sk_regra_tipo, :sk_config_regra, 'usuario',
                            :colunas_envolvidas, CAST(:parametros AS jsonb), :passou, :percentual_conformidade, :qtd_violacoes, :mensagem
                        )
                        """
                    ),
                    {
                        "sk_execucao": sk_execucao,
                        "sk_base": sk_base,
                        "sk_atributo": sk_atributo_regra,
                        "sk_regra_tipo": sk_regra_tipo,
                        "sk_config_regra": resultado["sk_config_regra"],
                        "colunas_envolvidas": resultado["colunas_envolvidas"],
                        "parametros": dumps(resultado["parametros"]),
                        "passou": resultado["passou"],
                        "percentual_conformidade": resultado["percentual_conformidade"],
                        "qtd_violacoes": resultado["qtd_violacoes"],
                        "mensagem": resultado["mensagem"],
                    },
                )

        logger.success(f"Perfilamento de '{nome_base}' persistido (sk_execucao={sk_execucao})")
        return sk_execucao

    def _salvar_matriz_par_a_par(self, conn, tabela: str, sk_execucao: int, sk_base: int, matriz: List[Dict[str, Any]], metodo: str = None):
        if not matriz:
            return
        colunas_ordem = list(matriz[0].keys())

        for i in range(len(colunas_ordem)):
            for j in range(i + 1, len(colunas_ordem)):
                coluna_a, coluna_b = colunas_ordem[i], colunas_ordem[j]
                valor = matriz[i].get(coluna_b)
                if valor is None:
                    continue

                if metodo is not None:
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO {tabela} (sk_execucao, sk_base, coluna_a, coluna_b, metodo, valor_correlacao)
                            VALUES (:exec, :base, :a, :b, :metodo, :valor)
                            """
                        ),
                        {"exec": sk_execucao, "base": sk_base, "a": coluna_a, "b": coluna_b, "metodo": metodo, "valor": valor},
                    )
                else:
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO {tabela} (sk_execucao, sk_base, coluna_a, coluna_b, correlacao_nulidade)
                            VALUES (:exec, :base, :a, :b, :valor)
                            """
                        ),
                        {"exec": sk_execucao, "base": sk_base, "a": coluna_a, "b": coluna_b, "valor": valor},
                    )
