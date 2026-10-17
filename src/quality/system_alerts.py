"""
Alertas automáticos (origem='sistema') gerados a partir das métricas de
perfilamento. Mesma lógica serve pro ydata-profiling e pro DuckDB, já que
os dois produzem o mesmo formato de dict (full["colunas"] / full["correlacoes"]).

Persistidos em fato_regra_aplicada, lado a lado com as regras que o usuário
configurar (origem='usuario'), pra uma visão unificada em vw_regras_aplicadas.
Os códigos usados aqui (distorcao_estatistica, curtose_extrema, etc.) têm que
bater com dim_regra.codigo (categoria='sistema').
"""

from typing import Any, Dict, List, Optional

LIMIAR_ASSIMETRIA = 2.0
LIMIAR_CURTOSE = 10.0
LIMIAR_CONCENTRACAO_CATEGORICA = 0.8
LIMIAR_CORRELACAO_ALTA = 0.9
LIMIAR_CARDINALIDADE_ALTA = 0.95
QTD_MINIMA_LINHAS_PARA_CARDINALIDADE = 20  # evita disparar em tabelas pequenas, onde tudo é "quase distinto"
LIMIAR_PERCENTUAL_ZEROS = 50.0
LIMIAR_PERCENTUAL_NEGATIVOS = 5.0
LIMIAR_PARSE_TIPO_FISICO_DIVERGENTE = 90.0  # mesmo limiar que o profiler usa pra classificar a coluna como Numeric/DateTime

# Separa, dentro do mesmo campo `mensagem` (sem mudar o schema do data mart),
# a evidência técnica (o número medido) da interpretação em linguagem simples
#: relatorio_pt.py usa esse mesmo separador pra renderizar as duas partes em
# colunas distintas na tabela detalhada.
SEPARADOR_EVIDENCIA_CONCLUSAO = " ||| "


def _alerta(
    codigo: str,
    nome_coluna: Optional[str],
    evidencia: str,
    conclusao: str,
    parametros: Dict[str, Any],
    colunas_envolvidas: Optional[str] = None,
) -> Dict[str, Any]:
    mensagem = f"{evidencia}{SEPARADOR_EVIDENCIA_CONCLUSAO}{conclusao}" if conclusao else evidencia
    return {
        "codigo": codigo,
        "nome_coluna": nome_coluna,
        "colunas_envolvidas": colunas_envolvidas or nome_coluna,
        "mensagem": mensagem,
        "parametros": parametros,
    }


def _metodo_legivel(colunas_metrics: Dict[str, Any], coluna_a: str, coluna_b: str, metodo: str) -> str:
    """O DuckDBProfiler agrupa Pearson (par numérico) e Cramér's V (par categórico) numa
    única chave "auto": aqui a gente distingue de novo pelo tipo de cada coluna, pra
    mensagem final ficar clara. Pra chaves explícitas do ydata (pearson/spearman/...),
    mantém o nome original."""
    if metodo != "auto":
        return metodo
    tipo_a = colunas_metrics.get(coluna_a, {}).get("tipo_dado_inferido")
    tipo_b = colunas_metrics.get(coluna_b, {}).get("tipo_dado_inferido")
    if tipo_a == "Numeric" and tipo_b == "Numeric":
        return "Pearson"
    if tipo_a in ("Categorical", "Text") and tipo_b in ("Categorical", "Text"):
        return "Cramér's V"
    return metodo


def _interpretar_assimetria(skew: float) -> str:
    """Explica em linguagem simples (sem jargão estatístico) o que assimetria
    positiva/negativa significa na prática."""
    if skew > 0:
        return (
            "A maioria dos valores é baixa, mas alguns poucos valores bem mais altos puxam a "
            "média para cima. É comum em campos de valor monetário ou quantidade; vale checar "
            "se esses valores altos são reais ou erro de digitação."
        )
    return (
        "A maioria dos valores é alta, mas alguns poucos valores bem mais baixos puxam a "
        "média para baixo."
    )


def _interpretar_curtose(kurt: float) -> str:
    """Kurtosis aqui é excedente (normal = 0), convenção do pandas/DuckDB. Texto em
    linguagem simples, evitando termos como "cauda"/"distribuição normal"."""
    if kurt > 0:
        return (
            "Há mais valores muito acima ou muito abaixo da média do que o esperado, "
            "indicando possíveis outliers (anomalias) na coluna; vale investigar se esses "
            "valores são reais ou erro de coleta."
        )
    return (
        "Os valores estão espalhados de forma mais uniforme que o esperado, com poucos ou "
        "nenhum valor muito distante da média."
    )


def _interpretar_concentracao_categorica() -> str:
    return (
        "Poucas categorias concentram quase todos os registros. Pode refletir a realidade do "
        "negócio (a maioria dos casos realmente cai nessa categoria) ou indicar que valores "
        "raros não estão sendo registrados, ou que uma opção domina por padrão do sistema."
    )


def _percentual(metrics: Dict[str, Any], chave_pct: str, chave_fracao: str) -> Optional[float]:
    """Alguns valores já vêm como percentual (0-100) e outros como fração (0-1): mesma convenção mista já usada no loader."""
    valor = metrics.get(chave_pct)
    if valor is not None:
        return valor
    fracao = metrics.get(chave_fracao)
    return fracao * 100 if fracao is not None else None


def gerar_alertas_automaticos(full: Dict[str, Any]) -> List[Dict[str, Any]]:
    alertas: List[Dict[str, Any]] = []

    for nome_coluna, metrics in full["colunas"].items():
        tipo = metrics.get("tipo_dado_inferido")

        if tipo == "Numeric":
            skew = metrics.get("skewness")
            if skew is not None and abs(skew) > LIMIAR_ASSIMETRIA:
                alertas.append(_alerta(
                    "distorcao_estatistica", nome_coluna,
                    f"Assimetria de {skew:.2f} (limiar: ±{LIMIAR_ASSIMETRIA}).",
                    _interpretar_assimetria(skew),
                    {"assimetria": skew, "limiar": LIMIAR_ASSIMETRIA},
                ))

            kurt = metrics.get("kurtosis")
            if kurt is not None and abs(kurt) > LIMIAR_CURTOSE:
                alertas.append(_alerta(
                    "curtose_extrema", nome_coluna,
                    f"Curtose de {kurt:.2f} (limiar: ±{LIMIAR_CURTOSE}).",
                    _interpretar_curtose(kurt),
                    {"curtose": kurt, "limiar": LIMIAR_CURTOSE},
                ))

            pct_zeros = _percentual(metrics, "percentual_valores_zero", "p_zeros")
            if pct_zeros is not None and pct_zeros > LIMIAR_PERCENTUAL_ZEROS:
                alertas.append(_alerta(
                    "muitos_zeros", nome_coluna,
                    f"{pct_zeros:.1f}% dos valores são zero (limiar: {LIMIAR_PERCENTUAL_ZEROS}%).",
                    "Pode indicar que o valor não é relevante ou não foi preenchido na maioria "
                    "dos registros, ou que zero é um valor legítimo e frequente nesse contexto.",
                    {"percentual_zeros": pct_zeros, "limiar": LIMIAR_PERCENTUAL_ZEROS},
                ))

            pct_neg = _percentual(metrics, "percentual_valores_negativos", "p_negative")
            if pct_neg is not None and pct_neg > LIMIAR_PERCENTUAL_NEGATIVOS:
                alertas.append(_alerta(
                    "muitos_negativos", nome_coluna,
                    f"{pct_neg:.1f}% dos valores são negativos (limiar: {LIMIAR_PERCENTUAL_NEGATIVOS}%).",
                    "Vale confirmar se valores negativos fazem sentido para essa coluna (por "
                    "exemplo, estorno ou ajuste) ou se indicam erro de lançamento.",
                    {"percentual_negativos": pct_neg, "limiar": LIMIAR_PERCENTUAL_NEGATIVOS},
                ))

        if tipo == "Categorical":
            concentracao = metrics.get("indice_concentracao_categorica")
            if concentracao is not None and concentracao > LIMIAR_CONCENTRACAO_CATEGORICA:
                alertas.append(_alerta(
                    "desequilibrio_categorico", nome_coluna,
                    f"Índice de concentração {concentracao:.2f} (limiar: {LIMIAR_CONCENTRACAO_CATEGORICA}).",
                    _interpretar_concentracao_categorica(),
                    {"indice_concentracao": concentracao, "limiar": LIMIAR_CONCENTRACAO_CATEGORICA},
                ))

        tipo_fisico = (metrics.get("tipo_fisico_duckdb") or "").upper()
        if tipo_fisico.startswith("VARCHAR"):
            pct_num = metrics.get("percentual_parse_numerico")
            pct_data = metrics.get("percentual_parse_data")
            pct_num_alto = pct_num is not None and pct_num >= LIMIAR_PARSE_TIPO_FISICO_DIVERGENTE
            pct_data_alto = pct_data is not None and pct_data >= LIMIAR_PARSE_TIPO_FISICO_DIVERGENTE
            if pct_num_alto or pct_data_alto:
                parseia_como, percentual = ("número", pct_num) if pct_num_alto else ("data", pct_data)
                alertas.append(_alerta(
                    "tipo_fisico_divergente", nome_coluna,
                    f"Coluna declarada como texto no banco de origem, mas {percentual:.1f}% dos "
                    f"valores são parseáveis como {parseia_como} (limiar: "
                    f"{LIMIAR_PARSE_TIPO_FISICO_DIVERGENTE:.0f}%).",
                    "Pode ser uma coluna que deveria ter sido modelada como tipo numérico ou data "
                    "no banco de origem. Isso nem sempre é um erro: identificadores como CPF e CNPJ "
                    "costumam ser mantidos como texto de propósito, para preservar zeros à esquerda; "
                    "vale confirmar o uso pretendido da coluna.",
                    {"percentual_parseavel": percentual, "tipo_fisico": tipo_fisico, "limiar": LIMIAR_PARSE_TIPO_FISICO_DIVERGENTE},
                ))

        qtd_linhas = metrics.get("qtd_linhas_total") or 0
        seletividade = metrics.get("seletividade")
        if (
            seletividade is not None
            and seletividade > LIMIAR_CARDINALIDADE_ALTA
            and qtd_linhas >= QTD_MINIMA_LINHAS_PARA_CARDINALIDADE
        ):
            alertas.append(_alerta(
                "cardinalidade_muito_alta", nome_coluna,
                f"{seletividade * 100:.1f}% dos valores são distintos (limiar: {LIMIAR_CARDINALIDADE_ALTA * 100:.0f}%).",
                "Provavelmente é um identificador único (como CPF, ID ou hash), não uma "
                "métrica a ser comparada entre registros.",
                {"percentual_distintos": seletividade * 100, "limiar": LIMIAR_CARDINALIDADE_ALTA * 100},
            ))

    colunas_metrics = full["colunas"]
    for metodo, matriz in (full.get("correlacoes") or {}).items():
        if not matriz:
            continue
        colunas_ordem = list(matriz[0].keys())
        for i in range(len(colunas_ordem)):
            for j in range(i + 1, len(colunas_ordem)):
                coluna_a, coluna_b = colunas_ordem[i], colunas_ordem[j]
                valor = matriz[i].get(coluna_b)
                if valor is None:
                    continue
                if abs(valor) > LIMIAR_CORRELACAO_ALTA:
                    metodo_legivel = _metodo_legivel(colunas_metrics, coluna_a, coluna_b, metodo)
                    alertas.append(_alerta(
                        "correlacao_alta", None,
                        f"Correlação ({metodo_legivel}) de {valor:.2f} entre '{coluna_a}' e '{coluna_b}' "
                        f"(limiar: ±{LIMIAR_CORRELACAO_ALTA}).",
                        "As duas colunas tendem a variar juntas e podem estar repetindo a mesma "
                        "informação (redundância, repetição); vale avaliar se é necessário manter as duas.",
                        {"metodo": metodo_legivel, "valor_correlacao": valor, "limiar": LIMIAR_CORRELACAO_ALTA},
                        colunas_envolvidas=f"{coluna_a},{coluna_b}",
                    ))

    return alertas
