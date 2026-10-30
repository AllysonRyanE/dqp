"""
Geração de relatórios HTML nativos em pt-BR, direto do data mart (Postgres).
Não depende do ydata-profiling nem do DuckDB: funciona pra qualquer execução
já persistida, de qualquer motor (`dim_execucao.ferramenta`).

Dois níveis, pensados pra públicos diferentes:
  - executivo: resumo de uma página: score final, variação, principais
    problemas. Pra quem só quer saber "essa base está boa ou não".
  - detalhado: tabela completa por coluna (todas as dimensões de score),
    todos os alertas/regras aplicados, correlações fortes e amostra de
    duplicatas. Pra quem vai investigar a fundo.
"""

import html
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from quality.system_alerts import SEPARADOR_EVIDENCIA_CONCLUSAO


def _separar_evidencia_conclusao(mensagem: Optional[str]) -> Tuple[str, str]:
    """Separa o campo `mensagem` (fato_regra_aplicada) em (evidência técnica,
    possível conclusão), quando o alerta foi gerado com as duas partes
    marcadas (ver system_alerts.py). Mensagens de regra de usuário, que não
    usam esse separador, caem inteiras na evidência, sem conclusão."""
    if not mensagem:
        return "", ""
    partes = mensagem.split(SEPARADOR_EVIDENCIA_CONCLUSAO, 1)
    if len(partes) == 2:
        return partes[0], partes[1]
    return mensagem, ""

CORES_CLASSIFICACAO = {
    "Ótimo": ("#8a6d00", "#fff4cc"),
    "Regular": ("#555555", "#e8e8e8"),
    "Crítico": ("#7a3b12", "#f0dcc8"),
}


def _badge_classificacao(classificacao: Optional[str]) -> str:
    if not classificacao:
        return ""
    cor_texto, cor_fundo = CORES_CLASSIFICACAO.get(classificacao, ("#333", "#eee"))
    return (
        f'<span style="background:{cor_fundo};color:{cor_texto};padding:3px 12px;'
        f'border-radius:12px;font-weight:600;font-size:0.9em;">{html.escape(classificacao)}</span>'
    )


def _fmt(valor: Any, sufixo: str = "", casas: int = 2) -> str:
    if valor is None:
        return "—"
    if isinstance(valor, (int, float)):
        return f"{valor:.{casas}f}{sufixo}"
    return html.escape(str(valor))


def _fmt_variacao(valor: Optional[float]) -> str:
    if valor is None:
        return "<span style=\"color:#888\">sem execução anterior</span>"
    cor = "#1a7f37" if valor >= 0 else "#c0342a"
    seta = "▲" if valor >= 0 else "▼"
    return f'<span style="color:{cor};font-weight:600">{seta} {abs(valor):.2f} pts</span>'


# ------------------------------------------------------------------ #
# Acesso a dados (Postgres: mesmas tabelas que o loader.py escreve)
# ------------------------------------------------------------------ #

def _buscar_execucao_base(engine: Engine, nome_base: str, sk_execucao: Optional[int] = None) -> Optional[Dict[str, Any]]:
    with engine.begin() as conn:
        if sk_execucao is not None:
            filtro_execucao = "AND fb.sk_execucao = :sk_execucao"
        else:
            filtro_execucao = ""
        row = conn.execute(
            text(
                f"""
                SELECT fb.sk_execucao, b.sk_base, b.nome_base, b.catalogo, b.schema_origem,
                       ex.data_hora_execucao, ex.ferramenta, ex.modo_coleta, ex.duracao_segundos,
                       fb.qtd_linhas, fb.qtd_colunas, fb.percentual_celulas_nulas,
                       fb.qtd_colunas_com_nulo, fb.qtd_linhas_duplicadas, fb.percentual_linhas_duplicadas,
                       fb.score_final_base, fb.classificacao_final_base,
                       fb.score_final_execucao_anterior, fb.variacao_score_final,
                       fb.dias_desde_execucao_anterior, fb.qtd_execucoes_anteriores
                FROM fato_perfilamento_base fb
                JOIN dim_base b ON b.sk_base = fb.sk_base
                JOIN dim_execucao ex ON ex.sk_execucao = fb.sk_execucao
                WHERE b.nome_base = :nome_base {filtro_execucao}
                ORDER BY ex.data_hora_execucao DESC
                LIMIT 1
                """
            ),
            {"nome_base": nome_base, "sk_execucao": sk_execucao},
        ).mappings().fetchone()
    return dict(row) if row else None


def _buscar_colunas(engine: Engine, sk_execucao: int) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT a.nome_coluna, a.posicao_coluna, f.tipo_dado_inferido,
                       f.percentual_valores_nulos, f.percentual_valores_distintos,
                       f.score_completude, f.score_unicidade, f.score_consistencia,
                       f.score_acuracia, f.score_atualidade, f.score_conformidade_regras,
                       f.score_geral, f.classificacao, f.variacao_score_geral
                FROM fato_perfilamento_coluna f
                JOIN dim_atributo a ON a.sk_atributo = f.sk_atributo
                WHERE f.sk_execucao = :sk_execucao
                ORDER BY f.score_geral ASC, a.posicao_coluna ASC
                """
            ),
            {"sk_execucao": sk_execucao},
        ).mappings().fetchall()
    return [dict(r) for r in rows]


def _buscar_regras(engine: Engine, sk_execucao: int) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT r.codigo, r.nome_exibicao, ra.origem, ra.colunas_envolvidas,
                       ra.passou, ra.percentual_conformidade, ra.qtd_violacoes, ra.mensagem
                FROM fato_regra_aplicada ra
                JOIN dim_regra r ON r.sk_regra_tipo = ra.sk_regra_tipo
                WHERE ra.sk_execucao = :sk_execucao
                ORDER BY ra.passou ASC, ra.origem, r.nome_exibicao
                """
            ),
            {"sk_execucao": sk_execucao},
        ).mappings().fetchall()
    return [dict(r) for r in rows]


def _buscar_matriz_correlacoes(engine: Engine, sk_execucao: int) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT coluna_a, coluna_b, valor_correlacao
                FROM fato_correlacao
                WHERE sk_execucao = :sk_execucao
                """
            ),
            {"sk_execucao": sk_execucao},
        ).mappings().fetchall()
    return [dict(r) for r in rows]


def _buscar_dependencias_funcionais(engine: Engine, sk_execucao: int) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT coluna_origem, coluna_destino, qtd_grupos_origem,
                       percentual_grupos_sem_excecao, qtd_linhas_excecao, qtd_linhas_avaliadas
                FROM fato_dependencia_funcional
                WHERE sk_execucao = :sk_execucao
                ORDER BY percentual_grupos_sem_excecao DESC, qtd_grupos_origem DESC
                """
            ),
            {"sk_execucao": sk_execucao},
        ).mappings().fetchall()
    return [dict(r) for r in rows]


def _buscar_padrao_ausencia_alto(engine: Engine, sk_execucao: int, limiar: float = 0.7) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT coluna_a, coluna_b, correlacao_nulidade
                FROM fato_padrao_ausencia
                WHERE sk_execucao = :sk_execucao AND ABS(correlacao_nulidade) >= :limiar
                ORDER BY ABS(correlacao_nulidade) DESC
                LIMIT 30
                """
            ),
            {"sk_execucao": sk_execucao, "limiar": limiar},
        ).mappings().fetchall()
    return [dict(r) for r in rows]


def _buscar_duplicatas_amostra(engine: Engine, sk_execucao: int, limite: int = 5) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT linha_duplicada FROM fato_duplicatas WHERE sk_execucao = :sk_execucao LIMIT :limite"),
            {"sk_execucao": sk_execucao, "limite": limite},
        ).fetchall()
    return [r[0] for r in rows]


# ------------------------------------------------------------------ #
# Construção do HTML
# ------------------------------------------------------------------ #

_CSS_BASE = """
body { font-family: -apple-system, "Segoe UI", Roboto, Arial, sans-serif; color: #1a1a1a; background: #f7f7f8;
       margin: 0; padding: 40px 24px; }
.container { max-width: 1100px; margin: 0 auto; }
h1 { font-size: 1.6em; margin-bottom: 4px; }
h2 { font-size: 1.2em; margin-top: 36px; border-bottom: 2px solid #eee; padding-bottom: 6px; }
.subtitulo { color: #666; font-size: 0.95em; margin-bottom: 24px; }
.cartao-score { display: flex; align-items: center; gap: 24px; background: white; border-radius: 12px;
                padding: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); margin-bottom: 24px; }
.score-numero { font-size: 2.8em; font-weight: 700; }
.grid-resumo { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin: 16px 0; }
.item-resumo { background: white; border-radius: 8px; padding: 14px 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.06); }
.item-resumo .rotulo { font-size: 0.8em; color: #777; }
.item-resumo .valor { font-size: 1.3em; font-weight: 600; }
table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden;
        box-shadow: 0 1px 2px rgba(0,0,0,0.06); font-size: 0.9em; }
th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #eee; }
th { background: #fafafa; font-weight: 600; color: #444; }
tr:last-child td { border-bottom: none; }
.lista-problemas { list-style: none; padding: 0; }
.lista-problemas li { background: white; border-radius: 8px; padding: 12px 16px; margin-bottom: 8px;
                      box-shadow: 0 1px 2px rgba(0,0,0,0.06); }
.falhou { border-left: 4px solid #c0342a; }
.passou { border-left: 4px solid #1a7f37; }
.rodape { margin-top: 40px; color: #999; font-size: 0.8em; text-align: center; }
.tag-origem { font-size: 0.75em; padding: 1px 8px; border-radius: 10px; background: #eee; color: #555; }
table.heatmap { table-layout: fixed; }
table.heatmap th, table.heatmap td { padding: 4px 6px; font-size: 0.78em; white-space: nowrap; }
table.heatmap th:first-child { text-align: right; background: #fafafa; }
"""


def _cabecalho(execucao: Dict[str, Any], titulo_pagina: str) -> str:
    dt = execucao["data_hora_execucao"]
    dt_str = dt.strftime("%d/%m/%Y às %H:%M") if isinstance(dt, datetime) else str(dt)
    return f"""
<div class="container">
<h1>{html.escape(titulo_pagina)}</h1>
<div class="subtitulo">
    Base: <strong>{html.escape(execucao['nome_base'])}</strong>
    ({html.escape(execucao['catalogo'] or '')}.{html.escape(execucao['schema_origem'] or '')})
    &middot; Perfilada em {dt_str} via <strong>{html.escape(execucao['ferramenta'])}</strong>
    &middot; coleta {html.escape(execucao['modo_coleta'])}
</div>
"""


def _cartao_score(execucao: Dict[str, Any]) -> str:
    classificacao = execucao["classificacao_final_base"]
    cor_texto, _ = CORES_CLASSIFICACAO.get(classificacao, ("#333", "#eee"))
    return f"""
<div class="cartao-score">
    <div class="score-numero" style="color:{cor_texto}">{_fmt(execucao['score_final_base'], casas=1)}</div>
    <div>
        <div>{_badge_classificacao(classificacao)}</div>
        <div style="margin-top:6px;color:#666">
            Variação desde a execução anterior: {_fmt_variacao(execucao.get('variacao_score_final'))}
        </div>
    </div>
</div>
"""


def _grid_resumo(execucao: Dict[str, Any]) -> str:
    itens = [
        ("Linhas", f"{execucao['qtd_linhas']:,}".replace(",", ".")),
        ("Colunas", str(execucao["qtd_colunas"])),
        ("% células nulas", _fmt(execucao["percentual_celulas_nulas"], "%")),
        ("Colunas com nulo", str(execucao["qtd_colunas_com_nulo"])),
        ("% linhas duplicadas", _fmt(execucao["percentual_linhas_duplicadas"], "%")),
        ("Duração do profiling", f"{execucao['duracao_segundos']:.1f}s" if execucao.get("duracao_segundos") else "—"),
    ]
    células = "".join(
        f'<div class="item-resumo"><div class="rotulo">{html.escape(rotulo)}</div>'
        f'<div class="valor">{valor}</div></div>'
        for rotulo, valor in itens
    )
    return f'<div class="grid-resumo">{células}</div>'


def _piores_colunas(colunas: List[Dict[str, Any]], n: int = 5) -> str:
    piores = [c for c in colunas if c["score_geral"] is not None][:n]
    if not piores:
        return "<p>Nenhuma coluna com problema relevante.</p>"
    linhas = "".join(
        f"<li>"
        f"<strong>{html.escape(c['nome_coluna'])}</strong> ({html.escape(c['tipo_dado_inferido'] or '?')}): "
        f"score {_fmt(c['score_geral'], casas=1)} {_badge_classificacao(c['classificacao'])}"
        f"</li>"
        for c in piores
    )
    return f'<ul class="lista-problemas">{linhas}</ul>'


def _lista_regras_falhas(regras: List[Dict[str, Any]], n: Optional[int] = None) -> str:
    falhas = [r for r in regras if not r["passou"]]
    if n:
        falhas = falhas[:n]
    if not falhas:
        return "<p>Nenhum alerta ou regra violada nessa execução. ✅</p>"
    def _linha(r):
        evidencia, conclusao = _separar_evidencia_conclusao(r["mensagem"])
        texto = f"{evidencia} {conclusao}".strip() if conclusao else evidencia
        return (
            f'<li class="falhou">'
            f'<span class="tag-origem">{"sistema" if r["origem"] == "sistema" else "regra do usuário"}</span> '
            f"<strong>{html.escape(r['nome_exibicao'])}</strong> "
            f"({html.escape(r['colunas_envolvidas'] or '')}): {html.escape(texto)}"
            f"</li>"
        )

    linhas = "".join(_linha(r) for r in falhas)
    return f'<ul class="lista-problemas">{linhas}</ul>'


def _tabela_colunas_completa(colunas: List[Dict[str, Any]]) -> str:
    cabecalho = (
        "<tr><th>Coluna</th><th>Tipo</th><th>% nulos</th><th>% distintos</th>"
        "<th>Completude</th><th>Unicidade</th><th>Consistência</th><th>Acurácia</th>"
        "<th>Atualidade</th><th>Regras</th><th>Score geral</th><th>Classificação</th></tr>"
    )
    linhas = "".join(
        "<tr>"
        f"<td>{html.escape(c['nome_coluna'])}</td>"
        f"<td>{html.escape(c['tipo_dado_inferido'] or '?')}</td>"
        f"<td>{_fmt(c['percentual_valores_nulos'], '%')}</td>"
        f"<td>{_fmt(c['percentual_valores_distintos'], '%')}</td>"
        f"<td>{_fmt(c['score_completude'])}</td>"
        f"<td>{_fmt(c['score_unicidade'])}</td>"
        f"<td>{_fmt(c['score_consistencia'])}</td>"
        f"<td>{_fmt(c['score_acuracia'])}</td>"
        f"<td>{_fmt(c['score_atualidade'])}</td>"
        f"<td>{_fmt(c['score_conformidade_regras'])}</td>"
        f"<td><strong>{_fmt(c['score_geral'], casas=1)}</strong></td>"
        f"<td>{_badge_classificacao(c['classificacao'])}</td>"
        "</tr>"
        for c in colunas
    )
    return f"<table>{cabecalho}{linhas}</table>"


def _tabela_regras_completa(regras: List[Dict[str, Any]]) -> str:
    if not regras:
        return "<p>Nenhum alerta ou regra avaliada nessa execução.</p>"
    cabecalho = (
        "<tr><th>Origem</th><th>Regra</th><th>Colunas</th><th>Resultado</th>"
        "<th>% conformidade</th><th>Violações</th><th>Evidência técnica</th>"
        "<th>Possível conclusão</th></tr>"
    )

    def _linha(r):
        evidencia, conclusao = _separar_evidencia_conclusao(r["mensagem"])
        return (
            "<tr>"
            f"<td><span class=\"tag-origem\">{'sistema' if r['origem'] == 'sistema' else 'usuário'}</span></td>"
            f"<td>{html.escape(r['nome_exibicao'])}</td>"
            f"<td>{html.escape(r['colunas_envolvidas'] or '')}</td>"
            f"<td>{'✅' if r['passou'] else '❌'}</td>"
            f"<td>{_fmt(r['percentual_conformidade'], '%', casas=2)}</td>"
            f"<td>{r['qtd_violacoes'] if r['qtd_violacoes'] is not None else '—'}</td>"
            f"<td>{html.escape(evidencia)}</td>"
            f"<td>{html.escape(conclusao)}</td>"
            "</tr>"
        )

    linhas = "".join(_linha(r) for r in regras)
    return f"<table>{cabecalho}{linhas}</table>"


def _cor_heatmap(valor: Optional[float]) -> str:
    if valor is None:
        return "#f3f3f3"
    intensidade = min(abs(valor), 1.0)
    if valor >= 0:
        r, g, b = 255, int(255 - 140 * intensidade), int(255 - 220 * intensidade)
    else:
        r, g, b = int(255 - 200 * intensidade), int(255 - 90 * intensidade), 255
    return f"rgb({r},{g},{b})"


def _heatmap_correlacoes(pares: List[Dict[str, Any]]) -> str:
    if not pares:
        return "<p>Nenhum par de colunas elegível para matriz de correlação nessa execução.</p>"

    colunas = sorted({p["coluna_a"] for p in pares} | {p["coluna_b"] for p in pares})
    valores = {(p["coluna_a"], p["coluna_b"]): p["valor_correlacao"] for p in pares}

    def _valor(a: str, b: str) -> Optional[float]:
        if a == b:
            return 1.0
        return valores.get((a, b), valores.get((b, a)))

    cabecalho = "<th></th>" + "".join(f"<th>{html.escape(c)}</th>" for c in colunas)
    linhas = "".join(
        f"<tr><th>{html.escape(a)}</th>"
        + "".join(
            f'<td style="background:{_cor_heatmap(_valor(a, b))};text-align:center">{_fmt(_valor(a, b), casas=2)}</td>'
            for b in colunas
        )
        + "</tr>"
        for a in colunas
    )
    return f'<div style="overflow-x:auto"><table class="heatmap"><tr>{cabecalho}</tr>{linhas}</table></div>'


def _tabela_dependencias_funcionais(achados: List[Dict[str, Any]]) -> str:
    if not achados:
        return "<p>Nenhuma dependência funcional encontrada entre colunas.</p>"
    cabecalho = (
        "<tr><th>Origem → Destino</th><th>Grupos na origem</th>"
        "<th>% grupos sem exceção</th><th>Linhas de exceção</th><th>Linhas avaliadas</th></tr>"
    )
    linhas = "".join(
        "<tr>"
        f"<td>{html.escape(a['coluna_origem'])} → {html.escape(a['coluna_destino'])}</td>"
        f"<td>{a['qtd_grupos_origem']}</td>"
        f"<td>{_fmt(a['percentual_grupos_sem_excecao'], '%')}</td>"
        f"<td>{a['qtd_linhas_excecao']}</td>"
        f"<td>{a['qtd_linhas_avaliadas']}</td>"
        "</tr>"
        for a in achados
    )
    return f"<table>{cabecalho}{linhas}</table>"


def _tabela_padrao_ausencia(pares: List[Dict[str, Any]]) -> str:
    if not pares:
        return "<p>Nenhum par de colunas com nulos fortemente correlacionados (≥0,70) encontrado.</p>"
    cabecalho = "<tr><th>Coluna A</th><th>Coluna B</th><th>Correlação de nulidade</th></tr>"
    linhas = "".join(
        "<tr>"
        f"<td>{html.escape(p['coluna_a'])}</td><td>{html.escape(p['coluna_b'])}</td>"
        f"<td>{_fmt(p['correlacao_nulidade'], casas=3)}</td>"
        "</tr>"
        for p in pares
    )
    return f"<table>{cabecalho}{linhas}</table>"


def _rodape() -> str:
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    return f'<div class="rodape">Relatório gerado automaticamente pelo dqp em {agora}.</div></div>'


def gerar_html_executivo(engine: Engine, nome_base: str, sk_execucao: Optional[int] = None) -> str:
    execucao = _buscar_execucao_base(engine, nome_base, sk_execucao)
    if execucao is None:
        raise ValueError(f"Nenhuma execução encontrada para a base '{nome_base}'")

    colunas = _buscar_colunas(engine, execucao["sk_execucao"])
    regras = _buscar_regras(engine, execucao["sk_execucao"])

    corpo = f"""<!doctype html><html lang="pt-br"><head><meta charset="utf-8">
<title>Resumo de qualidade: {html.escape(nome_base)}</title><style>{_CSS_BASE}</style></head><body>
{_cabecalho(execucao, "Resumo de Qualidade de Dados")}
{_cartao_score(execucao)}
{_grid_resumo(execucao)}
<h2>Colunas que mais precisam de atenção</h2>
{_piores_colunas(colunas)}
<h2>Alertas e regras violadas</h2>
{_lista_regras_falhas(regras)}
{_rodape()}
</body></html>"""
    return corpo


def gerar_html_detalhado(engine: Engine, nome_base: str, sk_execucao: Optional[int] = None) -> str:
    execucao = _buscar_execucao_base(engine, nome_base, sk_execucao)
    if execucao is None:
        raise ValueError(f"Nenhuma execução encontrada para a base '{nome_base}'")

    sk_execucao_real = execucao["sk_execucao"]
    colunas = _buscar_colunas(engine, sk_execucao_real)
    regras = _buscar_regras(engine, sk_execucao_real)
    matriz_correlacoes = _buscar_matriz_correlacoes(engine, sk_execucao_real)
    dependencias_funcionais = _buscar_dependencias_funcionais(engine, sk_execucao_real)
    padrao_ausencia_alto = _buscar_padrao_ausencia_alto(engine, sk_execucao_real)
    duplicatas = _buscar_duplicatas_amostra(engine, sk_execucao_real)

    bloco_duplicatas = ""
    if duplicatas:
        exemplos = "".join(f"<li><code>{html.escape(str(d))}</code></li>" for d in duplicatas)
        bloco_duplicatas = f"<h2>Amostra de linhas duplicadas</h2><ul>{exemplos}</ul>"

    corpo = f"""<!doctype html><html lang="pt-br"><head><meta charset="utf-8">
<title>Relatório detalhado: {html.escape(nome_base)}</title><style>{_CSS_BASE}</style></head><body>
{_cabecalho(execucao, "Relatório Detalhado de Qualidade de Dados")}
{_cartao_score(execucao)}
{_grid_resumo(execucao)}
<h2>Todas as colunas</h2>
{_tabela_colunas_completa(colunas)}
<h2>Alertas do sistema e regras de negócio</h2>
{_tabela_regras_completa(regras)}
<h2>Matriz de correlação entre colunas</h2>
{_heatmap_correlacoes(matriz_correlacoes)}
<h2>Dependências funcionais encontradas</h2>
{_tabela_dependencias_funcionais(dependencias_funcionais)}
<h2>Padrão de ausência: colunas com nulos correlacionados (≥0,70)</h2>
{_tabela_padrao_ausencia(padrao_ausencia_alto)}
{bloco_duplicatas}
{_rodape()}
</body></html>"""
    return corpo
