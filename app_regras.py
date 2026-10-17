"""
Interface pra o usuário configurar regras de negócio por coluna (origem='usuario'
em fato_regra_aplicada). As regras salvas aqui são lidas por
persistir_datamart_duckdb.py em toda execução seguinte (via
loader.buscar_regras_usuario_ativas + rules.user_rules_engine.avaliar_regras).

Rodar com: .venv/bin/streamlit run app_regras.py
"""

import json
import os
import sys
from datetime import date

import duckdb
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, "src")

from rules.user_rules_engine import avaliar_regras  # noqa: E402

load_dotenv()

CAMINHO_DUCKDB = "/tmp/dqp_extrato.duckdb"

st.set_page_config(page_title="dqp: Regras de Negócio", layout="wide")


@st.cache_resource
def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.environ['PG_DATAMART_USER']}:{os.environ['PG_DATAMART_PASSWORD']}"
        f"@{os.environ['PG_DATAMART_HOST']}:{os.environ['PG_DATAMART_PORT']}/{os.environ['PG_DATAMART_DATABASE']}"
    )


engine = get_engine()


def listar_bases():
    with engine.begin() as conn:
        return conn.execute(text("SELECT sk_base, nome_base FROM dim_base ORDER BY nome_base")).fetchall()


def listar_colunas(sk_base: int):
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT nome_coluna FROM dim_atributo WHERE sk_base = :sk ORDER BY posicao_coluna"), {"sk": sk_base}
        ).fetchall()
    return [r[0] for r in rows]


def listar_colunas_com_tipo(sk_base: int) -> dict:
    """{nome_coluna: tipo_dado_inferido}, segundo a execução mais recente dessa base.
    tipo_dado_inferido é por-execução (fato_perfilamento_coluna), não por-base: por
    isso não dá pra tirar isso de dim_atributo direto."""
    with engine.begin() as conn:
        sk_execucao = conn.execute(
            text("SELECT sk_execucao FROM fato_perfilamento_base WHERE sk_base = :sk ORDER BY sk_execucao DESC LIMIT 1"),
            {"sk": sk_base},
        ).scalar()
        if sk_execucao is None:
            return {}
        rows = conn.execute(
            text(
                """
                SELECT a.nome_coluna, f.tipo_dado_inferido
                FROM fato_perfilamento_coluna f
                JOIN dim_atributo a ON a.sk_atributo = f.sk_atributo
                WHERE f.sk_execucao = :sk_execucao
                ORDER BY a.posicao_coluna
                """
            ),
            {"sk_execucao": sk_execucao},
        ).fetchall()
    return {nome: tipo for nome, tipo in rows}


def listar_tipos_regra():
    with engine.begin() as conn:
        return conn.execute(
            text(
                "SELECT sk_regra_tipo, codigo, nome_exibicao, descricao FROM dim_regra "
                "WHERE categoria = 'usuario' ORDER BY nome_exibicao"
            )
        ).fetchall()


def listar_regras_configuradas(sk_base: int):
    with engine.begin() as conn:
        return conn.execute(
            text(
                """
                SELECT cru.sk_config_regra, cru.nome_regra, r.codigo, r.nome_exibicao, cru.parametros, cru.ativa, cru.criado_em
                FROM config_regra_usuario cru
                JOIN dim_regra r ON r.sk_regra_tipo = cru.sk_regra_tipo
                WHERE cru.sk_base = :sk
                ORDER BY cru.criado_em DESC
                """
            ),
            {"sk": sk_base},
        ).fetchall()


def ultimo_resultado(sk_config_regra: int):
    with engine.begin() as conn:
        return conn.execute(
            text(
                """
                SELECT ra.passou, ra.percentual_conformidade, ra.qtd_violacoes, ra.mensagem, ex.data_hora_execucao
                FROM fato_regra_aplicada ra
                JOIN dim_execucao ex ON ex.sk_execucao = ra.sk_execucao
                WHERE ra.sk_config_regra = :sk
                ORDER BY ex.data_hora_execucao DESC
                LIMIT 1
                """
            ),
            {"sk": sk_config_regra},
        ).fetchone()


def inserir_regra(sk_base: int, sk_regra_tipo: int, nome_regra: str, parametros: dict):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO config_regra_usuario (sk_base, sk_regra_tipo, nome_regra, parametros)
                VALUES (:sk_base, :sk_regra_tipo, :nome_regra, CAST(:parametros AS jsonb))
                """
            ),
            {
                "sk_base": sk_base,
                "sk_regra_tipo": sk_regra_tipo,
                "nome_regra": nome_regra or None,
                "parametros": json.dumps(parametros, ensure_ascii=False),
            },
        )


def alternar_ativa(sk_config_regra: int, ativa: bool):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE config_regra_usuario SET ativa = :ativa, atualizado_em = NOW() WHERE sk_config_regra = :sk"),
            {"ativa": ativa, "sk": sk_config_regra},
        )


def excluir_regra(sk_config_regra: int):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM config_regra_usuario WHERE sk_config_regra = :sk"), {"sk": sk_config_regra})


def valores_distintos_duckdb(tabela: str, coluna: str, limite: int = 200):
    con = duckdb.connect(CAMINHO_DUCKDB, read_only=True)
    try:
        rows = con.execute(
            f'SELECT DISTINCT "{coluna}" FROM "{tabela}" WHERE "{coluna}" IS NOT NULL ORDER BY 1 LIMIT {limite}'
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


st.title("dqp: Regras de Negócio")
st.caption(
    "Configure regras específicas do seu domínio, avaliadas junto com o profiling automático "
    "(Great Expectations + SQL contra o extrato DuckDB) e gravadas lado a lado com os alertas "
    "do sistema em `fato_regra_aplicada`."
)

bases = listar_bases()
if not bases:
    st.warning("Nenhuma base perfilada ainda. Rode `persistir_datamart_duckdb.py` primeiro.")
    st.stop()

mapa_bases = {nome: sk for sk, nome in bases}
nome_base_selecionada = st.sidebar.selectbox("Base de dados", list(mapa_bases.keys()))
sk_base = mapa_bases[nome_base_selecionada]

colunas = listar_colunas(sk_base)
colunas_tipos = listar_colunas_com_tipo(sk_base)  # {} se a base nunca foi perfilada
tipos_regra = listar_tipos_regra()

# Só filtra pra tipos que a UI restringe: pras demais, tipo não importa
# semanticamente (ex: valor_unico serve pra texto, número, data, tanto faz).
COLUNAS_POR_TIPO_EXIGIDO = {
    "sem_negativos": ("Numeric",),
    "intervalo_numerico": ("Numeric",),
    "soma_colunas": ("Numeric",),
    "intervalo_data": ("DateTime",),
}


def colunas_do_tipo(tipos_aceitos):
    if not colunas_tipos:
        # base nunca foi perfilada: não dá pra saber o tipo, libera tudo
        # (vai cair no guard de user_rules_engine.py se o tipo estiver errado)
        return colunas
    return [c for c in colunas if colunas_tipos.get(c) in tipos_aceitos]

tab_configuradas, tab_nova = st.tabs(["Regras configuradas", "Nova regra"])

with tab_configuradas:
    regras = listar_regras_configuradas(sk_base)

    if not regras:
        st.info("Nenhuma regra configurada pra essa base ainda: use a aba \"Nova regra\".")
    else:
        for sk_config, nome_regra, codigo, nome_exibicao, parametros, ativa, criado_em in regras:
            with st.container(border=True):
                col_info, col_ativa, col_excluir = st.columns([5, 1, 1])

                with col_info:
                    st.markdown(f"**{nome_regra or nome_exibicao}** · `{codigo}`")
                    st.code(json.dumps(parametros, ensure_ascii=False, indent=2), language="json")

                    resultado = ultimo_resultado(sk_config)
                    if resultado:
                        passou, pct, viol, msg, dt = resultado
                        icone = "✅" if passou else "❌"
                        st.markdown(
                            f"{icone} Última avaliação ({dt.strftime('%d/%m/%Y %H:%M')}): "
                            f"{pct}% conformidade, {viol} violação(ões): {msg}"
                        )
                    else:
                        st.caption("Ainda não avaliada: rode a pipeline de profiling pra essa base.")

                with col_ativa:
                    nova_ativa = st.toggle("Ativa", value=ativa, key=f"ativa_{sk_config}")
                    if nova_ativa != ativa:
                        alternar_ativa(sk_config, nova_ativa)
                        st.rerun()

                with col_excluir:
                    if st.button("Excluir", key=f"excluir_{sk_config}"):
                        excluir_regra(sk_config)
                        st.rerun()

    st.divider()
    st.subheader("Prévia (não grava resultado)")
    st.caption(
        "Avalia as regras ativas contra o último extrato salvo em `/tmp/dqp_extrato.duckdb` sem esperar a "
        "próxima rodada da pipeline. Só pra conferência rápida: o resultado oficial (gravado no histórico) "
        "só sai rodando `persistir_datamart_duckdb.py`."
    )
    if st.button("Avaliar regras ativas agora"):
        regras_ativas = [
            {"sk_config_regra": sk_config, "codigo": codigo, "parametros": parametros}
            for sk_config, _nome, codigo, _nome_exib, parametros, ativa, _criado in regras
            if ativa
        ]
        if not regras_ativas:
            st.warning("Nenhuma regra ativa pra avaliar.")
        else:
            with st.spinner("Avaliando..."):
                resultados = avaliar_regras(CAMINHO_DUCKDB, nome_base_selecionada, regras_ativas)
            for r in resultados:
                icone = "✅" if r["passou"] else "❌"
                st.markdown(f"{icone} `{r['codigo']}` ({r['colunas_envolvidas']}): {r['mensagem']}")

with tab_nova:
    mapa_tipos = {f"{nome_exibicao} ({codigo})": (sk, codigo) for sk, codigo, nome_exibicao, _desc in tipos_regra}
    escolha = st.selectbox("Tipo de regra", list(mapa_tipos.keys()))
    sk_regra_tipo, codigo = mapa_tipos[escolha]
    descricao = next(d for _sk, c, _n, d in tipos_regra if c == codigo)
    st.caption(descricao)

    nome_regra = st.text_input("Nome da regra (opcional, só pra identificar na lista)")

    parametros = None

    if codigo == "valores_distintos_entre":
        coluna = st.selectbox("Coluna", colunas)
        c1, c2 = st.columns(2)
        minimo = c1.number_input("Mínimo de valores distintos", min_value=0, value=1, step=1)
        maximo = c2.number_input("Máximo de valores distintos", min_value=0, value=100, step=1)
        parametros = {"coluna": coluna, "minimo": int(minimo), "maximo": int(maximo)}

    elif codigo == "comprimento_maximo":
        coluna = st.selectbox("Coluna", colunas)
        maximo = st.number_input("Comprimento máximo (caracteres)", min_value=1, value=50, step=1)
        parametros = {"coluna": coluna, "maximo": int(maximo)}

    elif codigo == "chave_composta_unica":
        colunas_sel = st.multiselect("Colunas (selecione 2 ou mais)", colunas)
        if len(colunas_sel) >= 2:
            parametros = {"colunas": colunas_sel}
        elif colunas_sel:
            st.warning("Selecione pelo menos 2 colunas.")

    elif codigo == "soma_colunas":
        colunas_numericas = colunas_do_tipo(("Numeric",))
        if not colunas_numericas:
            st.warning("Essa base não tem coluna Numeric: 'soma de colunas' não se aplica.")
        coluna_soma = st.selectbox("Coluna que deve ser igual à soma", colunas_numericas)
        colunas_parcelas = st.multiselect(
            "Colunas somadas (parcelas)", [c for c in colunas_numericas if c != coluna_soma]
        )
        tolerancia = st.number_input("Tolerância (diferença aceitável)", min_value=0.0, value=0.01, format="%.4f")
        if colunas_parcelas:
            parametros = {"coluna_soma": coluna_soma, "colunas_parcelas": colunas_parcelas, "tolerancia": tolerancia}
        else:
            st.warning("Selecione ao menos uma coluna parcela.")

    elif codigo == "sem_negativos":
        colunas_numericas = colunas_do_tipo(("Numeric",))
        if not colunas_numericas:
            st.warning("Essa base não tem coluna Numeric: 'sem negativos' não se aplica.")
        coluna = st.selectbox("Coluna", colunas_numericas)
        if coluna:
            parametros = {"coluna": coluna}

    elif codigo == "intervalo_numerico":
        colunas_numericas = colunas_do_tipo(("Numeric",))
        if not colunas_numericas:
            st.warning("Essa base não tem coluna Numeric: 'intervalo numérico' não se aplica.")
        coluna = st.selectbox("Coluna", colunas_numericas)
        c1, c2 = st.columns(2)
        usar_min = c1.checkbox("Definir mínimo", value=True, key="num_usar_min")
        minimo = c1.number_input("Mínimo", value=0.0, key="num_min") if usar_min else None
        usar_max = c2.checkbox("Definir máximo", value=False, key="num_usar_max")
        maximo = c2.number_input("Máximo", value=100.0, key="num_max") if usar_max else None
        if coluna:
            parametros = {"coluna": coluna, "minimo": minimo, "maximo": maximo}

    elif codigo == "intervalo_data":
        colunas_data = colunas_do_tipo(("DateTime",))
        if not colunas_data:
            st.warning("Essa base não tem coluna DateTime: 'intervalo de data' não se aplica.")
        coluna = st.selectbox("Coluna", colunas_data)
        c1, c2 = st.columns(2)
        usar_min = c1.checkbox("Definir data mínima", value=False, key="data_usar_min")
        minimo = c1.date_input("Data mínima", value=date(2000, 1, 1), key="data_min") if usar_min else None
        usar_max = c2.checkbox("Definir data máxima", value=False, key="data_usar_max")
        maximo = c2.date_input("Data máxima", value=date.today(), key="data_max") if usar_max else None
        if coluna:
            parametros = {
                "coluna": coluna,
                "minimo": minimo.isoformat() if minimo else None,
                "maximo": maximo.isoformat() if maximo else None,
            }

    elif codigo == "valores_aceitos":
        coluna = st.selectbox("Coluna", colunas)
        chave_cache = f"valores_carregados_{nome_base_selecionada}_{coluna}"
        if st.button("Carregar valores distintos do extrato DuckDB"):
            try:
                st.session_state[chave_cache] = valores_distintos_duckdb(nome_base_selecionada, coluna)
            except Exception as e:
                st.error(f"Não foi possível ler o extrato DuckDB: {e}")
        opcoes = st.session_state.get(chave_cache, [])
        if not opcoes:
            st.caption("Clique acima pra carregar os valores reais da coluna (evita digitar valores errados).")
        valores = st.multiselect("Valores aceitos", opcoes, default=opcoes)
        if valores:
            parametros = {"coluna": coluna, "valores": valores}

    elif codigo == "valor_unico":
        coluna = st.selectbox("Coluna", colunas)
        parametros = {"coluna": coluna}

    elif codigo == "formato_regex":
        coluna = st.selectbox("Coluna", colunas)
        regex = st.text_input("Expressão regular", placeholder=r"^([0-9]{11}|[0-9]{14})$")
        if regex:
            parametros = {"coluna": coluna, "regex": regex}

    elif codigo == "existe_em_outra_tabela":
        coluna = st.selectbox("Coluna", colunas)
        outras_bases = {nome: sk for nome, sk in [(n, s) for s, n in bases] if nome != nome_base_selecionada}
        if outras_bases:
            tabela_referencia = st.selectbox("Tabela de referência", list(outras_bases.keys()))
            colunas_ref = listar_colunas(outras_bases[tabela_referencia])
            coluna_referencia = st.selectbox("Coluna de referência", colunas_ref)
            parametros = {"coluna": coluna, "tabela_referencia": tabela_referencia, "coluna_referencia": coluna_referencia}
        else:
            st.warning("Precisa de pelo menos outra base perfilada pra usar essa regra.")

    elif codigo == "completude_condicional":
        coluna_condicionante = st.selectbox("Se esta coluna estiver preenchida...", colunas)
        opcoes_dependente = [c for c in colunas if c != coluna_condicionante]
        coluna_dependente = st.selectbox("...esta também deve estar", opcoes_dependente)
        parametros = {"coluna_condicionante": coluna_condicionante, "coluna_dependente": coluna_dependente}

    elif codigo == "nulidade_aceitavel":
        coluna = st.selectbox("Coluna", colunas)
        tolerancia = st.number_input(
            "Tolerância de nulidade (%)", min_value=0.0, max_value=100.0, value=10.0, step=1.0,
            help="Até esse % de nulos não penaliza a completude da coluna: só o que passar disso conta.",
        )
        parametros = {"coluna": coluna, "tolerancia_percentual": tolerancia}

    st.divider()
    if st.button("Salvar regra", type="primary", disabled=parametros is None):
        inserir_regra(sk_base, sk_regra_tipo, nome_regra, parametros)
        st.success("Regra salva! Ela entra na próxima execução de `persistir_datamart_duckdb.py`.")
        st.rerun()
