"""
Testes do DuckDBProfiler contra um arquivo DuckDB local sintético (sem
precisar de nenhuma fonte externa: Trino/Postgres/etc. não entram aqui).
Cobre principalmente a inferência de tipo (_inferir_tipo), que é a base de
praticamente toda regra de qualidade e de alerta do dqp.
"""

import duckdb
import pytest

from profilers.duckdb_profiler import ORDEM_DIAS_SEMANA_PT, DuckDBProfiler


@pytest.fixture
def caminho_duckdb(tmp_path):
    caminho = str(tmp_path / "teste.duckdb")
    con = duckdb.connect(caminho)
    con.execute(
        """
        CREATE TABLE pessoas (
            cpf VARCHAR,       -- texto, mas 100% numérico, largura fixa e quase-único (identificador sem zero à esquerda na amostra)
            matricula VARCHAR, -- idem, mas com zero à esquerda real em todo valor (prova direta de perda no cast)
            uf VARCHAR,        -- texto categórico de verdade
            nascimento VARCHAR, -- texto, mas 100% parseável como data
            idade BIGINT,      -- já numérico de verdade (fixo em 2 dígitos, mas baixa seletividade: não vira Identifier)
            observacao VARCHAR -- maioria nula
        )
        """
    )
    ufs = ["SP", "RJ", "MG", "BA"]
    for i in range(200):
        cpf = str(10000000000 + i)
        matricula = f"{i:04d}"  # "0000".."0199": zero à esquerda em toda linha
        uf = ufs[i % len(ufs)]
        nascimento = f"19{70 + i % 30}-0{1 + i % 9}-10"
        observacao = "ok" if i % 20 == 0 else None
        con.execute(
            "INSERT INTO pessoas VALUES (?, ?, ?, ?, ?, ?)",
            [cpf, matricula, uf, nascimento, 20 + i % 50, observacao],
        )
    con.close()
    return caminho


def test_coluna_texto_fixa_e_distinta_vira_identifier(caminho_duckdb):
    """Era o caso que o próprio comentário da fixture já apontava como "real"
    (CPF sem máscara) e que, antes desta mudança, virava Numeric — entrando em
    Pearson e ganhando média/assimetria sem nenhum sentido. Largura fixa (11
    dígitos) + seletividade 100% já bastam como prova de formato, mesmo sem
    nenhum zero à esquerda nesta amostra específica."""
    full = DuckDBProfiler(caminho_duckdb).profile_full("pessoas")
    cpf = full["colunas"]["cpf"]
    assert cpf["tipo_dado_inferido"] == "Identifier"
    assert cpf["tipo_fisico_duckdb"] == "VARCHAR"
    assert cpf["percentual_parse_numerico"] == 100.0
    # Mesmo virando Identifier, ainda ganha as estatísticas de texto (não fica sem nada):
    assert cpf["min_length"] == 11
    assert cpf["max_length"] == 11


def test_coluna_com_zero_a_esquerda_vira_identifier(caminho_duckdb):
    """Prova direta (não só formato): pelo menos um valor já perdeu o zero à
    esquerda no TRY_CAST. Isso sozinho basta, independente de seletividade."""
    full = DuckDBProfiler(caminho_duckdb).profile_full("pessoas")
    matricula = full["colunas"]["matricula"]
    assert matricula["tipo_dado_inferido"] == "Identifier"
    assert matricula["percentual_parse_numerico"] == 100.0


def test_coluna_numerica_de_baixa_cardinalidade_nao_vira_identifier(caminho_duckdb):
    """Controle negativo: largura fixa (2 dígitos) não é suficiente sozinha.
    'idade' tem seletividade baixa (50 valores distintos em 200 linhas) — é
    uma métrica de verdade, não um código, e deve continuar Numeric."""
    full = DuckDBProfiler(caminho_duckdb).profile_full("pessoas")
    idade = full["colunas"]["idade"]
    assert idade["tipo_dado_inferido"] == "Numeric"


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


# ---------------------------------------------------------------------- #
# Frequência das frequências (item 1), dependência funcional (item 3) e
# datas na análise (item 5): uma única tabela sintética cobre os três,
# porque as três adições vivem em colunas diferentes da mesma linha.
# ---------------------------------------------------------------------- #


@pytest.fixture
def caminho_duckdb_pedidos(tmp_path):
    caminho = str(tmp_path / "pedidos.duckdb")
    con = duckdb.connect(caminho)
    con.execute(
        """
        CREATE TABLE pedidos (
            pedido VARCHAR,       -- cada valor repete um número diferente de vezes (item 1)
            produto VARCHAR,      -- determina loja sem exceção nenhuma (item 3, caso positivo)
            loja VARCHAR,         -- não determina produto de volta (item 3, controle de direção)
            data_pedido VARCHAR   -- 4 datas distintas com gaps conhecidos (item 5)
        )
        """
    )

    # min=1, mediana=3, máximo=81: mesmo espírito do "linhas por pedido" do pedido original.
    distribuicao_pedido = [1, 2, 2, 3, 3, 3, 4, 5, 81]
    produto_para_loja = {"produto_a": "loja_1", "produto_b": "loja_1", "produto_c": "loja_2", "produto_d": "loja_2"}
    produtos_ciclo = list(produto_para_loja.keys())
    datas_ciclo = ["2026-01-01", "2026-01-03", "2026-01-04", "2026-01-10"]  # gaps consecutivos: 2, 1, 6 dias

    linhas = []
    linha_id = 0
    for indice_pedido, qtd in enumerate(distribuicao_pedido, start=1):
        for _ in range(qtd):
            produto = produtos_ciclo[linha_id % len(produtos_ciclo)]
            linhas.append((
                f"PED{indice_pedido:04d}",
                produto,
                produto_para_loja[produto],
                datas_ciclo[linha_id % len(datas_ciclo)],
            ))
            linha_id += 1

    con.executemany("INSERT INTO pedidos VALUES (?, ?, ?, ?)", linhas)
    con.close()
    return caminho


def test_frequencia_das_frequencias(caminho_duckdb_pedidos):
    full = DuckDBProfiler(caminho_duckdb_pedidos).profile_full("pedidos")
    pedido = full["colunas"]["pedido"]
    assert pedido["frequencia_minima_valor"] == 1
    assert pedido["frequencia_mediana_valor"] == 3.0
    assert pedido["frequencia_moda"] == 81  # continua sendo o máximo da distribuição


def test_dependencia_funcional_descoberta_com_excecoes_contadas(caminho_duckdb_pedidos):
    full = DuckDBProfiler(caminho_duckdb_pedidos).profile_full("pedidos")
    achados = full["dependencias_funcionais"]

    achado_produto_loja = next(a for a in achados if a["origem"] == "produto" and a["destino"] == "loja")
    assert achado_produto_loja["percentual_grupos_sem_excecao"] == 100.0
    assert achado_produto_loja["qtd_linhas_excecao"] == 0
    assert achado_produto_loja["qtd_grupos_origem"] == 4

    # Direção oposta não é FD (loja_1 cobre produto_a E produto_b), não pode aparecer:
    assert not any(a["origem"] == "loja" and a["destino"] == "produto" for a in achados)


def test_datas_intervalo_entre_datas_distintas(caminho_duckdb_pedidos):
    full = DuckDBProfiler(caminho_duckdb_pedidos).profile_full("pedidos")
    data_pedido = full["colunas"]["data_pedido"]
    assert data_pedido["intervalo_minimo_dias"] == 1
    assert data_pedido["intervalo_mediano_dias"] == 2.0
    assert data_pedido["intervalo_maximo_dias"] == 6
    assert sum(data_pedido["distribuicao_dia_semana"].values()) == data_pedido["qtd_linhas_total"] - data_pedido["qtd_nulos"]


# ---------------------------------------------------------------------- #
# Teto de colunas (item 3, proteção de custo): tabela larga não deve rodar
# a matriz de correlação (nem, por consequência, a busca de FD).
# ---------------------------------------------------------------------- #


@pytest.fixture
def caminho_duckdb_tabela_larga(tmp_path):
    caminho = str(tmp_path / "larga.duckdb")
    con = duckdb.connect(caminho)
    qtd_colunas = 65
    colunas_sql = ", ".join(f"c{i} VARCHAR" for i in range(qtd_colunas))
    con.execute(f"CREATE TABLE larga ({colunas_sql})")
    valores = ", ".join("'a'" for _ in range(qtd_colunas))
    for _ in range(10):
        con.execute(f"INSERT INTO larga VALUES ({valores})")
    con.close()
    return caminho


def test_tabela_larga_pula_matriz_de_correlacao(caminho_duckdb_tabela_larga):
    full = DuckDBProfiler(caminho_duckdb_tabela_larga).profile_full("larga")
    assert full["correlacoes"] == {}
    assert full["dependencias_funcionais"] == []


# ---------------------------------------------------------------------- #
# Tetos de custo configuráveis por variável de ambiente (sem alterar o
# código): como são lidos no import do módulo, o teste precisa setar a env
# var ANTES de recarregar o módulo, e desfazer os dois no final.
# ---------------------------------------------------------------------- #


def test_teto_de_colunas_e_configuravel_via_env(monkeypatch, caminho_duckdb_tabela_larga):
    import importlib

    import profilers.duckdb_profiler as modulo_profiler

    monkeypatch.setenv("DQP_TETO_COLUNAS_CORRELACAO", "100")
    try:
        importlib.reload(modulo_profiler)
        full = modulo_profiler.DuckDBProfiler(caminho_duckdb_tabela_larga).profile_full("larga")
        # 65 colunas, todas categóricas: acima do padrão de 60, mas abaixo do
        # teto de 100 vindo da env var, então a matriz de correlação roda.
        assert full["correlacoes"] != {}
    finally:
        monkeypatch.delenv("DQP_TETO_COLUNAS_CORRELACAO", raising=False)
        importlib.reload(modulo_profiler)


# ---------------------------------------------------------------------- #
# Regressões levantadas em Bugs_e_Proximos_Passos_dqp_2026-10-21.txt, a
# partir da rodada real contra as 26 bases jm_dayane_* (SAP/jmacedo).
# ---------------------------------------------------------------------- #


@pytest.fixture
def caminho_duckdb_tipos(tmp_path):
    caminho = str(tmp_path / "tipos.duckdb")
    con = duckdb.connect(caminho)
    con.execute(
        """
        CREATE TABLE medidas (
            valor_fracionario DOUBLE,     -- A2: 0.0/0.x não pode virar Identifier
            data_aaaammdd BIGINT,         -- B6: data disfarçada de inteiro
            hora_evento VARCHAR,          -- A6: só a hora varia, data sempre 1970-01-01
            data_com_hora VARCHAR,        -- A7: mesmo dia, horas distintas
            dia VARCHAR,                  -- A1: nome da coluna colide com o alias do GROUP BY antigo
            sempre_nula VARCHAR,          -- A4/B7: 100% nula
            as_vezes_nula VARCHAR         -- A4: nulo parcial, deve continuar na matriz
        )
        """
    )
    linhas = []
    for i in range(60):
        valor_fracionario = round((i % 7) / 10, 2)  # inclui 0.0, 0.1, ..., 0.6, casa com '0%' da regra antiga
        data_aaaammdd = 20260101 + (i % 28)  # datas válidas de janeiro/2026, largura sempre 8
        hora_evento = f"1970-01-01T{(i % 24):02d}:00:00Z"
        # só 2 dias civis distintos (2026-02-01 e 2026-02-04, gap real de 3
        # dias), cada um com 2 horários diferentes: testa se o intervalo é
        # calculado por DIA, não por timestamp (teria 4 timestamps distintos
        # com 2 gaps de 0 dia se contasse errado)
        dia_civil = "2026-02-01" if i % 2 == 0 else "2026-02-04"
        data_com_hora = f"{dia_civil}T{(i % 2) * 10:02d}:00:00Z"
        dia = f"2026-03-{1 + (i % 28):02d}"
        as_vezes_nula = None if i % 10 == 0 else f"v{i}"
        linhas.append((valor_fracionario, data_aaaammdd, hora_evento, data_com_hora, dia, None, as_vezes_nula))
    con.executemany("INSERT INTO medidas VALUES (?, ?, ?, ?, ?, ?, ?)", linhas)
    con.close()
    return caminho


def test_coluna_fracionaria_com_zero_nao_vira_identifier(caminho_duckdb_tipos):
    """A2: CAST(0.0 AS VARCHAR) LIKE '0%' casava com a regra antiga de zero à
    esquerda, mesmo a coluna sendo DOUBLE de verdade (nunca foi texto, nunca
    perdeu dígito nenhum). Regressão do achado A2 (vbak.netwr, lips.ntgew etc.)."""
    full = DuckDBProfiler(caminho_duckdb_tipos).profile_full("medidas")
    valor = full["colunas"]["valor_fracionario"]
    assert valor["tipo_dado_inferido"] == "Numeric"
    assert "mean" in valor  # só Numeric ganha estatística numérica de verdade


def test_data_disfarcada_de_inteiro_vira_datetime(caminho_duckdb_tipos):
    """B6: AAAAMMDD (comum em SAP) não pode entrar em Pearson/média como se
    fosse uma medida — tem que ser decodificado como data de verdade."""
    full = DuckDBProfiler(caminho_duckdb_tipos).profile_full("medidas")
    data = full["colunas"]["data_aaaammdd"]
    assert data["tipo_dado_inferido"] == "DateTime"
    assert data["min"].year == 2026
    assert data["min"].month == 1
    assert data["n_invalid_dates"] == 0


def test_coluna_so_de_hora_vira_time_sem_ciclo(caminho_duckdb_tipos):
    """A6: data sempre 1970-01-01 (sentinela), só a hora varia: não é uma
    data de verdade, então dia da semana e intervalo não fazem sentido."""
    full = DuckDBProfiler(caminho_duckdb_tipos).profile_full("medidas")
    hora = full["colunas"]["hora_evento"]
    assert hora["tipo_dado_inferido"] == "Time"
    assert hora["distribuicao_dia_semana"] == {}
    assert hora["intervalo_minimo_dias"] is None


def test_intervalo_entre_datas_e_por_dia_nao_por_timestamp(caminho_duckdb_tipos):
    """A7: com DISTINCT sobre TIMESTAMP (código antigo), os 2 horários dentro
    de cada um dos 2 dias civis contam como 'datas distintas' extras, criando
    pares com intervalo 0 e derrubando o mínimo/mediano pra 0 — mesmo havendo
    um gap real de 3 dias entre os únicos 2 dias civis de verdade."""
    full = DuckDBProfiler(caminho_duckdb_tipos).profile_full("medidas")
    data_com_hora = full["colunas"]["data_com_hora"]
    assert data_com_hora["tipo_dado_inferido"] == "DateTime"
    assert data_com_hora["intervalo_minimo_dias"] == 3
    assert data_com_hora["intervalo_maximo_dias"] == 3


def test_dia_da_semana_nao_colide_quando_coluna_se_chama_dia(caminho_duckdb_tipos):
    """A1: GROUP BY "dia" (o apelido antigo) resolvia pela própria coluna
    "dia" da tabela em vez do apelido, colapsando a contagem na última data de
    cada dia da semana. GROUP BY 1 (posição) não tem esse problema."""
    full = DuckDBProfiler(caminho_duckdb_tipos).profile_full("medidas")
    dia = full["colunas"]["dia"]
    assert dia["tipo_dado_inferido"] == "DateTime"
    assert sum(dia["distribuicao_dia_semana"].values()) == dia["qtd_linhas_total"] - dia["qtd_nulos"]
    # nomes traduzidos (B11): nenhuma chave em inglês deveria sobrar
    assert all(nome in ORDEM_DIAS_SEMANA_PT for nome in dia["distribuicao_dia_semana"])


def test_coluna_100_por_cento_nula_fica_fora_do_padrao_ausencia(caminho_duckdb_tipos):
    """A4: indicador de nulo constante (100% nula) faz CORR() devolver NaN,
    que o Postgres aceita como NUMERIC válido e maior que qualquer número de
    verdade — some_nula não pode nem entrar na matriz."""
    full = DuckDBProfiler(caminho_duckdb_tipos).profile_full("medidas")
    padrao_ausencia = full["padrao_ausencia"]
    if padrao_ausencia:
        colunas_na_matriz = set(padrao_ausencia[0].keys())
        assert "sempre_nula" not in colunas_na_matriz
        assert "as_vezes_nula" in colunas_na_matriz


def test_metrica_de_colunas_totalmente_nulas(caminho_duckdb_tipos):
    """B7: hoje nenhum alerta nem resumo aponta coluna 100% nula (comum em
    exportação SAP) — essa contagem por base torna isso visível."""
    full = DuckDBProfiler(caminho_duckdb_tipos).profile_full("medidas")
    assert full["tabela"]["n_vars_totalmente_nulas"] == 1


@pytest.fixture
def caminho_duckdb_tabela_pequena(tmp_path):
    caminho = str(tmp_path / "pequena.duckdb")
    con = duckdb.connect(caminho)
    con.execute("CREATE TABLE equipe (pedidos BIGINT)")
    con.executemany("INSERT INTO equipe VALUES (?)", [(100 + i,) for i in range(14)])
    con.close()
    return caminho


def test_tabela_pequena_nao_dispara_identifier_por_formato(caminho_duckdb_tabela_pequena):
    """A3: em tabela com poucas linhas, praticamente qualquer coluna tem
    quase todo valor distinto — o sinal de formato sozinho não é confiável
    aí. Regressão do achado A3 (vw_equipe_agregado.pedidos, 14 linhas)."""
    full = DuckDBProfiler(caminho_duckdb_tabela_pequena).profile_full("equipe")
    pedidos = full["colunas"]["pedidos"]
    assert pedidos["tipo_dado_inferido"] == "Numeric"


@pytest.fixture
def caminho_duckdb_fd_identifier(tmp_path):
    caminho = str(tmp_path / "fd_identifier.duckdb")
    con = duckdb.connect(caminho)
    con.execute(
        """
        CREATE TABLE cadastro (
            raiz_cnpj VARCHAR,  -- Identifier (zero à esquerda real): origem da FD
            nome_fantasia VARCHAR
        )
        """
    )
    raiz_para_nome = {f"{i:08d}": f"empresa_{i}" for i in range(6)}
    linhas = []
    for i, (raiz, nome) in enumerate(raiz_para_nome.items()):
        for _ in range(5):  # várias filiais por raiz: grupos com mais de 1 linha
            linhas.append((raiz, nome))
    con.executemany("INSERT INTO cadastro VALUES (?, ?)", linhas)
    con.close()
    return caminho


def test_dependencia_funcional_aceita_identifier_como_origem(caminho_duckdb_fd_identifier):
    """B2: antes, Identifier nunca entrava no universo do Cramér's V, então
    nunca virava candidato a FD — raiz_cnpj -> nome_fantasia (exemplo real do
    levantamento) não podia nem ser testado."""
    full = DuckDBProfiler(caminho_duckdb_fd_identifier).profile_full("cadastro")
    assert full["colunas"]["raiz_cnpj"]["tipo_dado_inferido"] == "Identifier"
    achados = full["dependencias_funcionais"]
    achado = next((a for a in achados if a["origem"] == "raiz_cnpj" and a["destino"] == "nome_fantasia"), None)
    assert achado is not None
    assert achado["percentual_grupos_sem_excecao"] == 100.0
    assert achado["qtd_linhas_avaliadas"] == 30


@pytest.fixture
def caminho_duckdb_fd_poucos_grupos(tmp_path):
    caminho = str(tmp_path / "fd_poucos_grupos.duckdb")
    con = duckdb.connect(caminho)
    con.execute("CREATE TABLE cadastro (status VARCHAR, ativo VARCHAR)")
    # só 2 grupos de origem: "dependência" trivial (quase qualquer coluna
    # binária "determina" outra binária por pura coincidência).
    linhas = [("A", "sim")] * 20 + [("B", "nao")] * 20
    con.executemany("INSERT INTO cadastro VALUES (?, ?)", linhas)
    con.close()
    return caminho


def test_dependencia_funcional_com_poucos_grupos_nao_e_reportada(caminho_duckdb_fd_poucos_grupos):
    """B3: com só 2 grupos de origem, a 'regra' é quase trivial — não deveria
    virar achado reportado."""
    full = DuckDBProfiler(caminho_duckdb_fd_poucos_grupos).profile_full("cadastro")
    assert not any(a["origem"] == "status" and a["destino"] == "ativo" for a in full["dependencias_funcionais"])
