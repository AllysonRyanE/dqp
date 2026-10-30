-- Data Mart de Perfilamento de Qualidade de Dados (dqp)
-- Esquema estrela. Grão principal: 1 coluna perfilada x 1 execução.
-- Ver arquitetura completa validada em conversa (dimensões, fatos, JSONB reduzido).

-- =============================================================================
-- DIMENSÕES
-- =============================================================================

-- Quando a execução aconteceu (grão: dia)
CREATE TABLE dim_calendario (
    sk_calendario INTEGER PRIMARY KEY,          -- YYYYMMDD
    data DATE NOT NULL UNIQUE,
    ano SMALLINT NOT NULL,
    trimestre SMALLINT NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    mes SMALLINT NOT NULL CHECK (mes BETWEEN 1 AND 12),
    nome_mes VARCHAR(20) NOT NULL,
    dia_mes SMALLINT NOT NULL CHECK (dia_mes BETWEEN 1 AND 31),
    dia_semana SMALLINT NOT NULL CHECK (dia_semana BETWEEN 0 AND 6),
    nome_dia_semana VARCHAR(20) NOT NULL,
    eh_fim_semana BOOLEAN NOT NULL
);

-- Metadados de uma rodada do profiling
CREATE TABLE dim_execucao (
    sk_execucao BIGSERIAL PRIMARY KEY,
    data_hora_execucao TIMESTAMP NOT NULL,
    ferramenta VARCHAR(50) NOT NULL DEFAULT 'ydata-profiling',
    versao_ferramenta VARCHAR(20),
    modo_coleta VARCHAR(20) NOT NULL CHECK (modo_coleta IN ('amostra', 'completo')),
    tamanho_amostra BIGINT,
    duracao_segundos NUMERIC(10, 2),
    config_json JSONB
);

CREATE INDEX idx_dim_execucao_data ON dim_execucao (data_hora_execucao);

-- A base/tabela perfilada
CREATE TABLE dim_base (
    sk_base SERIAL PRIMARY KEY,
    nome_base VARCHAR(200) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,        -- 'Trino', 'Postgres', 'CSV'...
    catalogo VARCHAR(100),
    schema_origem VARCHAR(100),
    dominio VARCHAR(100),
    descricao TEXT,
    UNIQUE (sistema_origem, catalogo, schema_origem, nome_base)
);

CREATE INDEX idx_dim_base_nome ON dim_base (nome_base);

-- Uma coluna dentro de uma base
CREATE TABLE dim_atributo (
    sk_atributo SERIAL PRIMARY KEY,
    sk_base INTEGER NOT NULL REFERENCES dim_base (sk_base),
    nome_coluna VARCHAR(200) NOT NULL,
    posicao_coluna INTEGER,

    -- Configuração de scoring definida pelo usuário (não é resultado de regra,
    -- é comportamento do score): ver fato_regra_aplicada para os RESULTADOS
    -- de validação (sistema + usuário).
    nulidade_esperada BOOLEAN NOT NULL DEFAULT FALSE,      -- suprime a penalidade de completude pra essa coluna
    desconsiderada BOOLEAN NOT NULL DEFAULT FALSE,         -- fora de Crítico/Regular/Ótimo, categoria "desconsiderada"

    UNIQUE (sk_base, nome_coluna)
);

CREATE INDEX idx_dim_atributo_base ON dim_atributo (sk_base);

-- =============================================================================
-- FATO PRINCIPAL: 1 linha = 1 coluna perfilada numa execução
-- =============================================================================

CREATE TABLE fato_perfilamento_coluna (
    sk_perfilamento_coluna BIGSERIAL PRIMARY KEY,
    sk_calendario INTEGER NOT NULL REFERENCES dim_calendario (sk_calendario),
    sk_execucao BIGINT NOT NULL REFERENCES dim_execucao (sk_execucao),
    sk_base INTEGER NOT NULL REFERENCES dim_base (sk_base),
    sk_atributo INTEGER NOT NULL REFERENCES dim_atributo (sk_atributo),

    -- Completude
    qtd_linhas_total BIGINT,
    qtd_valores_nulos BIGINT,
    percentual_valores_nulos NUMERIC(6, 3),

    -- Unicidade / cardinalidade
    qtd_valores_distintos BIGINT,
    percentual_valores_distintos NUMERIC(6, 3),
    qtd_valores_repetidos BIGINT,

    -- Tipagem
    tipo_dado_inferido VARCHAR(30),
    percentual_valores_parseaveis_numero NUMERIC(6, 3),
    percentual_valores_parseaveis_data NUMERIC(6, 3),

    -- Estatística descritiva (numéricos)
    valor_minimo NUMERIC,
    valor_maximo NUMERIC,
    data_minima TIMESTAMP,
    data_maxima TIMESTAMP,
    valor_medio NUMERIC,
    valor_mediano NUMERIC,
    desvio_padrao NUMERIC,
    variancia NUMERIC,
    assimetria NUMERIC,
    curtose NUMERIC,
    percentil_25 NUMERIC,
    percentil_75 NUMERIC,
    coeficiente_variacao NUMERIC,
    qtd_valores_zero BIGINT,
    percentual_valores_zero NUMERIC(6, 3),
    qtd_valores_negativos BIGINT,
    percentual_valores_negativos NUMERIC(6, 3),
    qtd_valores_infinitos BIGINT,

    -- Outliers
    qtd_outliers BIGINT,
    percentual_outliers NUMERIC(6, 3),

    -- Texto
    tamanho_medio_texto NUMERIC,
    tamanho_mediano_texto NUMERIC,
    tamanho_maximo_texto INTEGER,
    tamanho_minimo_texto INTEGER,
    qtd_caracteres_distintos BIGINT,

    -- Moda / concentração categórica
    valor_moda TEXT,
    frequencia_moda BIGINT,
    frequencia_minima_valor BIGINT,
    frequencia_mediana_valor NUMERIC,
    percentual_valor_mais_frequente NUMERIC(6, 3),
    indice_concentracao_categorica NUMERIC(6, 4),

    -- Datas
    qtd_datas_invalidas BIGINT,
    percentual_datas_invalidas NUMERIC(6, 3),
    distribuicao_dia_semana JSONB,       -- ex: {"Monday": 120, "Tuesday": 98, ...}
    intervalo_minimo_dias INTEGER,
    intervalo_mediano_dias NUMERIC,
    intervalo_maximo_dias INTEGER,

    -- Memória
    tamanho_memoria_bytes BIGINT,

    -- Scores (ISO/IEC 25012)
    score_completude NUMERIC(5, 2),
    score_unicidade NUMERIC(5, 2),
    score_consistencia NUMERIC(5, 2),
    score_acuracia NUMERIC(5, 2),
    score_atualidade NUMERIC(5, 2),
    score_conformidade_regras NUMERIC(5, 2),
    score_geral NUMERIC(5, 2),
    classificacao VARCHAR(10) CHECK (classificacao IN ('Crítico', 'Regular', 'Ótimo')),

    -- Histórico (grupo 2): NULL até existir execução anterior do mesmo atributo
    score_geral_execucao_anterior NUMERIC(5, 2),
    variacao_score_geral NUMERIC(6, 2),
    percentual_variacao_qtd_linhas NUMERIC(6, 3),
    variacao_percentual_valor_mais_frequente NUMERIC(6, 3),
    dias_desde_execucao_anterior INTEGER,
    qtd_execucoes_anteriores INTEGER,

    UNIQUE (sk_execucao, sk_atributo)
);

CREATE INDEX idx_fpc_atributo ON fato_perfilamento_coluna (sk_atributo);
CREATE INDEX idx_fpc_base ON fato_perfilamento_coluna (sk_base);
CREATE INDEX idx_fpc_calendario ON fato_perfilamento_coluna (sk_calendario);
CREATE INDEX idx_fpc_classificacao ON fato_perfilamento_coluna (classificacao);

-- =============================================================================
-- FATO AGREGADO: 1 linha = 1 base inteira numa execução
-- =============================================================================

CREATE TABLE fato_perfilamento_base (
    sk_perfilamento_base BIGSERIAL PRIMARY KEY,
    sk_calendario INTEGER NOT NULL REFERENCES dim_calendario (sk_calendario),
    sk_execucao BIGINT NOT NULL REFERENCES dim_execucao (sk_execucao),
    sk_base INTEGER NOT NULL REFERENCES dim_base (sk_base),

    qtd_linhas BIGINT,
    qtd_colunas INTEGER,
    qtd_celulas_nulas BIGINT,
    percentual_celulas_nulas NUMERIC(6, 3),
    qtd_colunas_com_nulo INTEGER,
    qtd_colunas_totalmente_nulas INTEGER,
    qtd_linhas_duplicadas BIGINT,
    percentual_linhas_duplicadas NUMERIC(6, 3),
    tamanho_memoria_bytes BIGINT,
    tamanho_medio_registro NUMERIC,
    tipos_coluna JSONB,          -- ex: {"Text": 30, "Numeric": 5}
    qtd_alertas INTEGER,
    alertas JSONB,               -- lista de strings

    score_final_base NUMERIC(5, 2),
    classificacao_final_base VARCHAR(10) CHECK (classificacao_final_base IN ('Crítico', 'Regular', 'Ótimo')),

    -- Histórico (grupo 2)
    score_final_execucao_anterior NUMERIC(5, 2),
    variacao_score_final NUMERIC(6, 2),
    percentual_variacao_qtd_linhas NUMERIC(6, 3),
    dias_desde_execucao_anterior INTEGER,
    qtd_execucoes_anteriores INTEGER,

    UNIQUE (sk_execucao, sk_base)
);

CREATE INDEX idx_fpb_base ON fato_perfilamento_base (sk_base);
CREATE INDEX idx_fpb_calendario ON fato_perfilamento_base (sk_calendario);

-- =============================================================================
-- FATOS SATÉLITE (grão diferente do principal)
-- =============================================================================

CREATE TABLE fato_correlacao (
    sk_correlacao BIGSERIAL PRIMARY KEY,
    sk_execucao BIGINT NOT NULL REFERENCES dim_execucao (sk_execucao),
    sk_base INTEGER NOT NULL REFERENCES dim_base (sk_base),
    coluna_a VARCHAR(200) NOT NULL,
    coluna_b VARCHAR(200) NOT NULL,
    metodo VARCHAR(30) NOT NULL,
    valor_correlacao NUMERIC(6, 4)
);

CREATE INDEX idx_fcorr_execucao ON fato_correlacao (sk_execucao);

-- Dependência funcional (origem -> destino): descoberta automática pelo
-- DuckDBProfiler, não regra declarada pelo usuário (ver config_regra_usuario
-- pra essa). Direcional, por isso coluna_origem/coluna_destino não são
-- intercambiáveis como em fato_correlacao: o achado A->B não implica B->A.
CREATE TABLE fato_dependencia_funcional (
    sk_dependencia_funcional BIGSERIAL PRIMARY KEY,
    sk_execucao BIGINT NOT NULL REFERENCES dim_execucao (sk_execucao),
    sk_base INTEGER NOT NULL REFERENCES dim_base (sk_base),
    coluna_origem VARCHAR(200) NOT NULL,
    coluna_destino VARCHAR(200) NOT NULL,
    qtd_grupos_origem BIGINT,
    percentual_grupos_sem_excecao NUMERIC(6, 3),
    qtd_linhas_excecao BIGINT,
    qtd_linhas_avaliadas BIGINT
);

CREATE INDEX idx_fdf_execucao ON fato_dependencia_funcional (sk_execucao);

CREATE TABLE fato_padrao_ausencia (
    sk_padrao_ausencia BIGSERIAL PRIMARY KEY,
    sk_execucao BIGINT NOT NULL REFERENCES dim_execucao (sk_execucao),
    sk_base INTEGER NOT NULL REFERENCES dim_base (sk_base),
    coluna_a VARCHAR(200) NOT NULL,
    coluna_b VARCHAR(200) NOT NULL,
    correlacao_nulidade NUMERIC(6, 4)
);

CREATE INDEX idx_fpa_execucao ON fato_padrao_ausencia (sk_execucao);

CREATE TABLE fato_duplicatas (
    sk_duplicata BIGSERIAL PRIMARY KEY,
    sk_execucao BIGINT NOT NULL REFERENCES dim_execucao (sk_execucao),
    sk_base INTEGER NOT NULL REFERENCES dim_base (sk_base),
    linha_duplicada JSONB NOT NULL
);

CREATE INDEX idx_fdup_execucao ON fato_duplicatas (sk_execucao);

-- =============================================================================
-- REGRAS: catálogo de tipos + resultado unificado (alertas automáticos do
-- sistema E regras configuradas pelo usuário caem na mesma tabela de fato,
-- diferenciadas por `origem`)
-- =============================================================================

CREATE TABLE dim_regra (
    sk_regra_tipo SERIAL PRIMARY KEY,
    codigo VARCHAR(60) NOT NULL UNIQUE,
    nome_exibicao VARCHAR(200) NOT NULL,
    descricao TEXT,
    categoria VARCHAR(20) NOT NULL CHECK (categoria IN ('usuario', 'sistema', 'ambos'))
);

INSERT INTO dim_regra (codigo, nome_exibicao, descricao, categoria) VALUES
    ('valores_distintos_entre', 'Quantidade de valores distintos esperada', 'Espera-se que a coluna tenha entre X e Y valores distintos (presets: poucos=10, muitos=1000)', 'usuario'),
    ('comprimento_maximo', 'Comprimento máximo de caracteres', 'Valores da coluna devem ter até X caracteres', 'usuario'),
    ('chave_composta_unica', 'Combinação de colunas deve ser única', 'A combinação dos valores de duas ou mais colunas não deve se repetir', 'usuario'),
    ('soma_colunas', 'Coluna deve ser a soma de outras', 'O valor da coluna Y deve ser igual à soma das colunas X e Z', 'usuario'),
    ('sem_negativos', 'Não deve haver valores negativos', 'Todos os valores da coluna devem ser >= 0', 'usuario'),
    ('intervalo_numerico', 'Valores devem estar num intervalo', 'Valores devem ser maiores/menores/estar entre X e Y', 'usuario'),
    ('intervalo_data', 'Datas devem estar num intervalo', 'Datas devem ser anteriores/posteriores/estar entre X e Y', 'usuario'),
    ('valores_aceitos', 'Lista de valores aceitáveis', 'Só os valores informados são considerados válidos', 'usuario'),
    ('valor_unico', 'Valores devem ser únicos', 'Nenhum valor pode se repetir na coluna', 'usuario'),
    ('formato_regex', 'Deve seguir um padrão/formato', 'Valores devem casar com uma expressão regular (ex: formato de CNPJ)', 'usuario'),
    ('existe_em_outra_tabela', 'Deve existir em outra tabela/coluna', 'Todo valor deve existir como valor de referência em outra base já perfilada', 'usuario'),
    ('completude_condicional', 'Completude condicional', 'Se a coluna A estiver preenchida, a coluna B também deve estar', 'usuario'),
    ('nulidade_aceitavel', 'Nulidade aceitável', 'Até X% de nulos na coluna é esperado/normal e não penaliza a completude: só o que passar da tolerância conta pro score', 'usuario'),
    ('distorcao_estatistica', 'Distorção estatística alta', 'Assimetria (skewness) fora do esperado', 'sistema'),
    ('curtose_extrema', 'Curtose extrema', 'Curtose fora do esperado: cauda muito pesada ou muito leve', 'sistema'),
    ('desequilibrio_categorico', 'Desequilíbrio categórico', 'Distribuição de categorias muito concentrada numa só', 'sistema'),
    ('correlacao_alta', 'Correlação alta entre colunas', 'Duas colunas muito correlacionadas: possível redundância', 'sistema'),
    ('cardinalidade_muito_alta', 'Cardinalidade muito alta', 'Quase todo valor é distinto: possível identificador/hash', 'sistema'),
    ('muitos_zeros', 'Muitos valores zero', 'Percentual alto de zeros na coluna', 'sistema'),
    ('muitos_negativos', 'Muitos valores negativos', 'Percentual alto de valores negativos na coluna', 'sistema'),
    ('tipo_fisico_divergente', 'Tipo físico divergente do conteúdo', 'Coluna declarada como texto no banco de origem, mas o conteúdo é majoritariamente parseável como número ou data', 'sistema'),
    ('dependencia_funcional_com_excecoes', 'Dependência funcional com exceções', 'Uma dependência funcional descoberta automaticamente (origem determina destino) tem linhas que quebram a regra, mesmo acima do limiar de aderência', 'sistema');

-- Regras de negócio que o usuário configurou (grão: 1 regra ativa por base).
-- `parametros` guarda os campos específicos de cada tipo (colunas envolvidas,
-- limites, regex, etc: o formato depende de `sk_regra_tipo`, ver
-- src/rules/user_rules_engine.py para o contrato de cada código).
CREATE TABLE config_regra_usuario (
    sk_config_regra BIGSERIAL PRIMARY KEY,
    sk_base INTEGER NOT NULL REFERENCES dim_base (sk_base),
    sk_regra_tipo INTEGER NOT NULL REFERENCES dim_regra (sk_regra_tipo),
    nome_regra VARCHAR(200),
    parametros JSONB NOT NULL,
    ativa BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMP NOT NULL DEFAULT NOW(),
    atualizado_em TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_cru_base_ativa ON config_regra_usuario (sk_base) WHERE ativa;

-- Resultado de uma regra aplicada (grão: regra x atributo/base x execução).
-- sk_atributo é NULL para regras de tabela inteira ou multi-coluna (nesse
-- caso, os nomes das colunas envolvidas ficam em `colunas_envolvidas`).
-- sk_config_regra é NULL para origem='sistema' (alertas automáticos não
-- vêm de uma regra configurada, são calculados direto das métricas).
CREATE TABLE fato_regra_aplicada (
    sk_regra_aplicada BIGSERIAL PRIMARY KEY,
    sk_execucao BIGINT NOT NULL REFERENCES dim_execucao (sk_execucao),
    sk_base INTEGER NOT NULL REFERENCES dim_base (sk_base),
    sk_atributo INTEGER REFERENCES dim_atributo (sk_atributo),
    sk_regra_tipo INTEGER NOT NULL REFERENCES dim_regra (sk_regra_tipo),
    sk_config_regra BIGINT REFERENCES config_regra_usuario (sk_config_regra),
    origem VARCHAR(10) NOT NULL CHECK (origem IN ('sistema', 'usuario')),
    colunas_envolvidas VARCHAR(500),
    parametros JSONB,
    passou BOOLEAN NOT NULL,
    percentual_conformidade NUMERIC(6, 3),
    qtd_violacoes BIGINT,
    mensagem TEXT
);

CREATE INDEX idx_fra_execucao ON fato_regra_aplicada (sk_execucao);
CREATE INDEX idx_fra_atributo ON fato_regra_aplicada (sk_atributo);
CREATE INDEX idx_fra_origem ON fato_regra_aplicada (origem);
CREATE INDEX idx_fra_tipo ON fato_regra_aplicada (sk_regra_tipo);

-- =============================================================================
-- VIEWS (conveniência: nome_base/nome_coluna já embutidos, sem JOIN manual)
-- =============================================================================

-- As três views abaixo mostram só a execução mais recente por base/atributo
-- (ROW_NUMBER() particionado, rn = 1): sem isso, uma base perfilada mais de
-- uma vez (comum em ambiente de teste, enquanto o schema ainda muda) aparece
-- duplicada em quem consulta sem filtrar por execução — aconteceu de verdade
-- com dependência funcional e padrão de ausência numa mesma análise (B8).
-- Quem precisar do histórico completo consulta a tabela fato diretamente.
CREATE OR REPLACE VIEW vw_perfilamento_base AS
WITH ultima AS (
    SELECT
        b.*, ex.data_hora_execucao, ex.modo_coleta,
        ROW_NUMBER() OVER (PARTITION BY b.sk_base ORDER BY ex.data_hora_execucao DESC) AS rn
    FROM fato_perfilamento_base b
    JOIN dim_execucao ex ON ex.sk_execucao = b.sk_execucao
)
SELECT
    d.nome_base, d.catalogo, d.schema_origem,
    u.data_hora_execucao, u.modo_coleta,
    u.qtd_linhas, u.qtd_colunas, u.qtd_celulas_nulas, u.percentual_celulas_nulas,
    u.qtd_colunas_com_nulo, u.qtd_linhas_duplicadas, u.percentual_linhas_duplicadas,
    pg_size_pretty(u.tamanho_memoria_bytes::bigint) AS tamanho_memoria,
    u.tamanho_medio_registro, u.tipos_coluna, u.qtd_alertas, u.alertas,
    u.score_final_base, u.classificacao_final_base,
    u.score_final_execucao_anterior, u.variacao_score_final,
    u.percentual_variacao_qtd_linhas, u.dias_desde_execucao_anterior, u.qtd_execucoes_anteriores,
    u.sk_perfilamento_base,
    u.qtd_colunas_totalmente_nulas
FROM ultima u
JOIN dim_base d ON d.sk_base = u.sk_base
WHERE u.rn = 1;

CREATE OR REPLACE VIEW vw_perfilamento_coluna AS
WITH ultima AS (
    SELECT
        f.*, ex.data_hora_execucao, ex.modo_coleta,
        ROW_NUMBER() OVER (PARTITION BY f.sk_atributo ORDER BY ex.data_hora_execucao DESC) AS rn
    FROM fato_perfilamento_coluna f
    JOIN dim_execucao ex ON ex.sk_execucao = f.sk_execucao
)
SELECT
    d.nome_base,
    a.nome_coluna, a.posicao_coluna,
    u.data_hora_execucao, u.modo_coleta,
    u.qtd_linhas_total, u.qtd_valores_nulos, u.percentual_valores_nulos,
    u.qtd_valores_distintos, u.percentual_valores_distintos, u.qtd_valores_repetidos,
    u.tipo_dado_inferido, u.percentual_valores_parseaveis_numero, u.percentual_valores_parseaveis_data,
    u.valor_minimo, u.valor_maximo, u.data_minima, u.data_maxima,
    u.valor_medio, u.valor_mediano, u.desvio_padrao, u.variancia,
    u.assimetria, u.curtose, u.percentil_25, u.percentil_75, u.coeficiente_variacao,
    u.qtd_valores_zero, u.percentual_valores_zero, u.qtd_valores_negativos, u.percentual_valores_negativos,
    u.qtd_valores_infinitos, u.qtd_outliers, u.percentual_outliers,
    u.tamanho_medio_texto, u.tamanho_mediano_texto, u.tamanho_maximo_texto, u.tamanho_minimo_texto,
    u.qtd_caracteres_distintos, u.valor_moda, u.frequencia_moda, u.percentual_valor_mais_frequente,
    u.indice_concentracao_categorica, u.qtd_datas_invalidas, u.percentual_datas_invalidas,
    pg_size_pretty(u.tamanho_memoria_bytes::bigint) AS tamanho_memoria,
    u.score_completude, u.score_unicidade, u.score_consistencia, u.score_acuracia, u.score_atualidade,
    u.score_conformidade_regras,
    u.score_geral, u.classificacao,
    u.score_geral_execucao_anterior, u.variacao_score_geral, u.percentual_variacao_qtd_linhas,
    u.variacao_percentual_valor_mais_frequente, u.dias_desde_execucao_anterior, u.qtd_execucoes_anteriores,
    u.sk_perfilamento_coluna,
    u.frequencia_minima_valor, u.frequencia_mediana_valor,
    u.distribuicao_dia_semana, u.intervalo_minimo_dias, u.intervalo_mediano_dias, u.intervalo_maximo_dias
FROM ultima u
JOIN dim_atributo a ON a.sk_atributo = u.sk_atributo
JOIN dim_base d ON d.sk_base = u.sk_base
WHERE u.rn = 1;

CREATE OR REPLACE VIEW vw_regras_aplicadas AS
WITH ultima_execucao_por_base AS (
    SELECT fra.sk_base, MAX(ex.data_hora_execucao) AS data_hora_execucao
    FROM fato_regra_aplicada fra
    JOIN dim_execucao ex ON ex.sk_execucao = fra.sk_execucao
    GROUP BY fra.sk_base
)
SELECT
    d.nome_base, a.nome_coluna,
    ex.data_hora_execucao,
    r.codigo AS regra, r.nome_exibicao, fra.origem,
    fra.colunas_envolvidas, fra.parametros,
    fra.passou, fra.percentual_conformidade, fra.qtd_violacoes, fra.mensagem
FROM fato_regra_aplicada fra
JOIN dim_regra r ON r.sk_regra_tipo = fra.sk_regra_tipo
JOIN dim_base d ON d.sk_base = fra.sk_base
JOIN dim_execucao ex ON ex.sk_execucao = fra.sk_execucao
JOIN ultima_execucao_por_base u ON u.sk_base = fra.sk_base AND u.data_hora_execucao = ex.data_hora_execucao
LEFT JOIN dim_atributo a ON a.sk_atributo = fra.sk_atributo;
