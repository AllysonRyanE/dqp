# Dicionário de dados: data mart `dqp_datamart`

Documenta cada coluna das tabelas do data mart: o que significa, de onde vem, e como é calculada. Ver `sql/create_schema.sql` pro DDL completo e `src/data_mart/loader.py` pra lógica de carga.

---

## Dimensões

### `dim_calendario`
Data da execução (grão: dia).

| Coluna | Significado |
|---|---|
| `sk_calendario` | Chave surrogate, formato `YYYYMMDD` |
| `data` | Data (sem hora) |
| `ano`, `trimestre`, `mes`, `nome_mes` | Partes da data, pra facilitar agregação em relatórios |
| `dia_mes`, `dia_semana`, `nome_dia_semana` | Idem |
| `eh_fim_semana` | Booleano |

### `dim_execucao`
Metadados de uma rodada da ferramenta (`persistir_datamart.py`).

| Coluna | Significado |
|---|---|
| `sk_execucao` | Chave surrogate (auto-incremento) |
| `data_hora_execucao` | Timestamp exato (mais granular que `dim_calendario`, usado pra ordenar histórico) |
| `ferramenta`, `versao_ferramenta` | Nome/versão do profiler usado (hoje sempre `ydata-profiling`, versão lida em runtime) |
| `modo_coleta` | `'completo'` (leu a tabela inteira) ou `'amostra'` (leu com `LIMIT`) |
| `tamanho_amostra` | Quantas linhas foram efetivamente lidas nessa execução |
| `duracao_segundos` | Tempo do `profiler.profile_full(df)`: **não** inclui conexão/fetch do Trino nem a gravação no Postgres |
| `config_json` | Config do ydata-profiling usada (JSONB): útil pra saber se `explorative`/`interactions` estavam ligados numa execução específica |

### `dim_base`
A tabela/fonte perfilada.

| Coluna | Significado |
|---|---|
| `sk_base` | Chave surrogate |
| `nome_base` | Nome da tabela na fonte |
| `sistema_origem` | `'Trino'`, `'Postgres'`, `'CSV'`... |
| `catalogo`, `schema_origem` | Localização dentro do sistema de origem (ex: catálogo `servico_hive`, schema `ingestedata`) |
| `dominio` | Categoria de negócio (ex: "Licitações", "Educação"): hoje não preenchido, campo livre pra uso futuro |

### `dim_atributo`
Uma coluna dentro de uma base. Chave natural = `(sk_base, nome_coluna)`: **se a coluna for renomeada na fonte, vira um atributo novo** (perde o histórico anterior, ver limitações no runbook).

| Coluna | Significado |
|---|---|
| `sk_atributo` | Chave surrogate |
| `sk_base` | FK pra `dim_base` |
| `nome_coluna` | Nome da coluna na fonte |
| `posicao_coluna` | Posição ordinal na tabela (0-indexed) |

---

## Fato principal: `fato_perfilamento_coluna`
**Grão: 1 linha = 1 coluna perfilada, numa execução.** Fonte: `_column_metrics()` em `ydata_profiler.py` → `QualityScorer.score()` → `DataMartLoader.salvar_perfilamento()`.

### Completude
| Coluna | Significado |
|---|---|
| `qtd_linhas_total` | Total de linhas da amostra/tabela nessa execução |
| `qtd_valores_nulos` | Quantidade de nulos na coluna |
| `percentual_valores_nulos` | `qtd_valores_nulos / qtd_linhas_total × 100` |

### Unicidade / cardinalidade
| Coluna | Significado |
|---|---|
| `qtd_valores_distintos` | Cardinalidade (valores únicos, ignorando nulos) |
| `percentual_valores_distintos` | Seletividade: `qtd_valores_distintos / qtd_linhas_total × 100` |
| `qtd_valores_repetidos` | `qtd_linhas_total − qtd_valores_distintos`: é um proxy de cardinalidade, **não** é "linha inteira duplicada" (isso é `fato_perfilamento_base.qtd_linhas_duplicadas`) |

### Tipagem
| Coluna | Significado |
|---|---|
| `tipo_dado_inferido` | Tipo que o ydata-profiling inferiu: `Numeric`, `Text`, `Categorical`, `DateTime`, `Boolean`, `Unsupported` |
| `percentual_valores_parseaveis_numero` | Só calculado se `tipo_dado_inferido` é `Text`/`Categorical`: % de valores que, mesmo vindo como texto, parseiam como número (`pd.to_numeric`). `NULL` se a coluna é quase-única (>90% valores distintos: guarda contra um segfault real do pandas em colunas tipo hash/ID; testamos e o gatilho é cardinalidade, não comprimento de string, ver bugs no runbook) |
| `percentual_valores_parseaveis_data` | Idem, mas testando `pd.to_datetime` |

### Estatística descritiva (só preenchido quando `tipo_dado_inferido = 'Numeric'`)
| Coluna | Significado |
|---|---|
| `valor_minimo`, `valor_maximo` | Min/max |
| `valor_medio`, `valor_mediano` | Média, mediana (percentil 50) |
| `desvio_padrao`, `variancia` | Dispersão |
| `assimetria` | Skewness |
| `curtose` | Kurtosis |
| `percentil_25`, `percentil_75` | Quartis |
| `coeficiente_variacao` | `desvio_padrao / valor_medio` |
| `qtd_valores_zero`, `percentual_valores_zero` | Contagem/% de zeros |
| `qtd_valores_negativos`, `percentual_valores_negativos` | Idem, negativos |
| `qtd_valores_infinitos` | Valores `inf`/`-inf` |

### Datas (só preenchido quando `tipo_dado_inferido = 'DateTime'`)
| Coluna | Significado |
|---|---|
| `data_minima`, `data_maxima` | Min/max (equivalente a `valor_minimo`/`valor_maximo`, mas em `TIMESTAMP`: colunas separadas porque não dá pra misturar tipo com `NUMERIC`) |
| `qtd_datas_invalidas`, `percentual_datas_invalidas` | Valores que não conformam ao formato de data esperado: é a base do score de consistência pra colunas de data |

### Outliers (só preenchido quando `tipo_dado_inferido = 'Numeric'`)
| Coluna | Significado |
|---|---|
| `qtd_outliers` | Contagem pelo método IQR (1,5× o intervalo interquartil) |
| `percentual_outliers` | `qtd_outliers / qtd_linhas_total × 100`: alimenta diretamente o `score_acuracia` |

### Texto
| Coluna | Significado |
|---|---|
| `tamanho_medio_texto`, `tamanho_mediano_texto`, `tamanho_maximo_texto`, `tamanho_minimo_texto` | Comprimento de string (caracteres) |
| `qtd_caracteres_distintos` | Quantidade de caracteres únicos usados na coluna inteira |

### Moda / concentração categórica
| Coluna | Significado |
|---|---|
| `valor_moda` | Valor mais frequente (sempre armazenado como texto, `str()`) |
| `frequencia_moda` | Quantas vezes o valor mais frequente aparece |
| `percentual_valor_mais_frequente` | `frequencia_moda / total_nao_nulo × 100` |
| `indice_concentracao_categorica` | Índice de Herfindahl (soma das proporções ao quadrado): 0 = disperso, 1 = uma categoria domina tudo |

### Memória
| Coluna | Significado |
|---|---|
| `tamanho_memoria_bytes` | Memória ocupada pela coluna em bytes (estimativa do pandas/ydata) |

### Scores (ISO/IEC 25012)
| Coluna | Significado |
|---|---|
| `score_completude` | `100 × (1 − percentual_valores_nulos/100)`. Sempre calculado, sempre entra no `score_geral` |
| `score_unicidade` | `100 × (1 − percentual_valores_repetidos/100)`. Calculado e guardado, mas **não entra** no `score_geral`: é subjetivo demais (uma coluna categoria/FK pode repetir muito por natureza, sem isso ser defeito) |
| `score_consistencia` | Coerência interna de tipagem: ver seção "Reformulação" abaixo. `NULL` quando não se aplica ao tipo da coluna (`Numeric`) ou quando não deu pra medir (coluna quase-única, ver guarda de cardinalidade mais abaixo) |
| `score_acuracia` | `100 × (1 − percentual_outliers/100)`. Só calculado quando `tipo_dado_inferido = 'Numeric'`: `NULL` nas demais (a métrica de outlier via IQR simplesmente não existe pra Text/Categorical/DateTime) |
| `score_atualidade` | Só calculado quando `tipo_dado_inferido = 'DateTime'`, baseado em quão recente é o valor máximo de data da própria coluna (dias corridos até hoje). `NULL` nas demais colunas: não é "há quanto tempo a base foi atualizada", é o frescor daquela coluna de data especificamente. **Não entra** na média do `score_geral`: é informativo, não é dimensão universal de qualidade (nem toda coluna tem data pra medir isso) |
| `score_geral` | Média ponderada **dinâmica** entre completude, consistência e acurácia (pesos-base `6/15`, `5/15`, `4/15`: derivados de 30/25/25/20 originais sem unicidade). Quando `score_consistencia` e/ou `score_acuracia` são `NULL` pra aquela coluna, eles saem da conta e o peso dos que restam é renormalizado na hora (dividido pela soma dos pesos válidos), em vez de contar um 100 automático pra dimensão nunca avaliada: isso é o que originalmente inflava quase toda coluna pra Ótimo |
| `classificacao` | `'Crítico'` (<70), `'Regular'` (70–85) ou `'Ótimo'` (≥85) |

**Reformulação da consistência**: não é "violação de regra de negócio" (isso exigiria regra manual por dataset, fora de escopo). É coerência interna de tipagem:
- `DateTime`: `100 − percentual_datas_invalidas`
- `Text`/`Categorical`: quanto mais a coluna estiver "no meio do caminho" entre parecer número/data e não parecer (ex: 50% parseável), pior o score. 0% ou 100% parseável = consistente (mesmo que seja o tipo "errado": isso é avaliado em `tipo_dado_inferido`, não aqui)
- `Numeric` e demais: neutro (100), dimensão não se aplica

### Histórico (grupo 2): `NULL` até existir execução anterior do mesmo atributo
| Coluna | Significado |
|---|---|
| `score_geral_execucao_anterior` | `score_geral` da execução anterior desse mesmo `sk_atributo` |
| `variacao_score_geral` | `score_geral atual − score_geral_execucao_anterior` |
| `percentual_variacao_qtd_linhas` | Drift de volume: variação % de `qtd_linhas_total` vs. execução anterior |
| `variacao_percentual_valor_mais_frequente` | Proxy de drift de distribuição categórica |
| `dias_desde_execucao_anterior` | Recência real (dias corridos) |
| `qtd_execucoes_anteriores` | Quantas vezes esse atributo já foi perfilado antes |

**Cauda longa (histograma, value_counts, etc.)**: não é persistida no data mart. Existiu uma coluna `metricas_detalhadas` (JSONB) nessa finalidade, mas foi removida: o campo `value_counts_without_nan` inchava até 5 MB por linha em colunas de alta cardinalidade (ex: `__hashkey__`), sem benefício real de análise proporcional ao custo. Se precisar dessa cauda longa pontualmente, gere o relatório HTML (`gerar_relatorio_html.py`), que mostra tudo isso visualmente sem persistir nada.

---

## Fato agregado: `fato_perfilamento_base`
**Grão: 1 linha = 1 base inteira, numa execução** (não por coluna).

| Coluna | Significado |
|---|---|
| `qtd_linhas`, `qtd_colunas` | Tamanho da tabela nessa execução |
| `qtd_celulas_nulas`, `percentual_celulas_nulas` | Nulos agregados de todas as colunas |
| `qtd_colunas_com_nulo` | Quantas colunas têm pelo menos 1 nulo |
| `qtd_linhas_duplicadas`, `percentual_linhas_duplicadas` | Linha **inteira** duplicada (diferente de `qtd_valores_repetidos` por coluna) |
| `tamanho_memoria_bytes`, `tamanho_medio_registro` | Memória total / por registro |
| `tipos_coluna` | JSONB tipo `{"Text": 30, "Numeric": 5}`: contagem de colunas por tipo inferido |
| `qtd_alertas`, `alertas` | Quantidade e lista (JSONB) dos alertas que o ydata-profiling gerou |
| `score_final_base` | `0,85 × média(score_geral das colunas) + 0,15 × (100 − percentual_linhas_duplicadas)` |
| `classificacao_final_base` | Crítico/Regular/Ótimo do `score_final_base` |
| `score_final_execucao_anterior`, `variacao_score_final`, `percentual_variacao_qtd_linhas`, `dias_desde_execucao_anterior`, `qtd_execucoes_anteriores` | Histórico (grupo 2), mesma lógica da fato de coluna mas no nível da base |

---

## Fatos satélite

### `fato_correlacao`
Grão: par de colunas × execução × método. `coluna_a`/`coluna_b` (nomes), `metodo` (ex: `'auto'`), `valor_correlacao`. Só o triângulo superior da matriz é gravado (sem diagonal, sem repetir o par invertido).

### `fato_padrao_ausencia`
Grão: par de colunas × execução. `correlacao_nulidade`: o quanto a ausência de uma coluna coincide com a ausência de outra (`df.isna().corr()`). Só existe pra colunas que de fato têm algum nulo.

### `fato_duplicatas`
Grão: linha duplicada × execução. `linha_duplicada` (JSONB): o conteúdo bruto de uma linha que apareceu duplicada na amostra.
