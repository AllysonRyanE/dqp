# dqp: Data Quality Profiler

Ferramenta de linha de comando que automatiza o perfilamento e a avaliação de qualidade de dados em bases volumosas e heterogêneas, desenvolvida no contexto da Plataforma Big Data PE da Agência Estadual de Tecnologia da Informação (ATI-PE).

Este README é um guia rápido de instalação e uso. A descrição completa (motivação, arquitetura, dimensões de qualidade avaliadas) está no TCC; aqui o foco é colocar a ferramenta rodando.

## Visão geral

São três passos para colocar o dqp em operação numa máquina nova, pela primeira vez, e duas funcionalidades complementares que podem ser usadas a qualquer momento depois disso:

1. Preparar o ambiente (clonar o repositório, instalar dependências, configurar credenciais).
2. Executar a pipeline (extração, perfilamento, pontuação e persistência).
3. Consultar os resultados persistidos.
4. *(opcional)* Configurar regras de negócio específicas de cada base.
5. *(opcional)* Gerar relatórios HTML a partir do que já foi persistido.

## Requisitos

- Python 3.10+
- PostgreSQL (data mart onde os resultados são persistidos)
- Acesso de rede à(s) fonte(s) de dados que você quer perfilar (Trino, Postgres, MySQL, SQL Server, Oracle, ou um caminho local/remoto de arquivos Parquet)

## Passo 1: preparar o ambiente

```bash
git clone https://github.com/AllysonRyanE/dqp dqp
cd dqp

# ambiente virtual + dependências
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

`.env` e `fontes.ini` nunca vão para o repositório (estão os dois no `.gitignore`); copie os modelos e preencha suas próprias credenciais:

```bash
cp .env.example .env
cp fontes.ini.example fontes.ini
# edite o .env (FONTE e PG_DATAMART_*) e o fontes.ini (uma seção por fonte, ver Quadro 1 abaixo)
```

O dqp conecta-se a seis tipos de fonte diferentes (trino, postgres, mysql, sqlserver, oracle, ou arquivos parquet). A variável de ambiente `FONTE` escolhe, a cada execução, qual seção do `fontes.ini` usar. Isso permite ter várias fontes do mesmo tipo ao mesmo tempo no mesmo arquivo, cada uma com suas próprias credenciais, sem conflito entre elas.

Quadro 1: campos de cada seção de `fontes.ini`, por tipo de conector a que se aplicam.

| Campo | Aplica-se a |
|---|---|
| `tipo` | todos (obrigatório, decide qual conector roda) |
| `host` | trino, postgres, mysql, sqlserver, oracle |
| `port` | trino, postgres, mysql, sqlserver, oracle |
| `user` | trino, postgres, mysql, sqlserver, oracle |
| `password` | trino, postgres, mysql, sqlserver, oracle |
| `database` | postgres, mysql, sqlserver |
| `catalog` | trino |
| `service_name` | oracle |
| `schema` | trino, postgres, oracle, sqlserver (opcional, padrão `dbo`) |
| `path` | parquet: aceita um arquivo, um glob (`*`, `**`) ou um diretório comum (ver nota abaixo) |
| `tables` | todos; se ausente, assume `TABLES=TODAS` e perfila tudo (exceto parquet, que deriva o nome da tabela do próprio `path` quando ausente) |

> Atenção para parquet particionado em múltiplos níveis de subpastas (ex. `ano=2024/mes=01/dia=15/hora=09`). Se `path` apontar pra um diretório comum sem nenhum glob, o dqp só varre os arquivos diretamente dentro dele, sem descer em subpastas. Para múltiplos níveis, digite o glob manualmente: `path = dados/**/*.parquet` (recursivo) ou `path = dados/ano=*/mes=*/dia=*/hora=*/*.parquet` (níveis explícitos). O DuckDB trata o conjunto inteiro como uma única tabela.

Os resultados são sempre persistidos no mesmo data mart (PostgreSQL), independente da fonte escolhida, configurado pelas variáveis `PG_DATAMART_*` no `.env`. Duas formas de conseguir esse Postgres: se você já tem um instalado e rodando na máquina, a Opção B é mais direta, só criar usuário e banco nele. Se não tem, a Opção A sobe um Postgres isolado via Docker, sem precisar instalar nada no sistema.

**Opção A: Docker**

```bash
docker compose up -d
# depois, no .env: PG_DATAMART_HOST=localhost, PG_DATAMART_PORT=5432,
# PG_DATAMART_USER=dqp_user, PG_DATAMART_PASSWORD=dqp_senha, PG_DATAMART_DATABASE=dqp_datamart
```

O esquema (`sql/create_schema.sql`) roda sozinho na primeira subida do container. Nenhum passo manual a mais.

**Opção B: Postgres já instalado localmente**

```bash
sudo -u postgres psql -c "CREATE USER dqp_user WITH PASSWORD 'sua_senha_aqui';"
sudo -u postgres psql -c "CREATE DATABASE dqp_datamart OWNER dqp_user;"
psql -h localhost -U dqp_user -d dqp_datamart -f sql/create_schema.sql
```

## Passo 2: executar a pipeline

A pipeline extrai os dados da fonte configurada, perfila-os via DuckDB, avalia as regras de negócio ativas (se houver), calcula os scores de qualidade e persiste tudo no data mart.

```bash
# perfila as tabelas listadas no campo "tables" da seção da FONTE em fontes.ini
FONTE=<nome> .venv/bin/python persistir_datamart_duckdb.py

# ou passando tabelas diretamente na linha de comando (sobrescreve fontes.ini pontualmente)
FONTE=<nome> .venv/bin/python persistir_datamart_duckdb.py tabela1,tabela2

# ou perfilando todas as tabelas descobertas automaticamente no schema/diretório
FONTE=<nome> TABLES=TODAS .venv/bin/python persistir_datamart_duckdb.py
```

Se `tables` não estiver preenchido em `fontes.ini`, a pipeline assume `TABLES=TODAS` automaticamente. Também é possível definir uma `FONTE` padrão no próprio `.env`, usada quando a variável não é passada na linha de comando.

## Passo 3: consultar os resultados

Direto no PostgreSQL do data mart, com as credenciais do `.env` (`PG_DATAMART_*`), por linha de comando ou por ferramentas gráficas (DBeaver, pgAdmin):

```sql
-- score por execução, mais recente primeiro
SELECT nome_base, data_hora_execucao, score_final_base, classificacao_final_base
FROM vw_perfilamento_base ORDER BY data_hora_execucao DESC;
```

Para quem prefere não escrever SQL, veja a seção de relatórios HTML abaixo.

## Funcionalidade opcional: regras de negócio

O dqp permite configurar regras de negócio específicas do domínio de cada base (ex. uma coluna que não deve ter valores negativos, ou um intervalo de datas esperado), avaliadas automaticamente a cada nova execução da pipeline:

```bash
.venv/bin/streamlit run app_regras.py
```

A interface lista as regras já configuradas para a base escolhida, permite ativar/desativar/excluir cada uma, e oferece uma aba para cadastrar uma regra nova dentre treze tipos disponíveis. Regras salvas entram em vigor na próxima execução da pipeline, sem exigir nenhuma alteração de código.

## Funcionalidade opcional: relatórios HTML

Gera relatórios HTML autocontidos (um executivo e um detalhado) a partir do que já está persistido no data mart, sem precisar perfilar a base de novo:

```bash
.venv/bin/python gerar_relatorios_pt.py
# ou só para bases específicas:
.venv/bin/python gerar_relatorios_pt.py tabela1 tabela2
```

Os arquivos são salvos em `relatorios/`, prontos para abrir em qualquer navegador.

## Testes

```bash
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m pytest tests/
```

A suíte cobre a inferência de tipo e as métricas do profiler (`src/profilers/duckdb_profiler.py`), a pontuação de qualidade (`src/quality/quality_scorer.py`) e a geração de alertas automáticos (`src/quality/system_alerts.py`). Tudo sobre dados sintéticos, sem depender de nenhuma fonte externa.

## Licença

MIT. Ver [LICENSE](LICENSE).

## Autores

Allyson Ryan Emiliano da Silva, Breno Alexandre de Albuquerque Santos, Sheyla Lima Leal de Souza, Robson do Nascimento Fidalgo. Centro de Informática, Universidade Federal de Pernambuco (UFPE).
