"""
Gera os relatórios HTML nativos em pt-BR (executivo + detalhado) pra cada
base já perfilada no data mart: não depende de ydata-profiling nem DuckDB
em tempo de geração, só lê o que já foi persistido por
persistir_datamart.py / persistir_datamart_duckdb.py.

Uso: .venv/bin/python gerar_relatorios_pt.py [nome_base ...]
Sem argumentos, gera pra todas as bases com execução persistida.
"""

import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, "src")

from reports.relatorio_pt import gerar_html_detalhado, gerar_html_executivo  # noqa: E402

load_dotenv()


def datamart_connection_string() -> str:
    return (
        f"postgresql+psycopg2://{os.environ['PG_DATAMART_USER']}:{os.environ['PG_DATAMART_PASSWORD']}"
        f"@{os.environ['PG_DATAMART_HOST']}:{os.environ['PG_DATAMART_PORT']}/{os.environ['PG_DATAMART_DATABASE']}"
    )


def listar_bases_perfiladas(engine) -> list:
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT DISTINCT nome_base FROM dim_base ORDER BY nome_base")).fetchall()
    return [r[0] for r in rows]


def main():
    engine = create_engine(datamart_connection_string())
    os.makedirs("relatorios", exist_ok=True)

    bases = sys.argv[1:] or listar_bases_perfiladas(engine)
    if not bases:
        print("Nenhuma base perfilada encontrada no data mart.")
        return

    for nome_base in bases:
        print(f"\n{nome_base}")
        try:
            html_executivo = gerar_html_executivo(engine, nome_base)
            caminho_executivo = f"relatorios/{nome_base}_executivo.html"
            with open(caminho_executivo, "w", encoding="utf-8") as f:
                f.write(html_executivo)
            print(f"  executivo  -> {os.path.abspath(caminho_executivo)}")

            html_detalhado = gerar_html_detalhado(engine, nome_base)
            caminho_detalhado = f"relatorios/{nome_base}_detalhado.html"
            with open(caminho_detalhado, "w", encoding="utf-8") as f:
                f.write(html_detalhado)
            print(f"  detalhado  -> {os.path.abspath(caminho_detalhado)}")
        except ValueError as e:
            print(f"  aviso: {e}")


if __name__ == "__main__":
    main()
