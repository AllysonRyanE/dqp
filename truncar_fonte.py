"""
Remove do data mart todos os registros de uma fonte (dim_base, dim_atributo
e os fatos associados a ela), sem apagar o schema em si. Útil sobretudo em
ambiente de teste: uma fonte foi perfilada sob um schema antigo (sem colunas/
tabelas novas que esse trabalho foi acrescentando) e precisa ser descartada
antes de rodar a pipeline de novo.

Por padrão o comando só MOSTRA o que seria apagado (modo dry-run, nenhuma
linha é tocada). Passe --confirmar pra executar de fato, dentro de uma única
transação (tudo ou nada: qualquer erro no meio desfaz tudo via rollback).

FONTE aceita tanto um nome exato de fonte (sistema_origem, o mesmo valor que
persistir_datamart_duckdb.py grava) quanto um padrão SQL LIKE, quando contém
"%": útil quando várias seções de fontes.ini compartilham um prefixo comum,
como várias tabelas de uma mesma origem cadastradas uma a uma (ex.:
FONTE='jm_dayane_%' apaga as dezenas de fontes "jm_dayane_*" de uma vez, em
vez de rodar o comando uma vez por tabela).

Uso:
    FONTE=<nome_ou_padrao> .venv/bin/python truncar_fonte.py [--confirmar]

Exemplos:
    FONTE=jm_dayane_kna1 .venv/bin/python truncar_fonte.py              # só mostra o que seria apagado
    FONTE=jm_dayane_kna1 .venv/bin/python truncar_fonte.py --confirmar  # apaga de fato
    FONTE='jm_dayane_%' .venv/bin/python truncar_fonte.py --confirmar   # apaga todas as fontes com esse prefixo
"""

import os
import sys
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# Mesma ordem de dependência usada no restante do projeto (satélites antes
# do fato principal, fato principal antes de dim_atributo, e dim_base por
# último): respeita as FKs sem precisar de CASCADE no schema.
TABELAS_SATELITE_E_FATO_EM_ORDEM = [
    "fato_regra_aplicada",
    "fato_duplicatas",
    "fato_dependencia_funcional",
    "fato_padrao_ausencia",
    "fato_correlacao",
    "fato_perfilamento_coluna",
    "fato_perfilamento_base",
    "config_regra_usuario",
    "dim_atributo",
]


class _TeeStdout:
    def __init__(self, destino_original, caminho_arquivo):
        self._original = destino_original
        self._arquivo = open(caminho_arquivo, "a", encoding="utf-8")

    def write(self, texto):
        self._original.write(texto)
        self._arquivo.write(texto)
        return len(texto)

    def flush(self):
        self._original.flush()
        self._arquivo.flush()


def main():
    fonte = os.environ.get("FONTE", "").strip()
    if not fonte:
        print("FONTE não definida: ex.: FONTE=jm_dayane_kna1 .venv/bin/python truncar_fonte.py")
        print("Também aceita um padrão SQL LIKE: FONTE='jm_dayane_%' apaga várias fontes de uma vez.")
        sys.exit(1)

    confirmar = "--confirmar" in sys.argv

    os.makedirs("logs", exist_ok=True)
    timestamp_log = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho_log = f"logs/truncar_fonte_{fonte.replace('%', 'PADRAO')}_{timestamp_log}.log"
    logger.add(caminho_log, level="DEBUG", encoding="utf-8")
    sys.stdout = _TeeStdout(sys.stdout, caminho_log)

    conn = psycopg2.connect(
        host=os.environ["PG_DATAMART_HOST"],
        port=os.environ["PG_DATAMART_PORT"],
        dbname=os.environ["PG_DATAMART_DATABASE"],
        user=os.environ["PG_DATAMART_USER"],
        password=os.environ["PG_DATAMART_PASSWORD"],
    )
    cur = conn.cursor()

    operador = "LIKE" if "%" in fonte else "="
    cur.execute(
        f"SELECT sk_base, nome_base, sistema_origem FROM dim_base WHERE sistema_origem {operador} %s ORDER BY sistema_origem",
        (fonte,),
    )
    bases = cur.fetchall()

    if not bases:
        print(f"Nenhuma base encontrada para sistema_origem {operador} '{fonte}'. Nada a fazer.")
        conn.close()
        return

    print(f"{len(bases)} base(s) encontrada(s) para sistema_origem {operador} '{fonte}':")
    for sk, nome, sistema in bases:
        print(f"  sk_base={sk}  sistema_origem={sistema}  nome_base={nome}")

    sks = tuple(b[0] for b in bases)
    print()
    for tabela in TABELAS_SATELITE_E_FATO_EM_ORDEM:
        cur.execute(f"SELECT COUNT(*) FROM {tabela} WHERE sk_base IN %s", (sks,))
        print(f"  {tabela}: {cur.fetchone()[0]} linha(s) seria(m) apagada(s)")
    print(f"  dim_base: {len(bases)} linha(s) seria(m) apagada(s)")

    if not confirmar:
        print("\nModo dry-run: nada foi apagado. Rode de novo com --confirmar pra executar de fato.")
        conn.close()
        return

    try:
        for tabela in TABELAS_SATELITE_E_FATO_EM_ORDEM:
            cur.execute(f"DELETE FROM {tabela} WHERE sk_base IN %s", (sks,))
            print(f"{tabela}: {cur.rowcount} linha(s) apagada(s)")
        cur.execute("DELETE FROM dim_base WHERE sk_base IN %s", (sks,))
        print(f"dim_base: {cur.rowcount} linha(s) apagada(s)")
        conn.commit()
        print("COMMIT ok.")
    except Exception:
        conn.rollback()
        print("ERRO: rollback feito, nada foi apagado.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
