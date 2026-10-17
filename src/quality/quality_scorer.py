from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class QualityScores:
    """Scores de qualidade por dimensão ISO/IEC 25012"""
    completude: float
    unicidade: float
    consistencia: Optional[float]  # None quando não se aplica ao tipo da coluna
    acuracia: Optional[float]  # None quando não se aplica ao tipo da coluna
    atualidade: Optional[float]  # só se aplica a colunas DateTime; None nas demais
    conformidade_regras: Optional[float]  # None quando a coluna não tem regra de usuário configurada
    score_geral: float
    classificacao: str  # Crítico, Regular, Ótimo


class QualityScorer:
    """
    Calcula scores de qualidade alinhados à ISO/IEC 25012.
    """

    def __init__(self, weights: Dict[str, float] = None):
        """
        Args:
            weights: Pesos para cada dimensão no cálculo do score geral
        """
        # Unicidade NÃO entra aqui: é subjetiva (uma coluna pode repetir
        # muito por natureza, ex: categoria/FK, sem isso ser defeito de
        # qualidade). Continua calculada e disponível na coluna score_unicidade,
        # só não pondera o score_geral. Pesos originais eram 30/25/25/20
        # (completude/unicidade/consistência/acurácia): removendo a fatia de
        # unicidade (25) e renormalizando os outros 3 proporcionalmente sobre
        # 75: 30/75, 25/75, 20/75 = 6/15, 5/15, 4/15.
        #
        # conformidade_regras é uma dimensão nova (regras de negócio que o
        # usuário configura, ex: GE contra DuckDB): só entra quando a coluna
        # tem pelo menos uma regra ativa (ver calculate_rule_conformity_score).
        # O peso 5 foi escolhido só pra manter a MESMA proporção 6:5:4 entre
        # completude/consistência/acurácia de antes: quando nenhuma regra se
        # aplica (caso comum hoje), a renormalização em calculate_overall_score
        # cai exatamente nos pesos 6/15, 5/15, 4/15 de antes: comportamento
        # idêntico ao pré-regras pra qualquer coluna sem regra configurada.
        self.weights = weights or {
            "completude": 6 / 20,
            "consistencia": 5 / 20,
            "acuracia": 4 / 20,
            "conformidade_regras": 5 / 20,
        }

        # Thresholds para classificação Crítico/Regular/Ótimo
        self.thresholds = {
            "otimo": 85.0,
            "regular": 70.0,
        }

    def calculate_completeness_score(self, metrics: Dict[str, Any], tolerancia_nulidade: Optional[float] = None) -> float:
        """
        Score de Completude: 100 * (1 - % de valores nulos/vazios).

        `tolerancia_nulidade` vem da regra de usuário 'nulidade_aceitavel'
        (config_regra_usuario): quando definida, os primeiros X% de nulos
        são "de graça" (não penalizam), só o que passa da tolerância conta.
        Ex: 30% nulos com tolerância de 30% -> completude 100; com tolerância
        de 10% -> só os 20 pontos excedentes penalizam.
        """
        percentual_nulos = metrics.get("percentual_nulos", 0.0)
        if tolerancia_nulidade:
            percentual_nulos = max(0.0, percentual_nulos - tolerancia_nulidade)
        return max(0.0, 100.0 * (1 - percentual_nulos / 100.0))

    def calculate_uniqueness_score(self, metrics: Dict[str, Any]) -> float:
        """
        Score de Unicidade: baseado na ausência de duplicados
        """
        qtd_total = metrics.get("qtd_linhas_total", 1)
        qtd_duplicados = metrics.get("qtd_duplicados", 0)

        if qtd_total == 0:
            return 100.0

        percentual_duplicados = (qtd_duplicados / qtd_total) * 100
        return max(0.0, 100.0 * (1 - percentual_duplicados / 100.0))

    def calculate_consistency_score(self, metrics: Dict[str, Any]) -> Optional[float]:
        """
        Score de Consistência: coerência interna de tipagem da coluna
        (não depende de regra de negócio manual). Só é calculada de verdade
        pra DateTime e Text/Categorical: nos demais tipos, ou quando não deu
        pra medir (ex: guarda de segfault em string longa), retorna None
        (não se aplica) em vez de um 100 automático, pra não inflar o
        score_geral com uma dimensão que nem foi avaliada.

        - DateTime: % de valores que não conformam ao formato de data esperado.
        - Text/Categorical: o quanto a coluna está "misturada" entre parecer
          número/data e não parecer. Tudo de um jeito só (0% ou 100% parseável)
          é consistente, mesmo que seja o tipo "errado": isso é avaliado em
          outra dimensão (tipagem). Metade-metade é o pior caso (mistura real).
        - Numeric e demais tipos: dimensão não se aplica.
        """
        tipo = metrics.get("tipo_dado_inferido")

        if tipo == "DateTime":
            percentual_invalidas = metrics.get("percentual_datas_invalidas")
            if percentual_invalidas is None:
                return None
            return max(0.0, 100.0 - percentual_invalidas)

        if tipo in ("Text", "Categorical"):
            graus_mistura = [
                min(percentual, 100.0 - percentual) * 2
                for percentual in (metrics.get("percentual_parse_numerico"), metrics.get("percentual_parse_data"))
                if percentual is not None
            ]
            if graus_mistura:
                return max(0.0, 100.0 - max(graus_mistura))
            return None

        return None

    def calculate_accuracy_score(self, metrics: Dict[str, Any]) -> Optional[float]:
        """
        Score de Acurácia: baseado em outliers (IQR). Só se aplica a colunas
        Numeric: nas demais, retorna None (não se aplica) em vez de um 100
        automático.
        """
        if metrics.get("tipo_dado_inferido") != "Numeric":
            return None

        percentual_outliers = metrics.get("percentual_outliers")
        if percentual_outliers is None:
            return None
        return max(0.0, 100.0 * (1 - percentual_outliers / 100.0))

    def calculate_timeliness_score(self, metrics: Dict[str, Any]) -> Optional[float]:
        """
        Score de Atualidade: só se aplica a colunas DateTime: mede quão
        recente é a própria distribuição de datas da coluna (dias corridos
        entre hoje e o valor máximo de data encontrado nela). Não é
        "há quanto tempo a base foi atualizada" nem "há quanto tempo rodamos
        o profiling": é uma leitura de frescor daquela coluna especificamente.
        Não entra no score_geral (é informativo, não é dimensão de qualidade
        universal: nem toda coluna tem data pra medir isso).
        """
        if metrics.get("tipo_dado_inferido") != "DateTime":
            return None

        data_maxima = metrics.get("max")
        if not data_maxima:
            return None

        try:
            data_maxima_dt = data_maxima if isinstance(data_maxima, datetime) else datetime.fromisoformat(str(data_maxima))
        except ValueError:
            return None

        dias_desde_atualizacao = max(0, (datetime.now() - data_maxima_dt).days)

        if dias_desde_atualizacao <= 1:
            return 100.0
        elif dias_desde_atualizacao <= 7:
            return 90.0
        elif dias_desde_atualizacao <= 30:
            return 75.0
        elif dias_desde_atualizacao <= 90:
            return 60.0
        elif dias_desde_atualizacao <= 180:
            return 40.0
        else:
            return 20.0

    def calculate_rule_conformity_score(
        self, nome_coluna: str, resultados_regras_usuario: Optional[List[Dict[str, Any]]]
    ) -> Optional[float]:
        """
        Score de Conformidade de Regras: média do `percentual_conformidade` de
        toda regra de usuário (origem='usuario') que envolve essa coluna,
        tanto regras de coluna única quanto multi-coluna/tabela (chave
        composta, soma, completude condicional etc), identificadas por
        `colunas_envolvidas` conter o nome da coluna.

        None quando a coluna não tem nenhuma regra ativa configurada: mesma
        convenção de "não se aplica" usada em consistência/acurácia, pra não
        inflar nem penalizar o score_geral de colunas sem regra nenhuma.
        """
        if not resultados_regras_usuario:
            return None

        conformidades = [
            r["percentual_conformidade"]
            for r in resultados_regras_usuario
            if r.get("percentual_conformidade") is not None
            and nome_coluna in (r.get("colunas_envolvidas") or "").split(",")
        ]
        if not conformidades:
            return None

        return sum(conformidades) / len(conformidades)

    def calculate_overall_score(
        self,
        score_completude: float,
        score_consistencia: Optional[float],
        score_acuracia: Optional[float],
        score_conformidade_regras: Optional[float] = None,
    ) -> float:
        """
        Score Geral: média ponderada das dimensões que de fato se aplicam
        àquela coluna (completude sempre entra; as demais só quando não são
        None). O peso das dimensões ausentes é redistribuído proporcionalmente
        entre as que restam, em vez de contar um 100 "de mentirinha" pra
        dimensão que nem foi medida: isso é o que antes fazia quase toda
        coluna virar Ótimo.
        """
        dimensoes = [
            ("completude", score_completude),
            ("consistencia", score_consistencia),
            ("acuracia", score_acuracia),
            ("conformidade_regras", score_conformidade_regras),
        ]
        dimensoes_validas = [(nome, valor) for nome, valor in dimensoes if valor is not None]

        peso_total = sum(self.weights[nome] for nome, _ in dimensoes_validas)
        if peso_total == 0:
            return 0.0

        return sum(self.weights[nome] * valor for nome, valor in dimensoes_validas) / peso_total

    def calculate_final_base_score(
        self,
        media_score_colunas: float,
        percentual_linhas_duplicadas: float,
        peso_linhas_duplicadas: float = 0.15,
    ) -> float:
        """
        Score final da base: combina a média dos scores de coluna (qualidade
        por atributo) com a unicidade de linha inteira (qualidade do grão da
        tabela): duas bases com colunas idênticas mas taxas de duplicidade de
        linha diferentes não deveriam empatar.
        """
        score_unicidade_linhas = max(0.0, 100.0 - percentual_linhas_duplicadas)
        return round(
            (1 - peso_linhas_duplicadas) * media_score_colunas + peso_linhas_duplicadas * score_unicidade_linhas, 2
        )

    def classify(self, score_geral: float) -> str:
        """
        Classifica em Crítico, Regular ou Ótimo baseado no score geral
        """
        if score_geral >= self.thresholds["otimo"]:
            return "Ótimo"
        elif score_geral >= self.thresholds["regular"]:
            return "Regular"
        else:
            return "Crítico"

    def score(
        self,
        metrics: Dict[str, Any],
        nome_coluna: Optional[str] = None,
        resultados_regras_usuario: Optional[List[Dict[str, Any]]] = None,
        tolerancia_nulidade: Optional[float] = None,
    ) -> QualityScores:
        """
        Calcula todos os scores e classificação.

        Args:
            metrics: Dicionário com métricas de profiling
            nome_coluna: nome da coluna (só necessário se houver regras de
                usuário a considerar: sem ele, conformidade_regras fica None)
            resultados_regras_usuario: resultados de avaliar_regras() pra essa
                execução (mesma lista pra toda coluna: cada uma filtra a sua)
            tolerancia_nulidade: % de nulos tolerado sem penalizar completude
                (vem da regra 'nulidade_aceitavel', se configurada pra essa coluna)

        Returns:
            QualityScores com todos os scores calculados
        """
        score_completude = self.calculate_completeness_score(metrics, tolerancia_nulidade)
        score_unicidade = self.calculate_uniqueness_score(metrics)  # calculado/guardado, não pondera score_geral
        score_consistencia = self.calculate_consistency_score(metrics)
        score_acuracia = self.calculate_accuracy_score(metrics)
        score_atualidade = self.calculate_timeliness_score(metrics)
        score_conformidade_regras = (
            self.calculate_rule_conformity_score(nome_coluna, resultados_regras_usuario) if nome_coluna else None
        )

        score_geral = self.calculate_overall_score(
            score_completude,
            score_consistencia,
            score_acuracia,
            score_conformidade_regras,
        )

        classificacao = self.classify(score_geral)

        return QualityScores(
            completude=round(score_completude, 2),
            unicidade=round(score_unicidade, 2),
            consistencia=round(score_consistencia, 2) if score_consistencia is not None else None,
            acuracia=round(score_acuracia, 2) if score_acuracia is not None else None,
            atualidade=round(score_atualidade, 2) if score_atualidade is not None else None,
            conformidade_regras=round(score_conformidade_regras, 2) if score_conformidade_regras is not None else None,
            score_geral=round(score_geral, 2),
            classificacao=classificacao,
        )
