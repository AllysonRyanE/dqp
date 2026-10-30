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
    atualidade: Optional[float]  # só se aplica a colunas DateTime, None nas demais
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
        # Unicidade não entra aqui: é subjetiva (uma coluna pode repetir
        # muito por natureza, ex: FK), continua calculada à parte em
        # score_unicidade. conformidade_regras só entra quando a coluna tem
        # ao menos uma regra ativa; seu peso (5) mantém a mesma proporção
        # 6:5:4 entre completude/consistência/acurácia de antes, então uma
        # coluna sem regra renormaliza pros mesmos pesos do pré-regras.
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
        Ex: 30% nulos com tolerância de 30% -> completude 100. Com tolerância
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
        Score de Consistência: coerência interna de tipagem da coluna. Só
        calculada pra DateTime e Text/Categorical; nos demais tipos retorna
        None (não se aplica), pra não inflar o score_geral.

        - DateTime: % de valores que não conformam ao formato esperado.
        - Text/Categorical: o quanto a coluna está "misturada" entre parecer
          número/data e não parecer; metade-metade é o pior caso.
        - Numeric e demais tipos: não se aplica.
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
        """Score de Atualidade: só se aplica a colunas DateTime, mede dias
        corridos entre hoje e a data máxima da coluna. Não entra no
        score_geral: é informativo, nem toda coluna tem data pra medir."""
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
        """Score de Conformidade de Regras: média do `percentual_conformidade`
        de toda regra de usuário que envolve essa coluna (identificadas por
        `colunas_envolvidas`). None quando não há regra ativa, mesma
        convenção de "não se aplica" de consistência/acurácia."""
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
        """Score Geral: média ponderada das dimensões que se aplicam à
        coluna (completude sempre entra). O peso das ausentes é
        redistribuído entre as que restam, em vez de contar um 100 pra
        dimensão não medida, o que antes fazia quase tudo virar Ótimo."""
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
