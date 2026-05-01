"""
GoAP Scoring Engine -- Scores companies based on pain signals, sector match, news freshness, and AP fit.
"""

import logging
from typing import List, Dict

import yaml

logger = logging.getLogger(__name__)


class ScoringEngine:
    """Scores companies for FDI outreach priority."""

    def __init__(self, sector_config_path: str = "config/sector_config.yaml"):
        with open(sector_config_path, "r") as f:
            config = yaml.safe_load(f)
        self.scoring_config = config.get("scoring", {})
        self.pain_weights = self.scoring_config.get("pain_signals", {})
        self.size_pref = self.scoring_config.get("size_preference", {})
        self.freshness_bonus = self.scoring_config.get("freshness_bonus", {})
        self.min_score = self.scoring_config.get("min_score", 5)

    def _score_pain_signals(self, company: Dict) -> int:
        """Score based on pain signal types."""
        score = 0
        pain_signals = company.get("pain_signals", [])

        for signal in pain_signals:
            signal_type = signal.get("type", "")
            weight = self.pain_weights.get(signal_type, 3)

            # Boost for VERIFIED evidence
            confidence = signal.get("confidence", "UNKNOWN")
            if confidence == "VERIFIED":
                weight = int(weight * 1.2)

            score += weight

        return min(score, 20)  # Cap at 20

    def _score_size(self, company: Dict) -> int:
        """Score based on company size preference (Mittelstand preferred)."""
        size_class = company.get("size_class", "unknown")
        return self.size_pref.get(size_class, 1)

    def _score_news_activity(self, news_hits: List[Dict] = None) -> int:
        """Bonus score for recent news activity."""
        if not news_hits:
            return 0
        return min(len(news_hits) * 2, 6)  # Up to +6 for 3+ news hits

    def _score_freshness(self, news_hits: List[Dict] = None) -> int:
        """
        A2: Bonus for news recency.
        +3 if any article is within 7 days.
        +1 if any article is within 30 days.
        """
        if not news_hits:
            return 0

        best_freshness = None
        for hit in news_hits:
            days = hit.get("freshness_days")
            if days is not None:
                if best_freshness is None or days < best_freshness:
                    best_freshness = days

        if best_freshness is None:
            return 0

        within_7 = self.freshness_bonus.get("within_7_days", 3)
        within_30 = self.freshness_bonus.get("within_30_days", 1)

        if best_freshness <= 7:
            return within_7
        elif best_freshness <= 30:
            return within_30
        return 0

    def score_company(self, company: Dict, news_hits: List[Dict] = None) -> Dict:
        """
        Score a single company. Returns company dict with score breakdown.
        """
        pain_score = self._score_pain_signals(company)
        size_score = self._score_size(company)
        news_score = self._score_news_activity(news_hits)
        freshness_score = self._score_freshness(news_hits)

        total_score = pain_score + size_score + news_score + freshness_score

        return {
            **company,
            "scores": {
                "pain_signal": pain_score,
                "size_preference": size_score,
                "news_activity": news_score,
                "freshness_bonus": freshness_score,
                "total": total_score
            }
        }

    def score_all(self, companies: List[Dict], news_enrichment: Dict = None) -> List[Dict]:
        """
        Score all companies and return sorted by total score (descending).
        Filters out companies below min_score.
        """
        if news_enrichment is None:
            news_enrichment = {}

        scored = []
        for company in companies:
            news_hits = None
            comp_name = company.get("name", "")
            if comp_name in news_enrichment:
                news_hits = news_enrichment[comp_name].get("news_hits", [])

            scored_company = self.score_company(company, news_hits)

            if scored_company["scores"]["total"] >= self.min_score:
                scored.append(scored_company)
            else:
                logger.debug(f"Filtered out {comp_name} (score: {scored_company['scores']['total']})")

        # Sort by total score descending
        scored.sort(key=lambda x: x["scores"]["total"], reverse=True)

        logger.info(f"Scored {len(scored)} companies (filtered from {len(companies)}, min_score={self.min_score})")
        return scored
