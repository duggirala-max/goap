"""
GoAP News Scanner -- Scans Google News RSS for EU-wide company pain signals.
Expanded to cover all high-cost EU countries, not just Germany.
"""

import feedparser
import re
import time
import logging
import urllib.parse
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import yaml

logger = logging.getLogger(__name__)


class NewsScanner:
    """Scans Google News RSS feeds for company pain signals in target sectors across the EU."""

    GOOGLE_NEWS_RSS_BASE = "https://news.google.com/rss/search"

    def __init__(self, sector_config_path: str = "config/sector_config.yaml"):
        with open(sector_config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.sectors = self.config["sectors"]
        self.target_countries = self.config.get("target_countries", ["Germany"])

    def _build_queries(self) -> List[Dict]:
        """Build search queries from sector config -- sector x pain keyword x country combos."""
        queries = []
        for sector_key, sector_data in self.sectors.items():
            sector_terms = sector_data.get("keywords", [])[:3]
            pain_terms = sector_data.get("pain_keywords", [])[:5]
            expansion_terms = sector_data.get("expansion_keywords", [])[:3]

            for country in self.target_countries:
                for sector_term in sector_terms:
                    for pain_term in pain_terms:
                        query = f'{country} "{sector_term}" "{pain_term}"'
                        queries.append({
                            "query": query,
                            "sector": sector_key,
                            "signal_type": "pain",
                            "pain_keyword": pain_term,
                            "country": country
                        })

                    for exp_term in expansion_terms:
                        query = f'{country} "{sector_term}" "{exp_term}"'
                        queries.append({
                            "query": query,
                            "sector": sector_key,
                            "signal_type": "expansion",
                            "pain_keyword": exp_term,
                            "country": country
                        })

        return queries

    def _fetch_rss(self, query: str, max_results: int = 10) -> List[Dict]:
        """Fetch Google News RSS for a query."""
        encoded_query = urllib.parse.quote(query)
        url = f"{self.GOOGLE_NEWS_RSS_BASE}?q={encoded_query}&hl=en&gl=DE&ceid=DE:en"

        try:
            feed = feedparser.parse(url)
            results = []
            for entry in feed.entries[:max_results]:
                pub_date = None
                pub_datetime = None
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    pub_datetime = datetime(*entry.published_parsed[:6])
                    pub_date = pub_datetime.isoformat()

                results.append({
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "source": entry.get("source", {}).get("title", "Unknown"),
                    "published": pub_date,
                    "published_datetime": pub_datetime,
                    "summary": entry.get("summary", "")
                })
            return results
        except Exception as e:
            logger.warning(f"RSS fetch failed for query '{query}': {e}")
            return []

    def _extract_company_names(self, text: str) -> List[str]:
        """
        Extract potential company names from headline text.
        Uses patterns: capitalized words, words ending in AG/GmbH/SE, etc.
        """
        companies = []

        # Pattern: CompanyName AG/GmbH/SE/KG/SA/NV/SpA/AB/BV
        pattern_formal = (
            r"([A-Z][A-Za-z\u00e4\u00f6\u00fc\u00c4\u00d6\u00dc\u00df\-]+"
            r"(?:\s+[A-Z][A-Za-z\u00e4\u00f6\u00fc\u00c4\u00d6\u00dc\u00df\-]+)*)"
            r"\s+(?:AG|GmbH|SE|KG|KGaA|Co\.|SA|NV|SpA|AB|BV|Inc|Ltd)"
        )
        matches = re.findall(pattern_formal, text)
        companies.extend(matches)

        # Pattern: Known suffixes
        pattern_known = r"([A-Z][A-Za-z\u00e4\u00f6\u00fc\u00c4\u00d6\u00dc\u00df]{2,}(?:[-][A-Z][A-Za-z]+)?)"
        matches = re.findall(pattern_known, text)
        stop_words = {
            "The", "Germany", "German", "Europe", "European", "Asia", "India",
            "France", "French", "Italy", "Italian", "Sweden", "Swedish",
            "Austria", "Austrian", "Netherlands", "Dutch", "Belgium", "Belgian",
            "New", "For", "And", "But", "Not", "Has", "Its", "With", "From",
            "Will", "Can", "May", "Could", "Would", "Should", "That", "This",
            "After", "Before", "Over", "Under", "About", "Into", "Through",
            "During", "Between", "Here", "There", "Where", "When", "How",
            "What", "Why", "Who", "All", "Each", "Every", "Some", "Many",
            "More", "Most", "Other", "Than", "Then", "Also", "Just", "Only",
            "Still", "Even", "Already", "Plant", "Jobs", "Workers", "Production",
            "Market", "Sales", "Revenue", "Profit", "Loss", "Cost", "Price",
            "CEO", "CFO", "CTO", "COO", "Board", "Report", "Annual",
        }
        companies.extend([m for m in matches if m not in stop_words and len(m) > 2])

        return list(set(companies))

    def _calculate_freshness_days(self, pub_datetime: Optional[datetime]) -> Optional[int]:
        """Calculate days since article was published."""
        if not pub_datetime:
            return None
        delta = datetime.now() - pub_datetime
        return max(0, delta.days)

    def scan(self, max_queries: int = 50, delay_between_requests: float = 0.8) -> List[Dict]:
        """
        Run the full news scan across all EU target countries.
        Returns list of discovered companies with context.
        """
        queries = self._build_queries()[:max_queries]
        all_results = []
        seen_titles = set()

        logger.info(f"Scanning {len(queries)} queries across {len(self.target_countries)} EU countries via Google News RSS...")

        for i, q in enumerate(queries):
            entries = self._fetch_rss(q["query"], max_results=5)

            for entry in entries:
                title_key = entry["title"].lower().strip()
                if title_key in seen_titles:
                    continue
                seen_titles.add(title_key)

                companies = self._extract_company_names(entry["title"])
                freshness_days = self._calculate_freshness_days(entry.get("published_datetime"))

                if companies:
                    all_results.append({
                        "companies_mentioned": companies,
                        "headline": entry["title"],
                        "source": entry["source"],
                        "url": entry["link"],
                        "published": entry["published"],
                        "freshness_days": freshness_days,
                        "sector": q["sector"],
                        "signal_type": q["signal_type"],
                        "pain_keyword": q["pain_keyword"],
                        "country_query": q.get("country", "Unknown")
                    })

            if i < len(queries) - 1:
                time.sleep(delay_between_requests)

            if (i + 1) % 10 == 0:
                logger.info(f"  Processed {i + 1}/{len(queries)} queries, found {len(all_results)} unique results")

        logger.info(f"News scan complete: {len(all_results)} unique results from {len(queries)} queries")
        return all_results

    def match_to_seed(self, news_results: List[Dict], seed_companies: List[Dict]) -> Dict:
        """
        Match news results to seed companies by name.
        Returns enriched seed companies with news context.
        """
        seed_lookup = {}
        for company in seed_companies:
            name = company["name"].lower()
            simple_name = name.split()[0]
            seed_lookup[name] = company
            seed_lookup[simple_name] = company

        enriched = {}
        for result in news_results:
            for mentioned in result["companies_mentioned"]:
                mentioned_lower = mentioned.lower()
                matched_company = None

                if mentioned_lower in seed_lookup:
                    matched_company = seed_lookup[mentioned_lower]
                else:
                    for seed_name, seed_comp in seed_lookup.items():
                        if mentioned_lower in seed_name or seed_name in mentioned_lower:
                            matched_company = seed_comp
                            break

                if matched_company:
                    comp_name = matched_company["name"]
                    if comp_name not in enriched:
                        enriched[comp_name] = {
                            "company": matched_company,
                            "news_hits": []
                        }
                    enriched[comp_name]["news_hits"].append({
                        "headline": result["headline"],
                        "source": result["source"],
                        "url": result["url"],
                        "published": result["published"],
                        "freshness_days": result.get("freshness_days"),
                        "signal_type": result["signal_type"]
                    })

        return enriched

    def get_unmatched_news(self, news_results: List[Dict], seed_companies: List[Dict]) -> List[Dict]:
        """
        Return news results that did NOT match any seed company.
        Used for A1 (auto-discovery of new companies).
        """
        seed_names = set()
        for company in seed_companies:
            name = company["name"].lower()
            seed_names.add(name)
            seed_names.add(name.split()[0])

        unmatched = []
        seen_companies = set()

        for result in news_results:
            for mentioned in result["companies_mentioned"]:
                mentioned_lower = mentioned.lower()
                is_seed = False
                for seed_name in seed_names:
                    if mentioned_lower in seed_name or seed_name in mentioned_lower:
                        is_seed = True
                        break

                if not is_seed and mentioned_lower not in seen_companies and len(mentioned) > 3:
                    seen_companies.add(mentioned_lower)
                    unmatched.append({
                        "name": mentioned,
                        "headline": result["headline"],
                        "source": result["source"],
                        "url": result["url"],
                        "published": result["published"],
                        "freshness_days": result.get("freshness_days"),
                        "sector": result["sector"],
                        "signal_type": result["signal_type"],
                        "pain_keyword": result["pain_keyword"],
                        "country_query": result.get("country_query", "Unknown")
                    })

        logger.info(f"Found {len(unmatched)} unmatched company mentions for auto-discovery")
        return unmatched
