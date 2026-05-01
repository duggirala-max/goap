"""
GoAP Groq Enricher -- Generates targeted outreach emails using Groq API.
Produces DUAL drafts: English + Simple German.
Includes sender identity and minister CC mention.
"""

import os
import json
import logging
import time
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)


class GroqEnricher:
    """Generates crisp, targeted outreach emails using Groq LLM API."""

    GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
    MODEL = "llama-3.3-70b-versatile"

    # Sender identity
    SENDER_NAME = "Saidurga Gowtham Duggirala"
    SENDER_ROLE = "Government of Andhra Pradesh"
    MINISTER_CC = "Hon'ble Minister for Industries, Commerce and Food Processing"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("GROQ_API_KEY")
        if not self.api_key:
            logger.warning("No GROQ_API_KEY found. Email generation will be skipped.")
            self.enabled = False
        else:
            self.enabled = True

    def _call_groq(self, prompt: str, max_tokens: int = 800, max_retries: int = 3) -> Optional[str]:
        """Make a Groq API call with exponential backoff for rate limits."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.MODEL,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a senior government investment advisor drafting outreach emails "
                        "on behalf of Saidurga Gowtham Duggirala, representing the Government of Andhra Pradesh, India. "
                        "Your tone is professional, direct, and respectful. "
                        "You write short, high-impact emails that get responses from C-level executives. "
                        "No fluff. No buzzwords. Every sentence must add value. "
                        "The email should feel like it was written by someone who did their homework on the company. "
                        "CRITICAL FORMATTING RULE: NEVER use em dashes, en dashes, or any dash longer than a regular hyphen (-). "
                        "Use commas, semicolons, colons, or periods instead. No exceptions."
                    )
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": max_tokens,
            "temperature": 0.7
        }

        retry_delay = 5  # Start with 5s delay on 429
        for attempt in range(max_retries + 1):
            try:
                response = requests.post(
                    self.GROQ_API_URL,
                    headers=headers,
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 429:
                    if attempt < max_retries:
                        logger.warning(f"  Rate limit hit (429). Retrying in {retry_delay}s... (Attempt {attempt+1}/{max_retries})")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue
                    else:
                        logger.error("Rate limit hit and max retries exhausted.")
                        return None
                        
                response.raise_for_status()
                # Post-process: remove any em/en dashes that slipped through
                data = response.json()
                result = data["choices"][0]["message"]["content"].strip()
                result = result.replace("\u2014", "-").replace("\u2013", "-").replace("\u2012", "-")
                return result
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    logger.warning(f"  Request failed: {e}. Retrying in 2s...")
                    time.sleep(2)
                    continue
                logger.error(f"Groq API call failed after {max_retries} retries: {e}")
                return None

    def _build_prompt(self, company: Dict, ap_advantages: Dict, language: str = "english") -> str:
        """Build the email generation prompt for a specific language."""
        company_name = company.get("name", "")
        sector = company.get("sector", "")
        country = company.get("country", "Germany")
        why_target = company.get("why_target", "")
        decision_maker = company.get("decision_maker_role", "")

        # Get pain signals summary
        pain_summary = ""
        for signal in company.get("pain_signals", []):
            pain_summary += f"- {signal.get('type', '')}: {signal.get('detail', '')}\n"

        # Get AP advantages for this sector
        sector_key = company.get("ap_fit_sector", sector)
        sector_advantages = ap_advantages.get("sector_advantages", {}).get(sector_key, {})
        ap_pitch = sector_advantages.get("pitch", "")
        ap_facts = sector_advantages.get("key_facts", [])
        general = ap_advantages.get("general", {})
        incentives = general.get("incentives", {})

        # Public contact emails for forwarding mention
        public_contacts = company.get("public_contacts", {})
        fallback_email = public_contacts.get("fallback_email", "")
        forwarding_note = ""
        if fallback_email:
            forwarding_note = (
                f"\nNOTE: This email is also being sent to the company's public contact ({fallback_email}) "
                "to ensure it reaches the right person. Mention this briefly in the email."
            )

        if language == "german":
            lang_instruction = """
LANGUAGE: Write the ENTIRE email in simple, professional German (formal "Sie" form).
Do NOT use any English words except proper nouns and technical terms.
Do NOT use em dashes or en dashes anywhere. Use commas, semicolons, or periods instead.
Keep it simple and clear. Avoid complex sentence structures.
"""
        else:
            lang_instruction = """
LANGUAGE: Write in clear, professional English.
Do NOT use em dashes or en dashes anywhere. Use commas, semicolons, or periods instead.
"""

        prompt = f"""Write a SHORT outreach email (max 150 words) for the following target:

COMPANY: {company_name} ({country})
SECTOR: {sector}
RECIPIENT ROLE: {decision_maker}
COMPANY SITUATION: {why_target}
PAIN SIGNALS:
{pain_summary}

ANDHRA PRADESH OFFER:
{ap_pitch}

KEY FACTS:
{chr(10).join('- ' + f for f in ap_facts[:4])}

INCENTIVES:
- Capital Subsidy: {incentives.get('capital_subsidy', 'Up to 25-30%')}
- SGST: {incentives.get('sgst_reimbursement', '100% for 5 years')}
- Power: {incentives.get('power_subsidy', 'Concessional rates')}
- Land: {incentives.get('land', 'Ready industrial plots')}

{lang_instruction}

MANDATORY ELEMENTS (include ALL of these):
1. Subject line must reference the company's specific situation (not generic)
2. Opening line: "My name is Saidurga Gowtham Duggirala and I am reaching out to you on behalf of the Government of Andhra Pradesh, India."
3. Second line: "The Hon'ble Minister for Industries, Commerce and Food Processing is marked in CC on this communication."
4. Body must show you understand their specific pain point
5. Present Andhra Pradesh as a concrete solution with 2-3 hard numbers
6. Close with a specific ask (15-min call or site visit invitation)
7. Sign off as:
   "Saidurga Gowtham Duggirala
   Office of Industries & Commerce
   Government of Andhra Pradesh, India"
8. NEVER use em dashes or en dashes. Use hyphens (-), commas, or periods only.
9. Keep it under 150 words after the subject line.
{forwarding_note}
"""
        return prompt

    def generate_outreach_email(self, company: Dict, ap_advantages: Dict) -> Dict:
        """
        Generate DUAL outreach emails (English + German) for a specific company.
        Returns dict with 'english' and 'german' keys.
        """
        if not self.enabled:
            return {
                "english": "[Groq API key not configured]",
                "german": "[Groq API key not configured]"
            }

        # Generate English draft
        en_prompt = self._build_prompt(company, ap_advantages, language="english")
        english_draft = self._call_groq(en_prompt)

        # Longer pause between EN and DE to stay under TPM
        time.sleep(3.0) 

        # Generate German draft
        de_prompt = self._build_prompt(company, ap_advantages, language="german")
        german_draft = self._call_groq(de_prompt)

        return {
            "english": english_draft or "[English email generation failed]",
            "german": german_draft or "[German email generation failed]"
        }

    def enrich_all(self, companies: list, ap_advantages: Dict, delay: float = 4.0) -> list:
        """
        Generate outreach emails for all companies.
        Returns companies list with 'outreach_email_en' and 'outreach_email_de' fields added.
        """
        if not self.enabled:
            logger.warning("Groq API disabled. Skipping email generation for all companies.")
            for company in companies:
                company["outreach_email_en"] = "[Groq API key not configured]"
                company["outreach_email_de"] = "[Groq API key not configured]"
            return companies

        logger.info(f"Generating dual outreach emails (EN+DE) for {len(companies)} companies via Groq API...")
        logger.info("Using 4s delay between companies + 3s between drafts to avoid rate limits.")

        for i, company in enumerate(companies):
            logger.info(f"  [{i+1}/{len(companies)}] Generating emails for {company.get('name', 'Unknown')}...")
            drafts = self.generate_outreach_email(company, ap_advantages)
            company["outreach_email_en"] = drafts["english"]
            company["outreach_email_de"] = drafts["german"]

            # Rate limiting (increased to 4s)
            if i < len(companies) - 1:
                time.sleep(delay)

        logger.info("Dual email generation complete.")
        return companies

    # ================================================================
    # B1: Auto-extract new companies from news headlines (BATCHED)
    # ================================================================

    def extract_companies_from_news(self, unmatched_news: list, max_headlines: int = 30) -> list:
        """
        B1: Send a batch of unmatched news headlines to Groq and ask it to
        extract EU-based manufacturing/automotive companies with pain signals.
        Uses a SINGLE API call to minimize rate limit impact.

        Returns list of dicts: [{name, country, sector, why_target, pain_signals, ...}]
        """
        if not self.enabled:
            return []

        if not unmatched_news:
            logger.info("No unmatched news headlines for auto-discovery.")
            return []

        # Build headline batch (limit to prevent token overflow)
        headlines = []
        for item in unmatched_news[:max_headlines]:
            headlines.append(f"- [{item.get('sector', '')}] {item['headline']} (Source: {item.get('source', '')})")

        headlines_text = "\n".join(headlines)

        prompt = f"""Analyze these news headlines and extract EU-based companies that appear to be under operational pressure.

HEADLINES:
{headlines_text}

For each company you identify, provide ONLY the following JSON array (no other text):
[
  {{
    "name": "Company Name",
    "country": "Country",
    "sector": "automotive_components|ev_components|manufacturing|food_processing|food_processing_equipment",
    "why_target": "Brief explanation of their pain signal based on the headline",
    "pain_type": "restructuring|plant_closure|job_cuts|insolvency|cost_cutting|margin_pressure",
    "headline": "The original headline",
    "confidence": "LIKELY"
  }}
]

RULES:
1. Only include companies headquartered in EU high-cost countries (Germany, France, Italy, Austria, Sweden, Netherlands, Belgium, Finland, Spain, Czech Republic).
2. Only include companies in manufacturing, automotive, EV, or food processing sectors.
3. Only include companies showing clear distress signals (restructuring, closures, job cuts, insolvency).
4. Do NOT include companies you are not sure about.
5. Do NOT include news agencies, consulting firms, or banks.
6. Return ONLY the JSON array, nothing else. If no companies found, return [].
"""

        logger.info(f"B1: Analyzing {len(headlines)} unmatched headlines for new company discovery...")
        result = self._call_groq(prompt, max_tokens=1500)

        if not result:
            logger.warning("B1: Groq extraction failed.")
            return []

        # Parse JSON response
        try:
            # Clean up response (sometimes LLM wraps in markdown)
            cleaned = result.strip()
            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[1]
                cleaned = cleaned.rsplit("```", 1)[0]
            
            companies = json.loads(cleaned)
            if not isinstance(companies, list):
                return []

            # Convert to standard company format
            discovered = []
            for c in companies:
                company = {
                    "name": c.get("name", ""),
                    "country": c.get("country", "Unknown"),
                    "sector": c.get("sector", "manufacturing"),
                    "size_class": "mid_cap",
                    "website": "",
                    "pain_signals": [{
                        "type": c.get("pain_type", "restructuring"),
                        "detail": c.get("why_target", ""),
                        "evidence_type": "news_discovery",
                        "confidence": "LIKELY"
                    }],
                    "why_target": c.get("why_target", ""),
                    "decision_maker_role": "Head of International Operations / VP Manufacturing",
                    "linkedin_search": f"{c.get('name', '')} VP Operations OR Director Manufacturing",
                    "email_domain": "",
                    "email_pattern": "Unknown",
                    "email_confidence": "UNKNOWN",
                    "ap_fit_sector": c.get("sector", "manufacturing"),
                    "source": "news_discovery",
                    "discovery_headline": c.get("headline", "")
                }
                if company["name"]:
                    discovered.append(company)

            logger.info(f"B1: Discovered {len(discovered)} new companies from news headlines")
            return discovered

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"B1: Failed to parse Groq response: {e}")
            return []

    # ================================================================
    # B3: Competitor presence in India (BATCHED)
    # ================================================================

    def get_competitor_presence(self, companies: list) -> list:
        """
        B3: For a batch of companies, ask Groq (single call) whether their
        competitors already have operations in India.
        Adds 'competitor_india' field to each company.
        """
        if not self.enabled:
            for c in companies:
                c["competitor_india"] = "[Groq not configured]"
            return companies

        if not companies:
            return companies

        # Build company list for batch query
        company_list = []
        for c in companies:
            company_list.append(f"- {c.get('name', '')} ({c.get('sector', '')}, {c.get('country', '')})")

        companies_text = "\n".join(company_list)

        prompt = f"""For each company below, identify if their DIRECT COMPETITORS already have manufacturing or operations in India.

COMPANIES:
{companies_text}

Return a JSON object mapping company names to their competitor presence info:
{{
  "Company Name": "Competitor X has plant in Tamil Nadu. Competitor Y has R&D center in Pune.",
  "Another Company": "No major competitors in India yet."
}}

RULES:
1. Only mention REAL, verified competitor operations in India.
2. Be specific: name the competitor, the location, and the type of operation.
3. If you are not sure, say "No confirmed competitor presence in India."
4. Keep each entry to 1-2 sentences max.
5. Return ONLY the JSON object, nothing else.
"""

        logger.info(f"B3: Checking competitor presence in India for {len(companies)} companies...")
        time.sleep(3)  # Rate limit buffer
        result = self._call_groq(prompt, max_tokens=1500)

        if not result:
            for c in companies:
                c["competitor_india"] = "[Competitor analysis failed]"
            return companies

        try:
            cleaned = result.strip()
            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[1]
                cleaned = cleaned.rsplit("```", 1)[0]

            competitor_data = json.loads(cleaned)
            if not isinstance(competitor_data, dict):
                raise ValueError("Not a dict")

            for c in companies:
                name = c.get("name", "")
                c["competitor_india"] = competitor_data.get(name, "No data available")

            logger.info("B3: Competitor presence analysis complete.")

        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            logger.warning(f"B3: Failed to parse competitor data: {e}")
            for c in companies:
                c["competitor_india"] = "[Parse error]"

        return companies

