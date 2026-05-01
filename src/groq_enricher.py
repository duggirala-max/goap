"""
GoAP Groq Enricher — Generates targeted outreach emails using Groq API.
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

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("GROQ_API_KEY")
        if not self.api_key:
            logger.warning("No GROQ_API_KEY found. Email generation will be skipped.")
            self.enabled = False
        else:
            self.enabled = True

    def _call_groq(self, prompt: str, max_tokens: int = 500) -> Optional[str]:
        """Make a single Groq API call."""
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
                        "You are a senior government investment advisor writing outreach emails "
                        "on behalf of the Government of Andhra Pradesh, India. "
                        "Your tone is professional, direct, and respectful. "
                        "You write short, high-impact emails that get responses from C-level executives. "
                        "No fluff. No buzzwords. Every sentence must add value. "
                        "The email should feel like it was written by someone who did their homework on the company."
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

        try:
            response = requests.post(
                self.GROQ_API_URL,
                headers=headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"].strip()
        except requests.exceptions.RequestException as e:
            logger.error(f"Groq API call failed: {e}")
            return None

    def generate_outreach_email(self, company: Dict, ap_advantages: Dict) -> str:
        """
        Generate a targeted outreach email for a specific company.
        """
        if not self.enabled:
            return "[Groq API key not configured — email not generated]"

        # Build context for the LLM
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

RULES:
1. Subject line must reference the company's specific situation (not generic)
2. Opening line must show you know their pain point
3. Body must present AP as a concrete solution to their specific problem
4. Include 2-3 hard numbers (cost savings, incentives, market size)
5. Close with a specific ask (15-min call, site visit invitation)
6. Sign off as "Office of Industries & Commerce, Government of Andhra Pradesh"
7. Keep it under 150 words. Every word must earn its place.
"""

        result = self._call_groq(prompt)
        if result:
            return result
        return "[Email generation failed — Groq API error]"

    def enrich_all(self, companies: list, ap_advantages: Dict, delay: float = 1.5) -> list:
        """
        Generate outreach emails for all companies.
        Returns companies list with 'outreach_email' field added.
        """
        if not self.enabled:
            logger.warning("Groq API disabled. Skipping email generation for all companies.")
            for company in companies:
                company["outreach_email"] = "[Groq API key not configured]"
            return companies

        logger.info(f"Generating outreach emails for {len(companies)} companies via Groq API...")

        for i, company in enumerate(companies):
            logger.info(f"  [{i+1}/{len(companies)}] Generating email for {company.get('name', 'Unknown')}...")
            email = self.generate_outreach_email(company, ap_advantages)
            company["outreach_email"] = email

            # Rate limiting
            if i < len(companies) - 1:
                time.sleep(delay)

        logger.info("Email generation complete.")
        return companies
