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

