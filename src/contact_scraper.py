"""
GoAP Contact Scraper -- Finds publicly available contact emails from company websites.
Targets: press offices, HR departments, investor relations, general contact pages.
"""

import re
import logging
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Common contact page paths to check on company websites
CONTACT_PATHS = [
    "/contact",
    "/kontakt",
    "/en/contact",
    "/en/company/contact",
    "/company/contact",
    "/about/contact",
    "/press",
    "/presse",
    "/en/press",
    "/media",
    "/media/press",
    "/newsroom",
    "/en/newsroom",
    "/investor-relations",
    "/ir",
    "/career",
    "/karriere",
    "/en/career",
    "/imprint",
    "/impressum",
    "/en/imprint",
    "/about-us",
    "/en/about-us",
]

# Email regex pattern
EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

# Classify found emails by department
DEPT_KEYWORDS = {
    "press": ["press", "presse", "media", "pr@", "communications", "kommunikation", "newsroom"],
    "hr": ["hr@", "career", "karriere", "jobs@", "recruiting", "bewerbung", "personal", "talent"],
    "investor_relations": ["ir@", "investor", "investoren"],
    "general": ["info@", "contact@", "kontakt@", "office@", "zentrale", "reception"],
    "sales": ["sales@", "vertrieb@", "business@"],
}

# Domains to skip (not company emails)
SKIP_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "example.com", "sentry.io", "cloudflare.com", "googleapis.com",
    "w3.org", "schema.org", "facebook.com", "twitter.com",
    "linkedin.com", "instagram.com", "youtube.com", "google.com",
    "wixpress.com", "squarespace.com", "wordpress.com",
}


def _classify_email(email: str) -> str:
    """Classify an email by department based on the address pattern."""
    email_lower = email.lower()
    for dept, keywords in DEPT_KEYWORDS.items():
        for kw in keywords:
            if kw in email_lower:
                return dept
    return "unknown"


def _is_valid_company_email(email: str, company_domain: str) -> bool:
    """Check if email belongs to the company domain and is not a personal/junk address."""
    email_lower = email.lower()
    domain = email_lower.split("@")[-1]

    # Skip non-company domains
    if domain in SKIP_DOMAINS:
        return False

    # Prefer emails matching the company domain
    # But also accept emails from the same root domain
    company_root = company_domain.lower().replace("www.", "")
    if company_root in domain or domain in company_root:
        return True

    # Accept if domain looks corporate (not free email)
    if domain not in SKIP_DOMAINS and "." in domain:
        return True

    return False


def _scrape_page_for_emails(url: str, company_domain: str, timeout: int = 10) -> List[Dict]:
    """Scrape a single page for email addresses."""
    found_emails = []
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
    }

    try:
        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        if response.status_code != 200:
            return []

        # Parse HTML
        soup = BeautifulSoup(response.text, "html.parser")

        # Method 1: Find emails in mailto: links
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if href.startswith("mailto:"):
                email = href.replace("mailto:", "").split("?")[0].strip()
                if EMAIL_PATTERN.match(email) and _is_valid_company_email(email, company_domain):
                    context = link.get_text(strip=True) or ""
                    found_emails.append({
                        "email": email.lower(),
                        "department": _classify_email(email),
                        "context": context[:100],
                        "source_url": url,
                        "method": "mailto_link"
                    })

        # Method 2: Find emails in page text
        page_text = soup.get_text()
        text_emails = EMAIL_PATTERN.findall(page_text)
        for email in text_emails:
            if _is_valid_company_email(email, company_domain):
                # Avoid duplicates
                if email.lower() not in [e["email"] for e in found_emails]:
                    found_emails.append({
                        "email": email.lower(),
                        "department": _classify_email(email),
                        "context": "",
                        "source_url": url,
                        "method": "page_text"
                    })

        # Method 3: Check meta tags and structured data
        for meta in soup.find_all("meta"):
            content = meta.get("content", "")
            meta_emails = EMAIL_PATTERN.findall(content)
            for email in meta_emails:
                if _is_valid_company_email(email, company_domain):
                    if email.lower() not in [e["email"] for e in found_emails]:
                        found_emails.append({
                            "email": email.lower(),
                            "department": _classify_email(email),
                            "context": "meta tag",
                            "source_url": url,
                            "method": "meta_tag"
                        })

    except requests.exceptions.RequestException as e:
        logger.debug(f"Failed to scrape {url}: {e}")
    except Exception as e:
        logger.debug(f"Parse error for {url}: {e}")

    return found_emails


def scrape_company_contacts(company: Dict, delay: float = 1.0) -> Dict:
    """
    Scrape publicly available contact emails for a company.
    Checks multiple common contact page paths.

    Returns dict with:
      - press_email: str or None
      - hr_email: str or None
      - general_email: str or None
      - all_emails: List[Dict] (all found with classification)
    """
    website = company.get("website", "")
    if not website:
        return {"press_email": None, "hr_email": None, "general_email": None, "all_emails": []}

    # Normalize base URL
    if not website.startswith("http"):
        base_url = f"https://www.{website}"
    else:
        base_url = website

    company_domain = website.replace("https://", "").replace("http://", "").replace("www.", "")

    all_found = []
    seen_emails = set()

    logger.info(f"  Scraping contacts for {company.get('name', '')} ({base_url})...")

    for path in CONTACT_PATHS:
        url = urljoin(base_url + "/", path.lstrip("/"))
        page_emails = _scrape_page_for_emails(url, company_domain)

        for entry in page_emails:
            if entry["email"] not in seen_emails:
                seen_emails.add(entry["email"])
                all_found.append(entry)

        if page_emails:
            logger.debug(f"    Found {len(page_emails)} emails on {path}")

        time.sleep(delay * 0.3)  # Brief delay between page requests

    # Classify best emails by department
    press_email = None
    hr_email = None
    general_email = None
    ir_email = None

    for entry in all_found:
        dept = entry["department"]
        if dept == "press" and not press_email:
            press_email = entry["email"]
        elif dept == "hr" and not hr_email:
            hr_email = entry["email"]
        elif dept == "general" and not general_email:
            general_email = entry["email"]
        elif dept == "investor_relations" and not ir_email:
            ir_email = entry["email"]

    # Fallback: if no press email, use general or IR
    fallback_email = press_email or general_email or ir_email or hr_email
    if not fallback_email and all_found:
        fallback_email = all_found[0]["email"]

    result = {
        "press_email": press_email,
        "hr_email": hr_email,
        "general_email": general_email,
        "ir_email": ir_email,
        "fallback_email": fallback_email,
        "all_emails": all_found,
        "total_found": len(all_found)
    }

    found_count = len(all_found)
    if found_count > 0:
        logger.info(f"    Found {found_count} email(s): fallback={fallback_email}")
    else:
        logger.info(f"    No public emails found for {company.get('name', '')}")

    return result


def scrape_all_companies(companies: List[Dict], delay: float = 1.5) -> List[Dict]:
    """
    Scrape public contact emails for all companies.
    Adds 'public_contacts' field to each company dict.
    """
    logger.info(f"Scraping public contact emails for {len(companies)} companies...")

    for i, company in enumerate(companies):
        logger.info(f"[{i+1}/{len(companies)}] {company.get('name', 'Unknown')}")
        contacts = scrape_company_contacts(company, delay=delay)
        company["public_contacts"] = contacts

        # Rate limiting between companies
        if i < len(companies) - 1:
            time.sleep(delay)

    total_with_emails = sum(1 for c in companies if c.get("public_contacts", {}).get("total_found", 0) > 0)
    logger.info(f"Contact scraping complete: {total_with_emails}/{len(companies)} companies have public emails")

    return companies
