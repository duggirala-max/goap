"""
GoAP Main Orchestrator -- Government Outreach for Andhra Pradesh FDI.

Orchestrates the full pipeline:
1. Load curated seed companies
2. Scan Google News RSS for pain signals
3. Merge and deduplicate
4. Score companies
5. Scrape public contact emails from company websites
6. Verify emails via SMTP ping
7. Generate dual outreach emails (EN + DE) via Groq
8. Push to Google Sheets (new date-stamped tab)
"""

import os
import sys
import logging
import argparse
from datetime import datetime

import yaml

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.news_scanner import NewsScanner
from src.scoring_engine import ScoringEngine
from src.contact_scraper import scrape_all_companies
from src.email_verifier import verify_companies_emails
from src.groq_enricher import GroqEnricher
from src.sheets_pusher import SheetsPusher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("GoAP")


def load_seed_companies(path: str = "data/seed_companies.yaml") -> list:
    """Load curated seed companies from YAML."""
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    companies = data.get("companies", [])
    logger.info(f"Loaded {len(companies)} seed companies from {path}")
    return companies


def load_ap_advantages(path: str = "config/ap_advantages.yaml") -> dict:
    """Load AP advantages data."""
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return data


def main():
    parser = argparse.ArgumentParser(description="GoAP -- FDI Target Identification Pipeline")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run without pushing to Google Sheets")
    parser.add_argument("--skip-news", action="store_true",
                        help="Skip news scanning (use seed list only)")
    parser.add_argument("--skip-groq", action="store_true",
                        help="Skip Groq email generation")
    parser.add_argument("--skip-scrape", action="store_true",
                        help="Skip contact email scraping")
    parser.add_argument("--skip-verify", action="store_true",
                        help="Skip SMTP email verification")
    parser.add_argument("--max-companies", type=int, default=20,
                        help="Maximum companies to output (default: 20)")
    parser.add_argument("--max-queries", type=int, default=30,
                        help="Maximum news queries to run (default: 30)")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("GoAP -- Government Outreach for Andhra Pradesh FDI")
    logger.info(f"Run started at {datetime.now().isoformat()}")
    logger.info("=" * 60)

    # Step 1: Load seed companies
    logger.info("\n--- STEP 1: Loading seed companies ---")
    seed_companies = load_seed_companies()

    # Step 2: Load AP advantages
    logger.info("\n--- STEP 2: Loading AP advantages ---")
    ap_advantages = load_ap_advantages()

    # Step 3: News scanning
    news_enrichment = {}
    if not args.skip_news:
        logger.info("\n--- STEP 3: Scanning Google News RSS ---")
        try:
            scanner = NewsScanner()
            news_results = scanner.scan(max_queries=args.max_queries)
            news_enrichment = scanner.match_to_seed(news_results, seed_companies)
            logger.info(f"News enrichment: {len(news_enrichment)} seed companies matched with news")
        except Exception as e:
            logger.warning(f"News scanning failed (continuing with seed list only): {e}")
    else:
        logger.info("\n--- STEP 3: News scanning SKIPPED ---")

    # Step 4: Score companies
    logger.info("\n--- STEP 4: Scoring companies ---")
    scorer = ScoringEngine()
    scored_companies = scorer.score_all(seed_companies, news_enrichment)

    # Limit output
    scored_companies = scored_companies[:args.max_companies]
    logger.info(f"Top {len(scored_companies)} companies selected for output")

    # Step 5: Scrape public contact emails
    if not args.skip_scrape:
        logger.info("\n--- STEP 5: Scraping public contact emails ---")
        try:
            scored_companies = scrape_all_companies(scored_companies, delay=1.5)
        except Exception as e:
            logger.warning(f"Contact scraping failed (continuing without): {e}")
            for c in scored_companies:
                c["public_contacts"] = {"fallback_email": None, "all_emails": [], "total_found": 0}
    else:
        logger.info("\n--- STEP 5: Contact scraping SKIPPED ---")
        for c in scored_companies:
            c["public_contacts"] = {"fallback_email": None, "all_emails": [], "total_found": 0}

    # Step 6: Verify emails via SMTP ping
    if not args.skip_verify:
        logger.info("\n--- STEP 6: Verifying emails via SMTP ping ---")
        try:
            scored_companies = verify_companies_emails(scored_companies, delay=2.0)
        except Exception as e:
            logger.warning(f"Email verification failed (continuing without): {e}")
            for c in scored_companies:
                c["verified_emails"] = {"decision_maker": None, "public_verified": []}
    else:
        logger.info("\n--- STEP 6: Email verification SKIPPED ---")
        for c in scored_companies:
            c["verified_emails"] = {"decision_maker": None, "public_verified": []}

    # Step 7: Groq email generation (dual EN + DE)
    if not args.skip_groq:
        logger.info("\n--- STEP 7: Generating dual outreach emails (EN+DE) via Groq ---")
        try:
            enricher = GroqEnricher()
            scored_companies = enricher.enrich_all(scored_companies, ap_advantages)
        except Exception as e:
            logger.warning(f"Groq enrichment failed (continuing without emails): {e}")
            for c in scored_companies:
                c["outreach_email_en"] = f"[Groq error: {str(e)[:100]}]"
                c["outreach_email_de"] = f"[Groq error: {str(e)[:100]}]"
    else:
        logger.info("\n--- STEP 7: Groq email generation SKIPPED ---")
        for c in scored_companies:
            c["outreach_email_en"] = "[Skipped]"
            c["outreach_email_de"] = "[Skipped]"

    # Step 8: Push to Google Sheets
    logger.info("\n--- STEP 8: Pushing to Google Sheets ---")
    if args.dry_run:
        logger.info("[DRY RUN MODE]")
        print("\n" + "=" * 80)
        print("DRY RUN - RESULTS PREVIEW")
        print("=" * 80)
        for i, company in enumerate(scored_companies, 1):
            contacts = company.get("public_contacts", {})
            verified = company.get("verified_emails", {})
            pub_verified = verified.get("public_verified", [])

            print(f"\n{'-' * 60}")
            print(f"#{i} | {company.get('name', 'Unknown')} ({company.get('country', '')})")
            print(f"   Sector: {company.get('sector', '')}")
            print(f"   Score: {company.get('scores', {}).get('total', 0)}")
            print(f"   Why: {company.get('why_target', '')[:120]}...")
            print(f"   Public Email: {contacts.get('fallback_email', 'NONE')}")
            if pub_verified:
                for pv in pub_verified:
                    print(f"   Verified: {pv['email']} ({pv['department']}) - {pv['verdict']}")
            else:
                print(f"   Verified Emails: NONE")
            print(f"   Email Pattern: {company.get('email_pattern', 'Unknown')}")

            # Show email previews
            en_email = company.get("outreach_email_en", "")
            de_email = company.get("outreach_email_de", "")
            if en_email and not en_email.startswith("["):
                print(f"   EN Email: {en_email[:120]}...")
            else:
                print(f"   EN Email: {en_email[:80]}")
            if de_email and not de_email.startswith("["):
                print(f"   DE Email: {de_email[:120]}...")
            else:
                print(f"   DE Email: {de_email[:80]}")

        print(f"\n{'=' * 80}")
        print(f"Total: {len(scored_companies)} companies ready for outreach")
        print(f"{'=' * 80}\n")
    else:
        try:
            pusher = SheetsPusher()
            result = pusher.push(scored_companies, ap_advantages)
            logger.info(f"\n--- RESULTS ---")
            logger.info(f"Tab created: {result['tab_name']}")
            logger.info(f"Total input: {result['total_input']}")
            logger.info(f"Duplicates skipped: {result['duplicates_skipped']}")
            logger.info(f"New companies added: {result['new_added']}")
            if result.get("skipped_names"):
                logger.info(f"Skipped: {', '.join(result['skipped_names'])}")
        except Exception as e:
            logger.error(f"Google Sheets push failed: {e}")
            logger.info("Falling back to console output...")
            for company in scored_companies:
                print(f"{company.get('name', '')} | {company.get('sector', '')} | Score: {company.get('scores', {}).get('total', 0)}")
            sys.exit(1)

    logger.info(f"\nGoAP run completed at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
