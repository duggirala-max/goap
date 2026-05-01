"""
GoAP Main Orchestrator v3 -- Government Outreach for Andhra Pradesh FDI.

Pipeline:
1. Load seed companies (Germany + EU-wide)
2. Load AP advantages
3. Scan Google News RSS across EU countries
4. Auto-discover new companies from news (B1 via Groq)
5. Score all companies (with freshness bonus)
6. Scrape public contact emails
7. Scan career pages for expansion signals (B4)
8. Verify emails via SMTP ping
9. Competitor presence analysis (B3 via Groq)
10. Generate dual outreach emails (EN + DE) via Groq
11. Push to Google Sheets (new date-stamped tab)
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
from src.contact_scraper import scrape_all_companies, scan_career_pages
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


def load_seed_companies() -> list:
    """Load curated seed companies from YAML (Germany + EU-wide)."""
    all_companies = []

    # Load German seed list
    if os.path.exists("data/seed_companies.yaml"):
        with open("data/seed_companies.yaml", "r") as f:
            data = yaml.safe_load(f)
        companies = data.get("companies", [])
        all_companies.extend(companies)
        logger.info(f"Loaded {len(companies)} German seed companies")

    # Load EU-wide seed list
    if os.path.exists("data/eu_seed_companies.yaml"):
        with open("data/eu_seed_companies.yaml", "r") as f:
            data = yaml.safe_load(f)
        companies = data.get("companies", [])
        all_companies.extend(companies)
        logger.info(f"Loaded {len(companies)} EU-wide seed companies")

    logger.info(f"Total seed companies: {len(all_companies)}")
    return all_companies


def load_ap_advantages(path: str = "config/ap_advantages.yaml") -> dict:
    """Load AP advantages data."""
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return data


def main():
    parser = argparse.ArgumentParser(description="GoAP v3 -- FDI Target Identification Pipeline")
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
    parser.add_argument("--force-all", action="store_true",
                        help="Force all companies (skip deduplication)")
    parser.add_argument("--max-companies", type=int, default=25,
                        help="Maximum companies to output (default: 25)")
    parser.add_argument("--max-queries", type=int, default=50,
                        help="Maximum news queries to run (default: 50)")
    args = parser.parse_args()

    # Run metadata for summary row (A4)
    run_meta = {
        "groq_status": "Skipped" if args.skip_groq else "Enabled",
        "scrape_status": "Skipped" if args.skip_scrape else "Enabled",
        "verify_status": "Skipped" if args.skip_verify else "Enabled",
        "news_discovered": 0,
        "seed_count": 0,
        "total_scored": 0,
        "news_queries": 0,
        "countries_scanned": "N/A",
    }

    logger.info("=" * 60)
    logger.info("GoAP v3 -- Government Outreach for Andhra Pradesh FDI")
    logger.info(f"Run started at {datetime.now().isoformat()}")
    logger.info("=" * 60)

    # Step 1: Load seed companies (Germany + EU)
    logger.info("\n--- STEP 1: Loading seed companies (Germany + EU) ---")
    seed_companies = load_seed_companies()
    run_meta["seed_count"] = len(seed_companies)

    # Step 2: Load AP advantages
    logger.info("\n--- STEP 2: Loading AP advantages ---")
    ap_advantages = load_ap_advantages()

    # Step 3: News scanning (EU-wide)
    news_enrichment = {}
    unmatched_news = []
    if not args.skip_news:
        logger.info("\n--- STEP 3: Scanning Google News RSS (EU-wide) ---")
        try:
            scanner = NewsScanner()
            run_meta["countries_scanned"] = ", ".join(scanner.target_countries)
            news_results = scanner.scan(max_queries=args.max_queries)
            run_meta["news_queries"] = min(args.max_queries, len(scanner._build_queries()))
            news_enrichment = scanner.match_to_seed(news_results, seed_companies)
            unmatched_news = scanner.get_unmatched_news(news_results, seed_companies)
            logger.info(f"News enrichment: {len(news_enrichment)} seed companies matched")
            logger.info(f"Unmatched news: {len(unmatched_news)} potential new companies")
        except Exception as e:
            logger.warning(f"News scanning failed (continuing with seed list only): {e}")
    else:
        logger.info("\n--- STEP 3: News scanning SKIPPED ---")

    # Step 4: Auto-discover new companies from news (B1)
    discovered_companies = []
    if not args.skip_news and not args.skip_groq and unmatched_news:
        logger.info("\n--- STEP 4: Auto-discovering new companies from news (B1) ---")
        try:
            enricher = GroqEnricher()
            discovered_companies = enricher.extract_companies_from_news(unmatched_news, max_headlines=30)
            run_meta["news_discovered"] = len(discovered_companies)
            if discovered_companies:
                logger.info(f"B1: Adding {len(discovered_companies)} news-discovered companies to pipeline")
        except Exception as e:
            logger.warning(f"B1 auto-discovery failed: {e}")
    else:
        logger.info("\n--- STEP 4: Auto-discovery SKIPPED ---")

    # Merge seed + discovered companies
    all_companies = list(seed_companies)
    if discovered_companies:
        # Avoid duplicates with seed
        seed_names = {c["name"].lower() for c in seed_companies}
        for dc in discovered_companies:
            if dc["name"].lower() not in seed_names:
                all_companies.append(dc)
                seed_names.add(dc["name"].lower())

    # Step 5: Score companies
    logger.info("\n--- STEP 5: Scoring companies (with freshness bonus) ---")
    scorer = ScoringEngine()
    scored_companies = scorer.score_all(all_companies, news_enrichment)
    scored_companies = scored_companies[:args.max_companies]
    run_meta["total_scored"] = len(scored_companies)
    logger.info(f"Top {len(scored_companies)} companies selected for output")

    # Step 6: Scrape public contact emails
    if not args.skip_scrape:
        logger.info("\n--- STEP 6: Scraping public contact emails ---")
        try:
            scored_companies = scrape_all_companies(scored_companies, delay=1.5)
        except Exception as e:
            logger.warning(f"Contact scraping failed: {e}")
            for c in scored_companies:
                c["public_contacts"] = {"fallback_email": None, "all_emails": [], "total_found": 0}
    else:
        logger.info("\n--- STEP 6: Contact scraping SKIPPED ---")
        for c in scored_companies:
            c["public_contacts"] = {"fallback_email": None, "all_emails": [], "total_found": 0}

    # Step 7: Career page scanning (B4)
    if not args.skip_scrape:
        logger.info("\n--- STEP 7: Scanning career pages for expansion signals (B4) ---")
        try:
            scored_companies = scan_career_pages(scored_companies, delay=1.0)
        except Exception as e:
            logger.warning(f"Career scan failed: {e}")
            for c in scored_companies:
                c["career_signals"] = {"found": False, "keywords_matched": []}
    else:
        logger.info("\n--- STEP 7: Career scan SKIPPED ---")
        for c in scored_companies:
            c["career_signals"] = {"found": False, "keywords_matched": []}

    # Step 8: Verify emails via SMTP ping
    if not args.skip_verify:
        logger.info("\n--- STEP 8: Verifying emails via SMTP ping ---")
        try:
            scored_companies = verify_companies_emails(scored_companies, delay=2.0)
        except Exception as e:
            logger.warning(f"Email verification failed: {e}")
            for c in scored_companies:
                c["verified_emails"] = {"decision_maker": None, "public_verified": []}
    else:
        logger.info("\n--- STEP 8: Email verification SKIPPED ---")
        for c in scored_companies:
            c["verified_emails"] = {"decision_maker": None, "public_verified": []}

    # Step 9: Competitor presence analysis (B3)
    if not args.skip_groq:
        logger.info("\n--- STEP 9: Competitor presence in India analysis (B3) ---")
        try:
            enricher = GroqEnricher()
            scored_companies = enricher.get_competitor_presence(scored_companies)
        except Exception as e:
            logger.warning(f"B3 competitor analysis failed: {e}")
            for c in scored_companies:
                c["competitor_india"] = "[Analysis failed]"
    else:
        logger.info("\n--- STEP 9: Competitor analysis SKIPPED ---")
        for c in scored_companies:
            c["competitor_india"] = "[Skipped]"

    # Step 10: Groq email generation (dual EN + DE)
    if not args.skip_groq:
        logger.info("\n--- STEP 10: Generating dual outreach emails (EN+DE) via Groq ---")
        try:
            enricher = GroqEnricher()
            scored_companies = enricher.enrich_all(scored_companies, ap_advantages)
            run_meta["groq_status"] = f"OK ({len(scored_companies)} companies)"
        except Exception as e:
            logger.warning(f"Groq enrichment failed: {e}")
            run_meta["groq_status"] = f"FAILED: {str(e)[:50]}"
            for c in scored_companies:
                c["outreach_email_en"] = f"[Groq error: {str(e)[:100]}]"
                c["outreach_email_de"] = f"[Groq error: {str(e)[:100]}]"
    else:
        logger.info("\n--- STEP 10: Groq email generation SKIPPED ---")
        for c in scored_companies:
            c["outreach_email_en"] = "[Skipped]"
            c["outreach_email_de"] = "[Skipped]"

    # Step 11: Push to Google Sheets
    logger.info("\n--- STEP 11: Pushing to Google Sheets ---")
    if args.dry_run:
        logger.info("[DRY RUN MODE]")
        print("\n" + "=" * 80)
        print("DRY RUN - RESULTS PREVIEW")
        print("=" * 80)
        for i, company in enumerate(scored_companies, 1):
            contacts = company.get("public_contacts", {})
            career = company.get("career_signals", {})
            competitor = company.get("competitor_india", "")

            print(f"\n{'-' * 60}")
            print(f"#{i} | {company.get('name', 'Unknown')} ({company.get('country', '')})")
            print(f"   Sector: {company.get('sector', '')}")
            print(f"   Score: {company.get('scores', {}).get('total', 0)}")
            print(f"   Why: {company.get('why_target', '')[:120]}...")
            print(f"   Public Email: {contacts.get('fallback_email', 'NONE')}")
            if career.get("found"):
                print(f"   Career Signals: {', '.join(career.get('keywords_matched', []))}")
            if competitor and not competitor.startswith("["):
                print(f"   Competitor: {competitor[:100]}")
            if company.get("source") == "news_discovery":
                print(f"   [NEWS-DISCOVERED] {company.get('discovery_headline', '')[:100]}")

        print(f"\n{'=' * 80}")
        print(f"Total: {len(scored_companies)} companies | Seed: {run_meta['seed_count']} | News-discovered: {run_meta['news_discovered']}")
        print(f"{'=' * 80}\n")
    else:
        try:
            pusher = SheetsPusher()
            result = pusher.push(
                scored_companies, ap_advantages,
                skip_dedup=args.force_all,
                run_meta=run_meta
            )
            logger.info(f"\n--- RESULTS ---")
            logger.info(f"Tab created: {result['tab_name']}")
            logger.info(f"Total input: {result['total_input']}")
            logger.info(f"Duplicates skipped: {result['duplicates_skipped']}")
            logger.info(f"New companies added: {result['new_added']}")
            if result.get("skipped_names"):
                logger.info(f"Skipped: {', '.join(result['skipped_names'][:10])}")
        except Exception as e:
            logger.error(f"Google Sheets push failed: {e}")
            logger.info("Falling back to console output...")
            for company in scored_companies:
                print(f"{company.get('name', '')} | {company.get('country', '')} | {company.get('sector', '')} | Score: {company.get('scores', {}).get('total', 0)}")
            sys.exit(1)

    logger.info(f"\nGoAP v3 run completed at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
