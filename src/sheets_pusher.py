"""
GoAP Sheets Pusher -- Pushes target account list to Google Sheets.
Creates a new tab per run (date-stamped). Deduplicates against existing tabs.
v3: Force-all toggle, always creates tab, LinkedIn URLs, run summary row, competitor column.
"""

import os
import json
import logging
import urllib.parse
from datetime import datetime
from typing import List, Dict, Set, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


# Column headers for the output sheet
HEADERS = [
    "Company Name",
    "Country",
    "Sector",
    "Why Target?",
    "Evidence Type",
    "Decision Maker Role",
    "LinkedIn Search URL",
    "Public Contact Email (Verified)",
    "Email Department",
    "Email Verification Status",
    "Email Pattern (Decision Maker)",
    "Why AP Fits",
    "Competitor Presence in India",
    "Career Expansion Signals",
    "Confidence Score",
    "Source",
    "Last Updated",
    "Outreach Email (English)",
    "Outreach Email (German)",
]


def _generate_linkedin_url(company: Dict) -> str:
    """
    A3: Generate a clickable LinkedIn search URL for finding decision makers.
    """
    name = company.get("name", "")
    role = company.get("decision_maker_role", "VP Operations")
    # Build LinkedIn people search URL
    search_terms = f"{name} {role}"
    encoded = urllib.parse.quote(search_terms)
    return f"https://www.linkedin.com/search/results/people/?keywords={encoded}"


class SheetsPusher:
    """Handles Google Sheets API operations for GoAP."""

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    def __init__(self, spreadsheet_id: Optional[str] = None, creds_json: Optional[str] = None):
        self.spreadsheet_id = spreadsheet_id or os.environ.get("GOOGLE_SHEET_ID")
        creds_json = creds_json or os.environ.get("GCP_SERVICE_ACCOUNT_JSON")

        if not self.spreadsheet_id:
            raise ValueError("GOOGLE_SHEET_ID not set")
        if not creds_json:
            raise ValueError("GCP_SERVICE_ACCOUNT_JSON not set")

        # Parse credentials
        creds_info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(
            creds_info, scopes=self.SCOPES
        )

        self.service = build("sheets", "v4", credentials=creds)
        self.sheets = self.service.spreadsheets()

    def _get_existing_sheet_names(self) -> List[str]:
        """Get all existing sheet/tab names in the spreadsheet."""
        try:
            metadata = self.sheets.get(spreadsheetId=self.spreadsheet_id).execute()
            return [s["properties"]["title"] for s in metadata.get("sheets", [])]
        except HttpError as e:
            logger.error(f"Failed to get sheet metadata: {e}")
            return []

    def _get_existing_companies(self) -> Set[str]:
        """
        Scan ALL existing tabs and collect company names already listed.
        Used for deduplication across runs.
        """
        existing = set()
        sheet_names = self._get_existing_sheet_names()

        for name in sheet_names:
            try:
                result = self.sheets.values().get(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"'{name}'!A:A"
                ).execute()

                values = result.get("values", [])
                for row in values[1:]:  # Skip header
                    if row:
                        existing.add(row[0].strip().lower())
            except HttpError as e:
                logger.warning(f"Could not read tab '{name}': {e}")

        logger.info(f"Found {len(existing)} existing companies across {len(sheet_names)} tabs")
        return existing

    def _create_new_tab(self, tab_name: str):
        """Create a new tab/sheet in the spreadsheet."""
        body = {
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": tab_name
                        }
                    }
                }
            ]
        }
        try:
            self.sheets.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body
            ).execute()
            logger.info(f"Created new tab: '{tab_name}'")
        except HttpError as e:
            if "already exists" in str(e).lower():
                logger.warning(f"Tab '{tab_name}' already exists. Will overwrite.")
            else:
                raise

    def _format_header(self, tab_name: str):
        """Apply formatting to the header row."""
        metadata = self.sheets.get(spreadsheetId=self.spreadsheet_id).execute()
        sheet_id = None
        for s in metadata.get("sheets", []):
            if s["properties"]["title"] == tab_name:
                sheet_id = s["properties"]["sheetId"]
                break

        if sheet_id is None:
            return

        body = {
            "requests": [
                # Bold header with dark blue background
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 1
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {
                                    "red": 0.2,
                                    "green": 0.3,
                                    "blue": 0.5
                                },
                                "textFormat": {
                                    "bold": True,
                                    "foregroundColor": {
                                        "red": 1.0,
                                        "green": 1.0,
                                        "blue": 1.0
                                    }
                                }
                            }
                        },
                        "fields": "userEnteredFormat(textFormat,backgroundColor)"
                    }
                },
                # Freeze header + summary rows
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {
                                "frozenRowCount": 2
                            }
                        },
                        "fields": "gridProperties.frozenRowCount"
                    }
                },
                # Auto-resize columns
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": len(HEADERS)
                        }
                    }
                }
            ]
        }

        try:
            self.sheets.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body
            ).execute()
        except HttpError as e:
            logger.warning(f"Header formatting failed: {e}")

    def _build_summary_row(self, run_meta: Dict) -> List[str]:
        """
        A4: Build a run summary metadata row.
        """
        return [
            f"RUN: {run_meta.get('timestamp', '')}",
            f"Countries: {run_meta.get('countries_scanned', 'N/A')}",
            f"Seed: {run_meta.get('seed_count', 0)}",
            f"News-discovered: {run_meta.get('news_discovered', 0)}",
            f"News queries: {run_meta.get('news_queries', 0)}",
            f"Total scored: {run_meta.get('total_scored', 0)}",
            f"Dupes skipped: {run_meta.get('dupes_skipped', 0)}",
            f"Groq: {run_meta.get('groq_status', 'N/A')}",
            f"Scrape: {run_meta.get('scrape_status', 'N/A')}",
            f"Verify: {run_meta.get('verify_status', 'N/A')}",
            "",  # Email pattern
            "",  # AP fit
            "",  # Competitor
            "",  # Career signals
            "",  # Score
            "",  # Source
            "",  # Last updated
            "",  # EN email
            "",  # DE email
        ]

    def push(self, companies: List[Dict], ap_advantages: Dict,
             skip_dedup: bool = False, dry_run: bool = False,
             run_meta: Dict = None) -> Dict:
        """
        Push companies to a new date-stamped tab in Google Sheets.
        ALWAYS creates a new tab.

        Args:
            skip_dedup: If True, include ALL companies regardless of duplicates.
            run_meta: Dict with run metadata for summary row.
        """
        # Generate tab name
        tab_name = datetime.now().strftime("Targets_%Y-%m-%d_%H%M")
        if run_meta is None:
            run_meta = {}

        # Deduplication
        skipped = []
        if skip_dedup:
            logger.info("Force-all mode: skipping deduplication, including ALL companies.")
            new_companies = list(companies)
        elif not dry_run:
            existing = self._get_existing_companies()
            new_companies = []
            for company in companies:
                name_lower = company.get("name", "").strip().lower()
                if name_lower in existing:
                    skipped.append(company.get("name", ""))
                    logger.info(f"  SKIP (duplicate): {company.get('name', '')}")
                else:
                    new_companies.append(company)
        else:
            new_companies = list(companies)

        # Build rows - ALWAYS include header + summary
        rows = [HEADERS]

        # A4: Run summary row
        run_meta["dupes_skipped"] = len(skipped)
        run_meta["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        summary_row = self._build_summary_row(run_meta)
        rows.append(summary_row)

        sector_advantages = ap_advantages.get("sector_advantages", {})

        if not new_companies:
            # Still create the tab with a note
            note_row = [
                f"All {len(companies)} companies already exist in previous tabs.",
                "Re-run with 'Force all companies' enabled to regenerate.",
            ] + [""] * (len(HEADERS) - 2)
            rows.append(note_row)
            logger.warning(f"No new companies (all {len(skipped)} are duplicates). Tab will be created with note.")
        else:
            for company in new_companies:
                sector_key = company.get("ap_fit_sector", company.get("sector", ""))
                sector_adv = sector_advantages.get(sector_key, {})
                ap_fit_text = sector_adv.get("pitch", "See AP IDP 4.0 policy for sector-specific incentives")

                # Evidence types
                evidence_types = set()
                for signal in company.get("pain_signals", []):
                    evidence_types.add(signal.get("evidence_type", "unknown"))

                # Public contact email
                public_contacts = company.get("public_contacts", {})
                verified_emails = company.get("verified_emails", {})
                public_verified = verified_emails.get("public_verified", [])

                public_email = ""
                email_dept = ""
                email_status = ""

                if public_verified:
                    best = public_verified[0]
                    public_email = best.get("email", "")
                    email_dept = best.get("department", "unknown")
                    email_status = best.get("verdict", "UNKNOWN")
                elif public_contacts.get("fallback_email"):
                    public_email = public_contacts["fallback_email"]
                    email_dept = "unverified"
                    email_status = "SCRAPED (not SMTP verified)"

                # Email pattern
                from src.email_patterns import format_pattern_display
                email_domain = company.get("email_domain", company.get("website", ""))
                email_pattern = company.get("email_pattern", format_pattern_display(email_domain))

                # A3: LinkedIn search URL
                linkedin_url = _generate_linkedin_url(company)

                # B3: Competitor presence
                competitor_info = company.get("competitor_india", "")

                # B4: Career expansion signals
                career = company.get("career_signals", {})
                if career.get("found"):
                    career_text = f"SIGNALS: {', '.join(career.get('keywords_matched', []))}"
                else:
                    career_text = "No signals detected"

                row = [
                    company.get("name", ""),
                    company.get("country", ""),
                    company.get("sector", ""),
                    company.get("why_target", ""),
                    ", ".join(evidence_types),
                    company.get("decision_maker_role", ""),
                    linkedin_url,
                    public_email,
                    email_dept,
                    email_status,
                    email_pattern,
                    ap_fit_text.strip()[:500],
                    competitor_info[:500] if competitor_info else "",
                    career_text,
                    str(company.get("scores", {}).get("total", 0)),
                    company.get("discovery_headline", ""),
                    datetime.now().isoformat(),
                    company.get("outreach_email_en", ""),
                    company.get("outreach_email_de", ""),
                ]
                rows.append(row)

        if dry_run:
            logger.info(f"[DRY RUN] Would create tab '{tab_name}' with {len(rows) - 2} data rows")
            return {
                "tab_name": tab_name,
                "total_input": len(companies),
                "duplicates_skipped": len(skipped),
                "new_added": len(new_companies),
                "skipped_names": skipped,
                "dry_run": True
            }

        # ALWAYS create new tab
        self._create_new_tab(tab_name)

        # Write data
        body = {"values": rows}
        try:
            self.sheets.values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{tab_name}'!A1",
                valueInputOption="RAW",
                body=body
            ).execute()
            logger.info(f"Wrote {len(rows) - 2} data rows to tab '{tab_name}'")
        except HttpError as e:
            logger.error(f"Failed to write data: {e}")
            raise

        # Format header
        self._format_header(tab_name)

        return {
            "tab_name": tab_name,
            "total_input": len(companies),
            "duplicates_skipped": len(skipped),
            "new_added": len(new_companies),
            "skipped_names": skipped
        }
