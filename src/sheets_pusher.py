"""
GoAP Sheets Pusher -- Pushes target account list to Google Sheets.
Creates a new tab per run (date-stamped). Deduplicates against existing tabs.
"""

import os
import json
import logging
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
    "LinkedIn Search Strategy",
    "Public Contact Email (Verified)",
    "Email Department",
    "Email Verification Status",
    "Email Pattern (Decision Maker)",
    "Why AP Fits",
    "Confidence Score",
    "Source",
    "Last Updated",
    "Outreach Email (English)",
    "Outreach Email (German)",
]


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
                # Read column A (Company Name) from each tab
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
        # Get sheet ID for the new tab
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
                # Freeze header row
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {
                                "frozenRowCount": 1
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

    def push(self, companies: List[Dict], ap_advantages: Dict, dry_run: bool = False) -> Dict:
        """
        Push companies to a new date-stamped tab in Google Sheets.
        Deduplicates against existing tabs.

        Returns summary dict with counts.
        """
        # Generate tab name
        tab_name = datetime.now().strftime("Targets_%Y-%m-%d_%H%M")

        # Get existing companies for deduplication
        if not dry_run:
            existing = self._get_existing_companies()
        else:
            existing = set()

        # Filter out duplicates
        new_companies = []
        skipped = []
        for company in companies:
            name_lower = company.get("name", "").strip().lower()
            if name_lower in existing:
                skipped.append(company.get("name", ""))
                logger.info(f"  SKIP (duplicate): {company.get('name', '')}")
            else:
                new_companies.append(company)

        if not new_companies:
            logger.warning("No new companies to add (all duplicates). Skipping sheet update.")
            return {
                "tab_name": tab_name,
                "total_input": len(companies),
                "duplicates_skipped": len(skipped),
                "new_added": 0,
                "skipped_names": skipped
            }

        # Build rows
        rows = [HEADERS]
        sector_advantages = ap_advantages.get("sector_advantages", {})

        for company in new_companies:
            sector_key = company.get("ap_fit_sector", company.get("sector", ""))
            sector_adv = sector_advantages.get(sector_key, {})
            ap_fit_text = sector_adv.get("pitch", "See AP IDP 4.0 policy for sector-specific incentives")

            # Concatenate evidence types from pain signals
            evidence_types = set()
            for signal in company.get("pain_signals", []):
                evidence_types.add(signal.get("evidence_type", "unknown"))

            # Get verified public contact email
            public_contacts = company.get("public_contacts", {})
            verified_emails = company.get("verified_emails", {})
            public_verified = verified_emails.get("public_verified", [])

            # Best public email: first verified, then fallback
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

            # Email pattern for decision maker (not a guess, just the pattern)
            from src.email_patterns import format_pattern_display
            email_domain = company.get("email_domain", company.get("website", ""))
            email_pattern = company.get("email_pattern", format_pattern_display(email_domain))

            row = [
                company.get("name", ""),
                company.get("country", ""),
                company.get("sector", ""),
                company.get("why_target", ""),
                ", ".join(evidence_types),
                company.get("decision_maker_role", ""),
                company.get("linkedin_search", ""),
                public_email,
                email_dept,
                email_status,
                email_pattern,
                ap_fit_text.strip()[:500],
                str(company.get("scores", {}).get("total", 0)),
                "",  # Source URLs
                datetime.now().isoformat(),
                company.get("outreach_email_en", ""),
                company.get("outreach_email_de", ""),
            ]
            rows.append(row)

        if dry_run:
            logger.info(f"[DRY RUN] Would create tab '{tab_name}' with {len(rows) - 1} companies")
            for row in rows[1:]:
                logger.info(f"  > {row[0]} | Score: {row[12]} | Public Email: {row[7] or 'NONE'} | Status: {row[9]}")
            return {
                "tab_name": tab_name,
                "total_input": len(companies),
                "duplicates_skipped": len(skipped),
                "new_added": len(new_companies),
                "skipped_names": skipped,
                "dry_run": True
            }

        # Create new tab
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
            logger.info(f"Wrote {len(rows) - 1} companies to tab '{tab_name}'")
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
