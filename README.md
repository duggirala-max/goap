# GoAP — Government Outreach for Andhra Pradesh FDI

Automated pipeline to identify EU/German companies under operational pressure and generate a high-confidence target account list for FDI outreach.

**Output:** Google Sheets with date-stamped tabs containing scored companies, pain signals, outreach strategies, and AI-generated emails.

---

## How It Works

```
Manual Trigger (GitHub Actions)
    │
    ├── Load 20 curated seed companies (verified from 2024-2025 reports)
    ├── Scan Google News RSS for fresh pain signals
    ├── Deduplicate against existing Google Sheet entries
    ├── Score companies (pain × size × news activity)
    ├── Generate targeted outreach emails via Groq AI
    └── Push to new Google Sheet tab (date-stamped)
```

## Target Sectors
- Automotive Components
- EV / Electric Mobility
- Manufacturing / Industrial
- Food Processing
- Food Processing Equipment

## Quick Start

### 1. Google Cloud Service Account Setup

> **This is required.** The app password for Gmail does NOT work for Google Sheets API.

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or use existing)
3. Enable **Google Sheets API**:
   - Go to APIs & Services → Library
   - Search "Google Sheets API" → Enable
4. Create a Service Account:
   - Go to IAM & Admin → Service Accounts
   - Click **+ Create Service Account**
   - Name: `goap-sheets-writer`
   - Role: Editor
   - Click Done
5. Create a key:
   - Click on the service account
   - Go to Keys tab → Add Key → Create new key → JSON
   - **Download the JSON file** — you'll need its contents
6. Share your Google Sheet:
   - Open [your sheet](https://docs.google.com/spreadsheets/d/15KyIdUs0F8L9ST5wb8WyWfEgwWjurAEzDLYsrG6_aHA)
   - Click **Share**
   - Paste the service account email (found in the JSON file under `client_email`)
   - Give **Editor** access

### 2. GitHub Secrets

In your GitHub repo, go to Settings → Secrets and variables → Actions → New repository secret:

| Secret Name | Value |
|---|---|
| `GCP_SERVICE_ACCOUNT_JSON` | Entire contents of the downloaded JSON key file |
| `GOOGLE_SHEET_ID` | `15KyIdUs0F8L9ST5wb8WyWfEgwWjurAEzDLYsrG6_aHA` |
| `GROQ_API_KEY` | Your Groq API key from [console.groq.com](https://console.groq.com) |

### 3. Run the Pipeline

1. Go to your repo → Actions tab
2. Click **"GoAP — Update FDI Target List"**
3. Click **"Run workflow"**
4. Configure options:
   - **Skip news**: Use seed list only (faster)
   - **Skip groq**: Skip email generation
   - **Max companies**: Default 20
   - **Dry run**: Preview without updating sheet
5. Click **"Run workflow"**

### 4. Check Results

Open your [Google Sheet](https://docs.google.com/spreadsheets/d/15KyIdUs0F8L9ST5wb8WyWfEgwWjurAEzDLYsrG6_aHA) — a new tab will appear with the date/time of the run.

---

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Dry run (no Sheets, no Groq)
python src/main.py --dry-run --skip-groq --skip-news

# Full dry run with news scanning
python src/main.py --dry-run --skip-groq

# Full run with Groq (needs API key)
export GROQ_API_KEY="your-key-here"
python src/main.py --dry-run
```

## Output Columns

| Column | Description |
|---|---|
| Company Name | Verified company name |
| Country | HQ country |
| Sector | Target sector |
| Why Target? | Specific pain signal with evidence |
| Evidence Type | news_report / sector_trend / known_issue |
| Decision Maker Role | Suggested role (NOT a name) |
| LinkedIn Search Strategy | How to find the right contact |
| Email Pattern | e.g., firstname.lastname@company.de |
| Email Confidence | VERIFIED / LIKELY / UNKNOWN |
| Why AP Fits | Sector-specific AP advantage |
| Confidence Score | Numeric score (higher = stronger target) |
| Source | Evidence reference |
| Last Updated | Timestamp |
| Outreach Email Draft | Groq-generated targeted email |

## Editing the Seed List

To add or modify companies, edit `data/seed_companies.yaml`. Follow the existing format. Rules:

1. **No fabricated data** — every entry must be based on real reporting
2. **Mark confidence levels** — VERIFIED / LIKELY / UNKNOWN
3. **Include evidence type** — news_report / sector_trend / known_issue

## Deduplication

The pipeline checks ALL existing tabs in the Google Sheet before adding new companies. If a company name already exists in any tab, it will be skipped in the new run.

---

## Project Structure

```
GoAP/
├── .github/workflows/update_targets.yml   # GitHub Actions workflow
├── config/
│   ├── sector_config.yaml                 # Sector keywords + scoring weights
│   └── ap_advantages.yaml                 # AP FDI incentives per sector
├── data/
│   └── seed_companies.yaml                # Curated company list (20 real companies)
├── src/
│   ├── __init__.py
│   ├── main.py                            # Pipeline orchestrator
│   ├── news_scanner.py                    # Google News RSS scanner
│   ├── scoring_engine.py                  # Pain signal scoring
│   ├── email_patterns.py                  # Email pattern database
│   ├── groq_enricher.py                   # Groq AI email generator
│   └── sheets_pusher.py                   # Google Sheets integration
├── requirements.txt
└── README.md
```
