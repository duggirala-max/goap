"""
GoAP Email Verifier -- Free SMTP ping verification of email addresses.
Checks MX records and performs SMTP RCPT TO handshake to verify if a mailbox exists.

WARNING: This is a best-effort approach. Some servers will:
- Accept all addresses (catch-all) giving false positives
- Block verification attempts giving false negatives
- Rate-limit or blacklist aggressive checkers

This module is designed for LOW VOLUME government outreach (20 emails max per run).
"""

import re
import socket
import smtplib
import logging
import time
from typing import Dict, List, Optional, Tuple

try:
    import dns.resolver
    DNS_AVAILABLE = True
except ImportError:
    DNS_AVAILABLE = False
    logging.getLogger(__name__).warning(
        "dnspython not installed. MX record lookup disabled. "
        "Install with: pip install dnspython"
    )

logger = logging.getLogger(__name__)

# Email format validator
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')

# Sender address for SMTP verification (must look legitimate)
VERIFY_FROM = "verify@goap-check.in"


def _get_mx_records(domain: str) -> List[str]:
    """Get MX (Mail Exchange) records for a domain, sorted by priority."""
    if not DNS_AVAILABLE:
        return []

    try:
        mx_records = dns.resolver.resolve(domain, 'MX')
        # Sort by priority (lower = higher priority)
        sorted_mx = sorted(mx_records, key=lambda r: r.preference)
        return [str(r.exchange).rstrip('.') for r in sorted_mx]
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.NoNameservers) as e:
        logger.debug(f"MX lookup failed for {domain}: {e}")
        return []
    except Exception as e:
        logger.debug(f"MX lookup error for {domain}: {e}")
        return []


def _smtp_verify(email: str, mx_host: str, timeout: int = 10) -> Tuple[str, str]:
    """
    Perform SMTP RCPT TO verification against an MX server.

    Returns tuple of (status, detail):
      - ("VALID", "SMTP 250 response") -- mailbox exists
      - ("INVALID", "SMTP 550 response") -- mailbox does not exist
      - ("CATCH_ALL", "Server accepts all") -- server accepts everything (inconclusive)
      - ("UNKNOWN", "reason") -- could not determine
    """
    try:
        smtp = smtplib.SMTP(timeout=timeout)
        smtp.connect(mx_host, 25)
        smtp.ehlo_or_helo_if_needed()

        # Set sender
        code, msg = smtp.mail(VERIFY_FROM)
        if code != 250:
            smtp.quit()
            return ("UNKNOWN", f"MAIL FROM rejected: {code} {msg}")

        # Test the target email
        code, msg = smtp.rcpt(email)
        msg_str = msg.decode('utf-8', errors='replace') if isinstance(msg, bytes) else str(msg)

        # Also test a definitely-fake address to detect catch-all servers
        fake_email = f"goap_fake_test_xyz_99999@{email.split('@')[1]}"
        fake_code, fake_msg = smtp.rcpt(fake_email)

        smtp.quit()

        # If fake address is also accepted, it is a catch-all server
        if code == 250 and fake_code == 250:
            return ("CATCH_ALL", "Server accepts all addresses (catch-all domain)")

        if code == 250:
            return ("VALID", f"Mailbox exists (SMTP {code})")
        elif code == 550 or code == 551 or code == 553:
            return ("INVALID", f"Mailbox not found (SMTP {code}: {msg_str[:80]})")
        elif code == 452 or code == 421:
            return ("UNKNOWN", f"Server busy/rate limited (SMTP {code})")
        else:
            return ("UNKNOWN", f"Unexpected response (SMTP {code}: {msg_str[:80]})")

    except smtplib.SMTPConnectError as e:
        return ("UNKNOWN", f"Connection refused: {e}")
    except smtplib.SMTPServerDisconnected:
        return ("UNKNOWN", "Server disconnected")
    except socket.timeout:
        return ("UNKNOWN", "Connection timed out")
    except socket.gaierror:
        return ("UNKNOWN", f"Could not resolve MX host: {mx_host}")
    except Exception as e:
        return ("UNKNOWN", f"SMTP error: {str(e)[:80]}")


def verify_email(email: str, timeout: int = 10) -> Dict:
    """
    Verify a single email address.

    Steps:
    1. Format validation
    2. MX record lookup
    3. SMTP RCPT TO ping

    Returns dict with:
      - email: str
      - format_valid: bool
      - mx_found: bool
      - mx_host: str or None
      - smtp_status: VALID / INVALID / CATCH_ALL / UNKNOWN
      - smtp_detail: str
      - overall_verdict: VERIFIED / LIKELY / INVALID / UNKNOWN
    """
    result = {
        "email": email,
        "format_valid": False,
        "mx_found": False,
        "mx_host": None,
        "smtp_status": "UNKNOWN",
        "smtp_detail": "",
        "overall_verdict": "UNKNOWN"
    }

    # Step 1: Format check
    if not EMAIL_REGEX.match(email):
        result["smtp_detail"] = "Invalid email format"
        result["overall_verdict"] = "INVALID"
        return result
    result["format_valid"] = True

    # Step 2: MX record lookup
    domain = email.split("@")[1]
    mx_hosts = _get_mx_records(domain)

    if not mx_hosts:
        result["smtp_detail"] = "No MX records found for domain"
        # Domain might still work (A record fallback) but we cannot verify
        result["overall_verdict"] = "UNKNOWN"
        return result

    result["mx_found"] = True
    result["mx_host"] = mx_hosts[0]

    # Step 3: SMTP verification (try top 2 MX servers)
    for mx_host in mx_hosts[:2]:
        status, detail = _smtp_verify(email, mx_host, timeout=timeout)
        result["smtp_status"] = status
        result["smtp_detail"] = detail
        result["mx_host"] = mx_host

        if status in ("VALID", "INVALID"):
            break  # Definitive answer

    # Determine overall verdict
    if result["smtp_status"] == "VALID":
        result["overall_verdict"] = "VERIFIED"
    elif result["smtp_status"] == "CATCH_ALL":
        result["overall_verdict"] = "LIKELY"  # Cannot confirm but domain accepts mail
    elif result["smtp_status"] == "INVALID":
        result["overall_verdict"] = "INVALID"
    else:
        result["overall_verdict"] = "UNKNOWN"

    return result


def generate_candidate_emails(first_name: str, last_name: str, domain: str) -> List[str]:
    """
    Generate candidate email addresses from a name and domain.
    Returns list of possible emails in order of likelihood for German companies.
    """
    first = first_name.lower().strip()
    last = last_name.lower().strip()
    f_initial = first[0] if first else ""

    # Handle German special characters
    replacements = {
        "ae": "ae", "oe": "oe", "ue": "ue",
        "\u00e4": "ae", "\u00f6": "oe", "\u00fc": "ue", "\u00df": "ss"
    }
    for char, repl in replacements.items():
        first = first.replace(char, repl)
        last = last.replace(char, repl)

    candidates = [
        f"{first}.{last}@{domain}",
        f"{f_initial}.{last}@{domain}",
        f"{first}{last}@{domain}",
        f"{last}.{first}@{domain}",
        f"{first}_{last}@{domain}",
    ]

    return candidates


def verify_and_find_valid(candidates: List[str], timeout: int = 10, delay: float = 2.0) -> Optional[Dict]:
    """
    Try multiple candidate emails and return the first verified one.
    Stops as soon as a VALID or LIKELY result is found.
    """
    for i, email in enumerate(candidates):
        logger.debug(f"    Verifying candidate: {email}")
        result = verify_email(email, timeout=timeout)

        if result["overall_verdict"] in ("VERIFIED", "LIKELY"):
            logger.info(f"    Found valid email: {email} ({result['overall_verdict']})")
            return result

        if result["overall_verdict"] == "INVALID":
            logger.debug(f"    Rejected: {email}")

        # Delay between SMTP checks to avoid rate limiting
        if i < len(candidates) - 1:
            time.sleep(delay)

    return None


def verify_companies_emails(companies: List[Dict], delay: float = 2.0) -> List[Dict]:
    """
    For each company, generate candidate decision-maker emails and verify via SMTP.
    Also verifies any scraped public contact emails.

    Adds 'verified_emails' field to each company dict.
    """
    logger.info(f"Verifying emails for {len(companies)} companies via SMTP ping...")

    for i, company in enumerate(companies):
        domain = company.get("email_domain", company.get("website", ""))
        if not domain:
            company["verified_emails"] = {"decision_maker": None, "public_verified": []}
            continue

        logger.info(f"[{i+1}/{len(companies)}] {company.get('name', 'Unknown')} ({domain})")
        verified = {"decision_maker": None, "public_verified": []}

        # 1. Verify public contact emails (scraped)
        public_contacts = company.get("public_contacts", {})
        for email_entry in public_contacts.get("all_emails", []):
            email = email_entry.get("email", "")
            if email:
                result = verify_email(email, timeout=10)
                if result["overall_verdict"] in ("VERIFIED", "LIKELY"):
                    verified["public_verified"].append({
                        "email": email,
                        "department": email_entry.get("department", "unknown"),
                        "verdict": result["overall_verdict"],
                        "detail": result["smtp_detail"]
                    })
                    logger.info(f"    Public email verified: {email} ({result['overall_verdict']})")
                time.sleep(delay * 0.5)

        # 2. Note: We do NOT guess decision-maker emails.
        #    We only include emails that are either:
        #    a) Scraped from public company pages, or
        #    b) If a name is provided in the future, generated and SMTP-verified.
        #    For now, decision_maker stays None since we don't have real names.

        company["verified_emails"] = verified

        # Delay between companies
        if i < len(companies) - 1:
            time.sleep(delay)

    total_with_verified = sum(
        1 for c in companies
        if c.get("verified_emails", {}).get("public_verified")
    )
    logger.info(f"Email verification complete: {total_with_verified}/{len(companies)} have verified public emails")

    return companies
