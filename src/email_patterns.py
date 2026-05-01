"""
GoAP Email Patterns — Known email format patterns for German/EU companies.
"""

import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


# Known email patterns for German corporate domains
# Format: domain -> (pattern_template, confidence)
# Pattern placeholders: {first}, {last}, {f} (first initial), {l} (last initial)

KNOWN_PATTERNS = {
    # Automotive
    "zf.com": ("{first}.{last}@zf.com", "LIKELY"),
    "continental.com": ("{first}.{last}@continental.com", "LIKELY"),
    "schaeffler.com": ("{first}.{last}@schaeffler.com", "LIKELY"),
    "webasto.com": ("{first}.{last}@webasto.com", "LIKELY"),
    "brose.com": ("{first}.{last}@brose.com", "LIKELY"),
    "mahle.com": ("{first}.{last}@mahle.com", "LIKELY"),
    "hella.com": ("{first}.{last}@hella.com", "LIKELY"),
    "elringklinger.de": ("{first}.{last}@elringklinger.de", "LIKELY"),
    "safholland.com": ("{first}.{last}@safholland.com", "LIKELY"),

    # Industrial / Manufacturing
    "durr.com": ("{first}.{last}@durr.com", "LIKELY"),
    "sma.de": ("{first}.{last}@sma.de", "LIKELY"),
    "gea.com": ("{first}.{last}@gea.com", "LIKELY"),
    "krones.com": ("{first}.{last}@krones.com", "LIKELY"),
    "multivac.com": ("{first}.{last}@multivac.com", "LIKELY"),

    # Food Processing
    "suedzucker.de": ("{first}.{last}@suedzucker.de", "LIKELY"),
    "dmk.de": ("{first}.{last}@dmk.de", "UNKNOWN"),
    "toennies.de": ("{first}.{last}@toennies.de", "UNKNOWN"),
}

# Common German corporate email patterns (fallback)
COMMON_PATTERNS = [
    ("{first}.{last}@{domain}", "LIKELY"),
    ("{f}.{last}@{domain}", "UNKNOWN"),
    ("{first}{last}@{domain}", "UNKNOWN"),
]


def get_email_pattern(domain: str) -> Dict:
    """
    Get the email pattern for a given domain.
    Returns dict with 'pattern' and 'confidence'.
    """
    domain = domain.lower().strip()

    if domain in KNOWN_PATTERNS:
        pattern, confidence = KNOWN_PATTERNS[domain]
        return {
            "pattern": pattern,
            "confidence": confidence,
            "source": "known_pattern"
        }

    # Fallback to most common German corporate pattern
    return {
        "pattern": f"firstname.lastname@{domain}",
        "confidence": "UNKNOWN",
        "source": "generic_pattern"
    }


def format_pattern_display(domain: str) -> str:
    """
    Format the email pattern for display in the output table.
    E.g., "firstname.lastname@zf.com"
    """
    info = get_email_pattern(domain)
    pattern = info["pattern"]
    # Replace placeholders with readable format
    pattern = pattern.replace("{first}", "firstname")
    pattern = pattern.replace("{last}", "lastname")
    pattern = pattern.replace("{f}", "f")
    pattern = pattern.replace("{l}", "l")
    pattern = pattern.replace("{domain}", domain)
    return pattern


def get_confidence(domain: str) -> str:
    """Get email confidence level for a domain."""
    return get_email_pattern(domain)["confidence"]
