"""Hand-templated educational explanations for each signal type.

Each signal type gets a format string that's filled with evidence
from the signal's details dict. The templates are meant to be
iterated on as you read real signals on the dashboard.
"""

_ROLE_PHRASES = {
    "CHAIR": "Chair",
    "RANKING_MEMBER": "Ranking Member",
    "MEMBER": "a member",
}

SIGNAL_TEMPLATES: dict[str, str] = {
    "COMMITTEE_TRADE": (
        "{member} sits on the {committee_name} as {role_phrase}. "
        "This committee oversees the {sector} sector, the same sector as "
        "their {trade_type} of {ticker}. Members with direct sector oversight "
        "have access to private briefings and early knowledge of policy shifts "
        "that can move stocks before the public hears about them. "
        "This doesn't prove misuse, but it's a flag worth watching."
    ),
    "LEGISLATION_TIMING": (
        "{member} {trade_verb} {ticker} ({sector}) {proximity_phrase} "
        "a relevant legislative action on {bill_id}. "
        "The bill affects the {sector} sector, the same sector as the trade. "
        "Closer timing between trades and votes increases the chance that "
        "policy knowledge influenced the trade, but coincidences happen too."
    ),
}


def render_explanation(signal_type: str, details: dict) -> str:
    """Render a plain-English explanation for a signal.

    Args:
        signal_type: e.g. "COMMITTEE_TRADE" or "LEGISLATION_TIMING"
        details: the signal's evidence dict with keys matching template vars

    Returns:
        A human-readable explanation string.
    """
    template = SIGNAL_TEMPLATES.get(signal_type, "")
    if not template:
        return ""

    # Build template variables from the details dict
    variables = dict(details)

    # Add computed phrases
    role = details.get("role", "MEMBER")
    variables["role_phrase"] = _ROLE_PHRASES.get(role, "a member")

    trade_type = details.get("trade_type", "PURCHASE")
    variables["trade_verb"] = "bought" if trade_type == "PURCHASE" else "sold"

    # Proximity phrase for legislation timing
    proximity_days = details.get("proximity_days")
    if proximity_days is not None:
        abs_days = abs(proximity_days)
        direction = "before" if proximity_days < 0 else "after"
        variables["proximity_phrase"] = f"{abs_days} days {direction}"
    else:
        variables["proximity_phrase"] = "near"

    # Truncate bill title if present
    bill_title = details.get("bill_title", "")
    if len(bill_title) > 60:
        variables["bill_title_short"] = bill_title[:57] + "..."
    else:
        variables["bill_title_short"] = bill_title

    try:
        return template.format(**variables)
    except KeyError:
        # If template vars are missing, return what we can
        return template.format_map(
            {k: variables.get(k, f"[{k}]") for k in _extract_keys(template)}
        )


def _extract_keys(template: str) -> list[str]:
    """Extract format string keys from a template."""
    import string
    return [
        fname
        for _, fname, _, _ in string.Formatter().parse(template)
        if fname is not None
    ]
