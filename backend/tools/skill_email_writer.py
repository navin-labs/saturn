"""
Saturn Skill: Human Email Writer
Generates prompts for cold outreach, follow-ups, and replies.
Does NOT call LLM directly — returns prompts for call_llm() in saturn-server.py.
No external dependencies.
"""

_BANNED = [
    "hope this email finds you well",
    "i wanted to reach out",
    "circle back",
    "touch base",
    "synergy",
    "synergies",
    "leverage",
    "deep dive",
    "moving forward",
    "please do not hesitate",
    "feel free to",
    "as per my last email",
    "i am writing to",
    "i hope you are doing well",
    "best regards",
    "kind regards",
]


def outreach_prompt(
    sender: str,
    contact: str,
    company: str,
    pain: str,
    service: str,
    result: str = "",
) -> str:
    result_line = f"\nProof: {result}" if result else ""
    return (
        f"You are {sender}, founder of an AI automation agency.\n"
        f"Write a cold email to {contact} at {company}.\n"
        f"Their pain: {pain}\nYour offer: {service}{result_line}\n\n"
        f"Rules:\n"
        f"- Max 5 sentences. Short paragraphs.\n"
        f"- Line 1: one specific observation about their business.\n"
        f"- Line 2: the problem this creates for them.\n"
        f"- Line 3: one concrete result you got for a similar business.\n"
        f"- Line 4: soft specific ask (15-min call or reply with one word).\n"
        f"- Sign as {sender} only. No company name in sign-off.\n"
        f"- Do NOT use: {', '.join(_BANNED[:6])}\n"
        f"- Write like a human, not a sales email.\n"
        f"- Output: subject line on first line, blank line, then body."
    )


def followup_prompt(sender: str, contact: str, summary: str, num: int = 1) -> str:
    if num == 1:
        tone = "short, curious, 2 sentences max. Ask if they saw the previous email."
    else:
        tone = "final. Assume they are busy. Leave door open in 1 sentence. No re-pitch."
    return (
        f"You are {sender}. Write follow-up #{num} to {contact}.\n"
        f"Original email was about: {summary}\n"
        f"Tone: {tone}\n"
        f"Rules: No re-pitching. No 'just following up'. Sound human."
    )


def reply_prompt(sender: str, contact: str, their_msg: str, context: str = "") -> str:
    return (
        f"You are {sender}. Reply to this message from {contact}:\n"
        f"---\n{their_msg}\n---\n"
        f"Context: {context}\n\n"
        f"Rules:\n"
        f"- Match their energy and length.\n"
        f"- Interested -> advance to next step.\n"
        f"- Objection -> acknowledge, one fact, re-ask.\n"
        f"- No -> thank them, leave door open in one sentence.\n"
        f"- Sound like a real person."
    )


def validate(text: str) -> dict:
    issues = []
    lower = text.lower()
    for phrase in _BANNED:
        if phrase in lower:
            issues.append(f"banned: '{phrase}'")
    words = text.split()
    if len(words) > 160:
        issues.append(f"too long: {len(words)} words")
    if len(words) < 15:
        issues.append(f"too short: {len(words)} words")
    return {"valid": not issues, "issues": issues, "word_count": len(words)}
