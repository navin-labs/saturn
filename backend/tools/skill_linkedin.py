"""
Saturn Skill: LinkedIn Outreach
Prompt builders for LinkedIn messages. No API calls, no scraping.
Used by: Hunter (scoring), Echo (messages)
No external dependencies.
"""


def connection_request_prompt(sender: str, contact: str, role: str, company: str, reason: str) -> str:
    return (
        f"Write a LinkedIn connection request from {sender} to {contact} "
        f"({role} at {company}).\n"
        f"Reason: {reason}\n"
        f"Rules:\n"
        f"- Under 280 characters.\n"
        f"- Do not start with Hi or Hello.\n"
        f"- No pitch. Connect first, pitch later.\n"
        f"- Sound like a peer, not a vendor.\n"
        f"- Output the message only. Nothing else."
    )


def inmail_prompt(sender: str, contact: str, role: str, company: str, pain: str, service: str) -> str:
    return (
        f"Write a LinkedIn InMail from {sender} to {contact} ({role} at {company}).\n"
        f"Their pain: {pain}\nYour service: {service}\n"
        f"Rules:\n"
        f"- 3-4 sentences max.\n"
        f"- Line 1: specific observation about their business or role.\n"
        f"- Line 2: the problem this creates.\n"
        f"- Line 3: concrete result for a similar company.\n"
        f"- Line 4: soft ask.\n"
        f"- No 'I hope this message finds you well'.\n"
        f"- Output the message only."
    )


def score_prompt(profile_text: str, niche: str) -> str:
    return (
        f"Score this LinkedIn profile as a sales prospect for {niche} services.\n"
        f"Profile:\n{profile_text}\n\n"
        f"Respond with ONLY a valid JSON object. No markdown. No explanation.\n"
        f"Format: "
        f'{{"score":0-100,"priority":"hot|warm|cold",'
        f'"signals":["..."],"red_flags":["..."],'
        f'"angle":"one sentence approach"}}'
    )


def parse_score(llm_output: str) -> dict:
    """Safely parse LLM score output. Returns error dict on failure."""
    import json
    import re

    text = llm_output.strip()
    text = re.sub(r"^```[a-z]*\n?", "", text)
    text = re.sub(r"\n?```$", "", text)
    text = text.strip()
    try:
        data = json.loads(text)
        score = int(data.get("score", 0))
        return {
            "status": "success",
            "score": score,
            "priority": data.get("priority", "cold"),
            "signals": data.get("signals", []),
            "red_flags": data.get("red_flags", []),
            "angle": data.get("angle", ""),
        }
    except Exception as e:
        return {"status": "parse_error", "reason": str(e), "raw": llm_output[:200]}
