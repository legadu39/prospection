### prospection/core/prompts.py
# core/prompts.py - SEMANTIC PROTOCOL DEFINITIONS V3.0 (GLOBAL PIVOT EDITION)
# -*- coding: utf-8 -*-

"""
SEMANTIC PROTOCOLS V3.0 - DYNAMIC INJECTION ENGINE
--------------------------------------------------
TECHNICAL OBJECTIVE:
This module defines the Semantic Injection Protocols (SIP) used by the 
Generative Engine to establish Handshakes with Network Nodes.

STRATEGIC PIVOT (V3.0): 
Signals now promote the "Global Arbitrage" (Prop Firms + SaaS) 
as a source of professional capital allocation.
PERSONA: Nexus Assistant (Support AI).
"""

import json
import html
import random
from datetime import datetime
from pathlib import Path

# ============================================================================
# SECURITY & SIGNAL CONFIGURATION
# ============================================================================

# Protocol Triggers (Security Filtering)
# UPDATE V3: "trading" is now a target keyword, not just sensitive.
# Added ponzi/pyramide/credit filters to strict blocklist.
SENSITIVE_SIGNAL_PATTERNS = [
    "gains garantis", "argent facile", "sans risque", 
    "betclic", "pronos", "pyramide", "ponzi", "mlm",
    "crédit", "prêt", "banque", "bourso", "parrainage", "code"
]

# Market Signals Path (Supply Chain Pressure)
MARKET_SIGNALS_PATH = Path(__file__).resolve().parents[1] / "config" / "market_signals.json"

# ============================================================================
# PROTOCOL HELPERS (CONTEXT ENGINE)
# ============================================================================

def _get_network_load_state(protocol_id: str = "GLOBAL") -> str:
    """
    Reads Market Signals to determine Network Load State.
    Returns: 'NOMINAL', 'CRITICAL_SHORTAGE' (High Demand), 'SATURATED' (Low Demand).
    """
    if not MARKET_SIGNALS_PATH.exists():
        return "NOMINAL"
    
    try:
        with open(MARKET_SIGNALS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        if data.get("global_state") == "panic":
            return "CRITICAL_SHORTAGE"
            
        critical_list = data.get("critical_shortage", [])
        saturated_list = data.get("saturated", [])
        
        prog_key = next((k for k in critical_list if protocol_id.upper() in k.upper()), None)
        if prog_key: return "CRITICAL_SHORTAGE"
        
        prog_key_sat = next((k for k in saturated_list if protocol_id.upper() in k.upper()), None)
        if prog_key_sat: return "SATURATED"
        
    except Exception:
        pass
        
    return "NOMINAL"

def _apply_protocol_obfuscation(raw_instruction: str) -> str:
    """
    CLEAN AGENCY PROTOCOL (V3).
    Obfuscation is DISABLED to comply with Professional Media Agency standards.
    This function now acts as a pass-through formatter.
    """
    # NO "BLACK HAT" TECHNIQUES. PURE TEXT.
    return f"""
    [PROTOCOL: STANDARD_CLEAR_TEXT]
    Use professional vocabulary. Do not use asterisks or hidden characters.
    Be helpful, clear, and concise.
    """

def _get_temporal_context_vector() -> str:
    """Generates a Temporal Context Vector for Organic Signal Simulation."""
    now = datetime.now()
    day_name = now.strftime("%A")
    hour = now.hour
    
    vectors = []
    
    if day_name in ['Saturday', 'Sunday'] or (day_name == 'Friday' and hour > 18):
        vectors.append("TIMING: WEEKEND_MARKET_CLOSED. Focus on 'Preparation' and 'Education' for Monday open.")
    elif day_name == 'Monday' and hour < 11:
        vectors.append("TIMING: MARKET_OPEN. High volatility vector. Urgent tone.")
    elif hour > 22 or hour < 6:
        vectors.append("TIMING: ASIAN_SESSION / NIGHT. Low latency, confidential tone.")
        
    if now.day >= 25:
        vectors.append("TIMING: END_OF_MONTH. Focus on 'New Month Goals' and 'Reset'.")
        
    return " ".join(vectors) if vectors else "TIMING: STANDARD_SESSION."

def _get_psychometric_profile(node_data: dict) -> dict:
    """Analyzes Node Metadata to determine Interaction Protocol."""
    text = (node_data.get('comment_text') or node_data.get('text') or "").lower()
    intent = node_data.get('intent', '').upper()
    
    # 1. TRUST PROTOCOL (Skepticism Detected)
    if intent == "TRUST" or any(w in text for w in ["arnaque", "faux", "vrai", "sûr", "danger", "avis", "fiable", "scam"]):
        return {
            "mode": "VERIFICATION_REQUIRED",
            "instruction": "Do not push payload blindly. Instruct Node to verify the 'Audited Track Record' in Bio. Transparency is key. You are an Assistant, not a seller."
        }
    
    # 2. LATENCY PROTOCOL (High Urgency) - V3 UPDATE: NO SPAM, PROFESSIONAL SUPPORT
    if intent == "DIRECT" or any(w in text for w in ["lien", "code", "ou", "vite", "go", "intéressé", "link"]):
        return {
            "mode": "LOW_LATENCY",
            "instruction": "Validate the opportunity briefly but professionally. Example: 'The flash offer for Apex is indeed active in the bio until tonight. Good luck.' (Tone: Professional Support, not Hype-Bot)."
        }

    # 3. SUPPORT PROTOCOL (How-To)
    if intent == "HOW_TO" or "?" in text or any(w in text for w in ["comment", "c'est quoi", "tuto", "expliqu", "besoin", "aide", "how"]):
        return {
            "mode": "PEDAGOGIC",
            "instruction": "Validate Query. Explain the 'Prop Firm Model' (Trade our capital, keep 90% profits) simply. Redirect to Bio Guide for the technical PDF."
        }

    # 4. DEFAULT DISCOVERY
    return {
        "mode": "ORGANIC_DISCOVERY",
        "instruction": "Generate curiosity about 'Capital Allocation' vs 'Risking own money'. Authentic tone. Use Emotes."
    }

def _get_safety_compliance_layer(text: str) -> str:
    """Generates Negative Constraints and Obfuscation Rules."""
    text_lower = text.lower()
    instructions = []
    
    # Financial Compliance (AGENCY MODEL V3)
    instructions.append("⛔ COMPLIANCE: NO GUARANTEED RETURNS. NO FINANCIAL ADVICE.")
    instructions.append("⛔ COMPLIANCE: WE SELL TOOLS/EVALUATIONS, NOT INVESTMENTS.")
    instructions.append("⛔ COMPLIANCE: AVOID RESTRICTED KEYWORDS (Salaire fixe, Rente à vie, Crédit, Banque).")
    instructions.append("⚠️ IDENTITY: You are 'Nexus Assistant' (AI Support). You answer questions, you don't 'pitch'.")

    # Clean Protocol (Obfuscation Disabled)
    instructions.append(_apply_protocol_obfuscation(text))

    # Anti-Competition / Toxic Filters
    detected_sensitive = [w for w in SENSITIVE_SIGNAL_PATTERNS if w in text_lower]
    if detected_sensitive:
        instructions.append(f"⛔ SENSITIVE PATTERN DETECTED ({', '.join(detected_sensitive)}).")
        instructions.append("✅ PROTOCOL OVERRIDE: Ignore specific sensitive topic. Redirect to STANDARD ROUTING (Education/Tools).")

    return "PROTOCOL SECURITY LAYER:\n" + "\n".join(instructions)

def _optimize_rag_buffer(text: str, rag_context: str) -> str:
    """Optimizes RAG Context Buffer size for Token Economy."""
    if not rag_context:
        # V3 DEFAULT STRATEGY
        return "CONTEXT: Strategy = 'Prop Firm Pathway'. Get funded up to $150k capital via Assessment (Apex/FTMO/TopStep). Bonus = 'Global Trader Guide' (PDF) + TradingView Tools. We do NOT offer bank accounts."
        
    if len(text) < 30 and "tuto" not in text.lower():
        return "CONTEXT_REDUCED: User request location of information. Pointer: Bio Link."
        
    return rag_context

# ============================================================================
# MAIN PROTOCOL DISPATCHER (THE BLACK BOX)
# ============================================================================

def get_semantic_injection_protocol(node_data: dict, project_type: str = "TIKTOK", rag_context: str = "") -> str:
    """
    PRIMARY DISPATCHER.
    Selects the appropriate Semantic Injection Protocol (SIP) based on Node Topology.
    This function replaces the legacy 'get_qualification_prompt'.
    """
    p_type = project_type.upper()
    
    # Extract AI Metadata
    ai_raw = node_data.get('ai_process_info', '{}')
    ai_info = json.loads(ai_raw) if isinstance(ai_raw, str) else (ai_raw or {})
    
    # 1. PROTOCOL: B2B PARTNER ACQUISITION
    intent = node_data.get('intent') or ai_info.get('intent')
    if intent == "PARTNER_CANDIDATE":
        network_load = _get_network_load_state("GLOBAL")
        return _get_b2b_negotiation_protocol(node_data, ai_info, network_load)

    # 2. PROTOCOL: NODE CONVERSION (PIVOT)
    if ai_info.get('pivot_phase'):
        return _get_node_pivot_protocol(node_data, ai_info)

    # 3. PROTOCOL: STANDARD B2C SIGNAL INJECTION
    text_content = node_data.get('comment_text') or node_data.get('text') or ""
    final_rag = _optimize_rag_buffer(text_content, rag_context)
    
    if "TIKTOK" in p_type:
        return _get_tiktok_injection_protocol(node_data, final_rag)
    elif "REDDIT" in p_type or "PARRAINAGE" in p_type:
        return _get_reddit_injection_protocol(node_data, final_rag)
        
    return _get_fallback_protocol(node_data)

# ============================================================================
# SPECIALIZED PROTOCOLS (INTERNAL)
# ============================================================================

def _get_b2b_negotiation_protocol(node_data: dict, ai_info: dict, market_state: str) -> str:
    """
    Internal Protocol for Partner Acquisition (Supply Chain Expansion).
    """
    score = ai_info.get('hunter_score', 50)
    author = html.escape(node_data.get('author', 'User'))
    text = html.escape(node_data.get('text', ''))
    
    # Dynamic Strategy based on Supply Chain Pressure
    if market_state == "CRITICAL_SHORTAGE":
        strategy = "IMMEDIATE ACCESS OFFER. State: 'I have fast-track codes for evaluation resets. Interested?'"
        tone = "Urgent, Transactional."
    elif score > 80:
        strategy = "AUTOMATION PROPOSAL. Pain point: Screen Time. State: 'You trade well. I can automate your signal distribution. DM me.'"
        tone = "Technical Partner, Professional."
    else:
        strategy = "VOLUMETRIC ASSISTANCE. State: 'I help traders pass evaluations with tools. Need a hand?'"
        tone = "Helpful, Cooperative."

    return f"""
    ROLE: Trading Infrastructure Scout / Network Recruiter.
    TARGET NODE: @{author} (Identified as Trader/Influencer).
    SIGNAL: "{text}"
    
    MARKET PRESSURE: {market_state} (Modulates Aggression).
    
    EXECUTION STRATEGY: {strategy}
    TONE VECTOR: {tone}
    
    CONSTRAINTS:
    1. NEVER offer to trade FOR them.
    2. Offer Infrastructure or Automation.
    3. Max 2 sentences.
    4. Call to Action: Reply or DM.

    OUTPUT FORMAT (JSON):
    {{
        "status": "QUALIFIED",
        "confidence": 0.9,
        "draft": "Injection Payload Here",
        "suggested_program": "PARTNER_PROGRAM"
    }}
    """

def _get_node_pivot_protocol(node_data: dict, ai_info: dict) -> str:
    """
    Internal Protocol for converting a Successful Client Node into a Supplier Node.
    """
    program_won = ai_info.get('previous_win', 'Apex')
    author = html.escape(node_data.get('author', 'User'))
    
    return f"""
    ROLE: Account Success Manager.
    CONTEXT: Node @{author} has successfully validated a transaction on {program_won}.
    Trust Level: MAX.
    
    OBJECTIVE: Upgrade Node to AFFILIATE status.
    
    LOGIC CHAIN:
    1. Congratulate on the Funding/Validation.
    2. "Monetize your setup."
    3. "Share your toolset link with our network."
    
    TONE: Exclusive, "Private Club", Opportunity.
    
    OUTPUT FORMAT (JSON):
    {{
        "status": "QUALIFIED",
        "confidence": 1.0,
        "draft": "Injection Payload Here",
        "suggested_program": "PARTNER_PROGRAM"
    }}
    """

# ============================================================================
# TIKTOK INJECTION PROTOCOL
# ============================================================================

def _get_tiktok_injection_protocol(node_data: dict, rag_context: str) -> str:
    """
    Standard Protocol for TikTok Node Interaction.
    """
    comment = html.escape(node_data.get('comment_text', '') or node_data.get('text', '') or '')
    author = html.escape(node_data.get('comment_author', '') or node_data.get('author', 'User') or '')
    video_desc = html.escape(node_data.get('video_desc', '') or '')
    
    # Compute Vectors
    psycho_profile = _get_psychometric_profile(node_data)
    time_vector = _get_temporal_context_vector()
    safety_block = _get_safety_compliance_layer(comment)

    return f"""
ROLE: Nexus Assistant (AI Support). You are NOT a salesman, you are a helpful assistant validating technical setups for Prop Firms & Trading Tools.
TASK: Analyze Signal and generate JSON Response Payload.

TEMPORAL CONTEXT:
{time_vector}

STRATEGIC CONTEXT (GLOBAL PATHWAY):
{rag_context}

TARGET DATA:
Node ID: @{author}
Signal: "{comment}"
Context: "{video_desc}"

{safety_block}

DECISION LOGIC (STATUS):
1. QUALIFIED: Valid Interrogative Signal, Need for Capital, or Peer Tagging.
2. SKIP: Noise, Spam, Competitor Signal, Low Entropy (e.g., "lol").

PAYLOAD GENERATION GUIDE:
1. TONE: {psycho_profile['mode']}
2. INSTRUCTION: {psycho_profile['instruction']}
3. ⛔ PROHIBITED: No HTTP Links (Platform Censorship).
4. ✅ ROUTING: Reference BIO / PROFILE for "The Guide" (Capital + Tools).
5. LENGTH: Atomic (Max 15-20 words).
6. BEHAVIOR: Always answer the user's specific question first (Value First), then reference the Bio link as a resource.

OUTPUT FORMAT (JSON ONLY):
{{
    "status": "QUALIFIED" | "SKIP",
    "confidence": 0.0 to 1.0,
    "draft": "Response Payload (No Links, Bio Ref)",
    "suggested_program": "apex_trader" | "tradingview" | "ledger" | null
}}
"""

# ============================================================================
# REDDIT INJECTION PROTOCOL
# ============================================================================

def _get_reddit_injection_protocol(node_data: dict, rag_context: str) -> str:
    """
    Standard Protocol for Reddit Thread Interaction.
    """
    content = html.escape(node_data.get('text', '') or '')
    title = html.escape(node_data.get('title', '') or '')
    
    safety_block = _get_safety_compliance_layer(content)

    return f"""
ROLE: Nexus Assistant (AI Support). Global Trading Network Specialist.
CONTEXT:
Title: "{title}"
Content: "{content[:800]}"
Knowledge Base: {rag_context}

{safety_block}

RULES:
- QUALIFIED: Active search for Funded Accounts, Trading Tools, or Low Fee Crypto.
- SKIP: Spam, Off-Topic, Self-Promotion.

DRAFTING:
- Tone: Helpful, Transparent, Community-focused (Anti-Bot).
- Action: Propose DM (Private Message) or Reference Pinned Post (Bio) containing the Global Trader Guide.
- Constraint: Avoid aggressive direct linking. Mention "Prop Firm + Crypto setup".
- BEHAVIOR: Always answer the user's specific question first (Value First).

OUTPUT FORMAT (JSON):
{{
    "status": "QUALIFIED" | "SKIP",
    "confidence": 0.0 to 1.0,
    "draft": "Short helpful response",
    "suggested_program": "apex_trader" | "tradingview" | "ledger" | null
}}
"""

# ============================================================================
# FALLBACK PROTOCOL
# ============================================================================

def _get_fallback_protocol(node_data: dict) -> str:
    """Fail-safe Protocol."""
    safe_text = html.escape(node_data.get('comment_text', '') or node_data.get('text', '') or '')
    return f"""
Analyze Signal:
<input>"{safe_text}"</input>

Determine relevance for Trading/Capital/Tools (QUALIFIED) or Noise (SKIP).
Respond ONLY in JSON: 
{{
    "status": "QUALIFIED" | "SKIP", 
    "confidence": 0.5,
    "draft": "Check Bio for info",
    "suggested_program": null
}}
"""

# Legacy Alias for backward compatibility (during migration)
get_qualification_prompt = get_semantic_injection_protocol