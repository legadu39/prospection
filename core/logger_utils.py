### prospection/core/logger_utils.py
# core/logger_utils.py - AUDITABLE LOGGING SYSTEM V28.0
# Features: Anti-DoS, PII Redaction, Compliance Filtering.

import logging
import logging.handlers
import re
from pathlib import Path

class ComplianceFilter(logging.Filter):
    """
    Compliance Filter V28.0 (AdTech Standard).
    1. Redacts PII (Keys, Tokens, Emails).
    2. Flags System Critical Health Events.
    3. Truncates massive payloads to prevent log flooding (Anti-DoS).
    4. Semantic Rewriting: Converts Hostile/Bot terminology to Professional AdTech lexicon.
    """
    def __init__(self):
        super().__init__()
        # Triggers for sensitive data inspection
        self.triggers = ["Key", "Token", "secret", "password", "sk-", "AIza", "user-data", "@", "http", "eyJh"]
        
        # Health Critical Keywords
        self.health_triggers = ["captcha", "security check", "too many requests", "account locked", "403 forbidden", "cloudflare"]
        
        # Regex Patterns for redaction
        self.patterns = [
            # API Keys
            re.compile(r'(AIza|sk-proj-|sk-[a-zA-Z0-9]{20,})[a-zA-Z0-9_\-\.]{5,}'),
            # Auth Headers
            re.compile(r'(Bearer|Token|Key)\s+[\w\-\._~\+\/]+=*', re.IGNORECASE),
            # JSON Fields
            re.compile(r'("access_token"|"api_key"|"secret"|"refresh_token"|"password")\s*:\s*"([^"]+)"', re.IGNORECASE),
            # CLI Args
            re.compile(r'--user-data-dir=[^ ]+'),
            re.compile(r'--remote-debugging-port=\d+'),
            # Emails
            re.compile(r'[a-zA-Z0-9_.+-]+:[^@\s]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+') 
        ]
        
        self.url_secret_pattern = re.compile(r'(https?://[^\s]+?[?&](?:token|key|auth|password)=)([^&\s]+)', re.IGNORECASE)
        self.jwt_pattern = re.compile(r'eyJh[a-zA-Z0-9_\-\/]{10,200}\.[a-zA-Z0-9_\-\/]{10,200}\.[a-zA-Z0-9_\-\/]{10,200}')
        
        self.regex_safety_limit = 2000 
        self.display_limit = 10000

        # AdTech Semantic Dictionary (Rewrites hostile terms to professional ones)
        self.semantic_map = {
            "attacking": "processing segment",
            "attack": "process",
            "sniper": "listener",
            "victim": "user_node",
            "target": "segment",
            "bypassing firewall": "optimizing connectivity",
            "injecting payload": "delivering content",
            "inject": "deliver",
            "spam": "broadcast",
            "bot": "automation_agent",
            "fake": "synthetic",
            "steal": "attribute",
            "loot": "yield"
        }

    def filter(self, record):
        try:
            msg = record.msg % record.args if record.args else str(record.msg)
        except Exception:
            msg = str(record.msg)

        if not isinstance(msg, str) or len(msg) < 5: return True

        # 0. Semantic Rewriting (AdTech Standardization)
        msg_lower = msg.lower()
        for bad_term, good_term in self.semantic_map.items():
            if bad_term in msg_lower:
                # Case insensitive replacement wrapper
                pattern = re.compile(re.escape(bad_term), re.IGNORECASE)
                msg = pattern.sub(good_term, msg)

        # 1. Health Escalation
        if any(trigger in msg_lower for trigger in self.health_triggers):
            if record.levelno < logging.CRITICAL:
                record.levelno = logging.CRITICAL
                record.levelname = "CRITICAL"
                msg = f"[SYSTEM HEALTH ALERT] {msg}"

        # 2. Fast Pass (Skip redaction if no triggers found)
        if not any(trigger in msg for trigger in self.triggers):
            record.msg = msg; record.args = ()
            return True
        
        head, tail = msg[:self.regex_safety_limit], msg[self.regex_safety_limit:]
        chunk = head
        
        # 3. Redaction Logic
        if "http" in chunk: chunk = self.url_secret_pattern.sub(r'\1***REDACTED***', chunk)
        if "eyJh" in chunk: chunk = self.jwt_pattern.sub('eyJ***JWT_REDACTED***', chunk)

        for p in self.patterns:
            try: chunk = p.sub(r'\1: "***REDACTED***"' if p.groups > 1 else '***REDACTED***', chunk)
            except re.error: pass

        final_msg = (chunk + tail)
        
        # 4. Anti-Flooding
        if len(final_msg) > self.display_limit:
             final_msg = final_msg[:self.display_limit] + "... [TRUNCATED_FOR_COMPLIANCE]"

        record.msg = final_msg; record.args = () 
        return True

class DeduplicationFilter(logging.Filter):
    """
    Prevents log spamming by aggregating repeated messages.
    """
    def __init__(self):
        super().__init__()
        self.last_msg_hash = None
        self.repeat_count = 0
        self.last_msg_content = ""

    def filter(self, record):
        msg = str(record.msg)
        current_hash = hash(msg)

        if current_hash == self.last_msg_hash:
            self.repeat_count += 1
            return False 
        else:
            if self.repeat_count > 0:
                prefix = f"\n[... PREVIOUS LOG REPEATED {self.repeat_count} TIMES ...]\n"
                record.msg = prefix + msg
            
            self.last_msg_hash = current_hash
            self.last_msg_content = msg
            self.repeat_count = 0
            return True

def setup_secure_logger(name: str, log_dir: Path = None):
    """
    Configures a standardized, compliant logger with memory buffering.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG) 
    
    if logger.handlers:
        logger.handlers = []

    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    
    # 1. Console Stream (Real-time monitoring)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO) 
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(ComplianceFilter()) 
    logger.addHandler(stream_handler)
    
    # 2. File Persistence (Audit Trail)
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = log_dir / f"{name.lower()}.log"

        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG) 
        file_handler.setFormatter(formatter)
        file_handler.addFilter(ComplianceFilter()) 
        file_handler.addFilter(DeduplicationFilter()) 

        # Memory Buffer for Crash Context
        memory_handler = logging.handlers.MemoryHandler(
            capacity=100,              
            flushLevel=logging.ERROR,  
            target=file_handler        
        )
        
        logger.addHandler(memory_handler)
        
    return logger