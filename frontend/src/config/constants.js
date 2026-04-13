import { Shield, Smartphone, Zap, Bitcoin, GraduationCap, Clock, HelpCircle, Globe, BarChart3 } from 'lucide-react';

// URL de secours - V3 GLOBAL (Generic Landing Page instead of Bank)
export const BACKUP_OFFER_URL = import.meta.env.VITE_BACKUP_URL || "https://nexus-insights.io/tools";

// Definitions of Strategies (V3: GLOBAL PIVOT - ENGLISH UI)
export const STRATEGIES = {
  TRUST: {
    id: 'TRUST',
    tagline: "Pro Infrastructure",
    title: "Audited Trading Environment.",
    description: "Access institutional tools via our tech partners. Your access is secured by recognized industry leaders.",
    accent: "from-blue-600 to-indigo-600",
    icon: Shield,
    primaryFeature: "Tech Guarantee",
    cta: "View Tools"
  },
  HOW_TO: {
    id: 'HOW_TO',
    tagline: "Training & Capital",
    title: "How to manage $100k capital?",
    description: "Follow this interactive guide to pass your Prop Firm evaluation. 100% digital process, no personal risk.",
    accent: "from-emerald-500 to-teal-600",
    icon: GraduationCap, 
    primaryFeature: "Trader Guide V3",
    cta: "Access Guide"
  },
  DIRECT: {
    id: 'DIRECT',
    tagline: "Priority Access",
    title: "Quota Allocation.",
    description: "Daily allocation slot detected. Activate your professional license before the current cohort fills up.",
    accent: "from-rose-500 to-pink-600",
    icon: Zap,
    primaryFeature: "Instant Allocation",
    cta: "Secure Slot"
  }
};

// --- NEW: BUSINESS INTELLIGENCE (KPIs & THRESHOLDS) ---
export const BUSINESS_KPI = {
  STOCKS: {
    // V3: No more bank stock management, but "firing windows" (Cohorts)
    PROP_FIRM_RESET_HOURS: 24, // Daily reset of challenge allocations
    SAAS_TRIAL_PERIOD: 30,     // Standard TradingView trial duration
    COHORT_CLOSING_LIMIT: 5    // Artificial urgency on promo end
  },
  YIELD: {
    MIN_ROI_PERCENT: 20, // Cuts the channel if margin < 20%
    HIGH_COST_ALERT: 150 // Alert if infra cost exceeds 150€/day
  },
  AUTO_HEALING: {
    MAX_ERROR_RATE: 0.15, // 15% error rate triggers circuit breaker
    PROXY_TIMEOUT_MS: 5000
  },
  COHORTS: {
    STAGNATION_HOURS: 24 // A lead is "dormant" after 24h without interaction
  }
};