import { STRATEGIES } from '../config/constants';

// Helper interne pour vérifier le consentement (évite dépendance circulaire avec analytics.js)
const hasConsent = () => {
    if (typeof window === 'undefined') return false;
    try {
        return sessionStorage.getItem('nexus_consent_given') === 'true';
    } catch (e) { return false; }
};

// --- INTELLIGENCE N°1 : SÉLECTION DE STRATÉGIE (MAB & CONTEXTE) ---
export const selectBestStrategy = () => {
    // FIX HYDRATION: Return default immediately if running on server/build time
    if (typeof window === 'undefined') return 'DIRECT';
    
    // 1. Priorité absolue : Continuité de session (Sticky Strategy)
    try {
        const stickyStrategy = sessionStorage.getItem('nexus_sticky_strategy');
        if (stickyStrategy && STRATEGIES[stickyStrategy]) {
            return stickyStrategy;
        }
    } catch (e) { /* Storage access denied */ }

    const params = new URLSearchParams(window.location.search);
    
    // Forçage manuel
    if (params.get('strategy') === 'trust') return 'TRUST';
    if (params.get('strategy') === 'tuto') return 'HOW_TO';
    if (params.get('strategy') === 'urgency') return 'DIRECT';

    // 2. Détection du Contexte (Device & Source)
    const isMobile = /iPhone|iPad|iPod|Android/i.test(navigator.userAgent);
    const referrer = document.referrer.toLowerCase();
    const isSocialSource = referrer.includes('tiktok') || referrer.includes('instagram') || referrer.includes('facebook');

    // 3. Heuristique UX
    let chosen = 'TRUST'; // Default Desktop

    if (isMobile) {
        chosen = 'DIRECT';
    } else if (isSocialSource) {
        chosen = 'HOW_TO';
    }

    // 4. MAB (Multi-Armed Bandit) - Exploration/Exploitation légère
    // PROTECTED: Ne lit les stats historiques que si consentement donné
    if (hasConsent()) {
        try {
            const statsStr = localStorage.getItem('nexus_strategy_stats');
            if (statsStr) {
                const stats = JSON.parse(statsStr);
                let bestConversion = 0;
                let bestPerformer = null;
                
                ['TRUST', 'HOW_TO', 'DIRECT'].forEach(key => {
                    const s = stats[key];
                    if (s && s.views > 10) {
                        const rate = s.clicks / s.views;
                        if (rate > bestConversion) {
                            bestConversion = rate;
                            bestPerformer = key;
                        }
                    }
                });
                
                if (bestPerformer && bestPerformer !== chosen && bestConversion > 0.15) {
                     chosen = bestPerformer;
                }
            }
        } catch (e) {
            // Fallback silencieux en cas d'erreur de parsing/storage
        }
    }
    
    try {
        sessionStorage.setItem('nexus_sticky_strategy', chosen);
    } catch(e) {}
    
    return chosen;
};

// --- INTELLIGENCE N°2 : HEURISTIQUES SYSTÈME & PERFORMANCE (ECO-MODE) ---
/**
 * Analyse l'environnement matériel pour adapter l'UX (Animations vs Perfs)
 * @returns {Promise<string>} 'PERFORMANCE' | 'BALANCED' | 'ECO'
 */
export const getSystemStatus = async () => {
    if (typeof window === 'undefined') return 'PERFORMANCE';

    let mode = 'PERFORMANCE';
    let urgencyScore = 0;

    // 1. Détection Batterie (API Navigateur)
    try {
        if (navigator.getBattery) {
            const battery = await navigator.getBattery();
            // Si batterie < 20% et pas en charge -> ECO MODE
            if (battery.level < 0.2 && !battery.charging) {
                mode = 'ECO';
                urgencyScore += 50;
            }
        }
    } catch (e) { /* API non supportée */ }

    // 2. Détection Réseau (API Network Information)
    try {
        const conn = navigator.connection;
        if (conn) {
            if (conn.saveData || conn.effectiveType === '2g' || conn.effectiveType === 'slow-2g') {
                mode = 'ECO';
                urgencyScore += 30;
            } else if (conn.effectiveType === '3g') {
                if (mode !== 'ECO') mode = 'BALANCED';
            }
        }
    } catch (e) { /* API non supportée */ }

    // 3. Détection Matériel (Concurrency CPU)
    if (navigator.hardwareConcurrency && navigator.hardwareConcurrency <= 4) {
        if (mode !== 'ECO') mode = 'BALANCED';
    }

    // Sauvegarde du score d'urgence pour l'analyse
    // PROTECTED: Uniquement si consentement
    if (urgencyScore > 0 && hasConsent()) {
        try {
            sessionStorage.setItem('nexus_urgency_score', urgencyScore.toString());
        } catch(e) {}
    }

    return mode;
};

// Legacy Wrapper pour compatibilité existante
export const analyzeUserConstraints = async () => {
    const mode = await getSystemStatus();
    // Si on est en mode ECO critique ou heure tardive, on force DIRECT
    const hour = new Date().getHours();
    const isLate = hour >= 1 && hour <= 5;
    
    if (mode === 'ECO' || isLate) return 'DIRECT';
    return null;
};

// --- INTELLIGENCE N°3 : ANTICIPATION D'INTENTION (PATTERN MATCHING) ---
const PUBLIC_DOMAINS = ['gmail.com', 'outlook.com', 'hotmail.com', 'yahoo.com', 'orange.fr', 'wanadoo.fr', 'icloud.com'];

/**
 * Analyse une entrée texte (email) pour déduire le contexte (B2B vs B2C)
 * @param {string} input - La chaîne à analyser
 * @returns {string|null} - 'BUSINESS' | 'PERSONAL' | null
 */
export const detectUserTypeIntent = (input) => {
    if (!input || !input.includes('@')) return null;

    const parts = input.split('@');
    if (parts.length !== 2) return null;
    
    const domain = parts[1].toLowerCase();
    if (!domain.includes('.')) return null;

    // Si le domaine n'est pas dans la liste des domaines publics connus, c'est probablement une entreprise
    const isPublic = PUBLIC_DOMAINS.some(d => domain === d || domain.endsWith('.' + d));
    
    return isPublic ? 'PERSONAL' : 'BUSINESS';
};

// --- INTELLIGENCE N°4 : PERSISTANCE & TIME-TRAVEL (SNAPSHOTS) ---

/**
 * Sauvegarde un instantané de la session pour reprise ultérieure
 * @param {string} step - L'étape actuelle ('liquidite', 'allocation')
 * @param {object} data - Données contextuelles
 */
export const saveSessionSnapshot = (step, data = {}) => {
    if (typeof window === 'undefined' || !hasConsent()) return;
    try {
        const snapshot = {
            ts: Date.now(),
            step: step,
            data: data,
            scroll: window.scrollY
        };
        localStorage.setItem('nexus_session_snapshot', JSON.stringify(snapshot));
    } catch (e) { /* Storage blocked or quota exceeded */ }
};

/**
 * Tente de restaurer une session précédente ("Warm Resume")
 * @returns {object|null} L'état restauré ou null
 */
export const restoreUserContext = () => {
    if (typeof window === 'undefined' || !hasConsent()) return 'COLD_START';

    try {
        // 1. Analyse Snapshot Time-Travel
        const snapshotStr = localStorage.getItem('nexus_session_snapshot');
        if (snapshotStr) {
            const snapshot = JSON.parse(snapshotStr);
            // Validité du snapshot : 30 minutes
            if (Date.now() - snapshot.ts < 1000 * 60 * 30) {
                return { type: 'WARM_RESUME', data: snapshot };
            }
        }

        // 2. Analyse basique (Legacy)
        const uxEvents = JSON.parse(sessionStorage.getItem('nexus_ux_events') || '{}');
        const events = uxEvents.events || [];
        const hasClicked = events.some(e => e.type === 'CLICK_CTA');
        
        if (hasClicked) return { type: 'WARM_RESUME', data: null };

    } catch (e) {
        console.warn("Context restore failed", e);
    }

    return { type: 'COLD_START', data: null };
};

// --- INTELLIGENCE N°5 : VÉLOCITÉ (SCROLL PHYSICS) ---
export const detectIntentByVelocity = (onIntentDetected) => {
    if (typeof window === 'undefined') return;

    let scrollEvents = [];
    const TRACKING_WINDOW = 3500; 
    const START_TIME = performance.now();
    let hasTriggered = false;

    const listener = () => {
        if (hasTriggered) return;
        const now = performance.now();
        if (now - START_TIME > TRACKING_WINDOW) {
            window.removeEventListener('scroll', listener);
            analyze();
            return;
        }
        scrollEvents.push({ y: window.scrollY, t: now });
    };

    const analyze = () => {
        if (scrollEvents.length < 5) return; 
        const totalDistance = scrollEvents.reduce((acc, curr, i, arr) => {
            if (i === 0) return 0;
            return acc + Math.abs(curr.y - arr[i-1].y);
        }, 0);

        const duration = scrollEvents[scrollEvents.length - 1].t - scrollEvents[0].t;
        const velocity = totalDistance / duration;

        if (velocity > 1.2 && totalDistance > 800) {
            hasTriggered = true;
            if (onIntentDetected) onIntentDetected('DIRECT');
        }
    };

    window.addEventListener('scroll', listener, { passive: true });
    setTimeout(() => {
        window.removeEventListener('scroll', listener);
        if (!hasTriggered) analyze();
    }, TRACKING_WINDOW + 100);
};