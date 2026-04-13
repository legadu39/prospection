// utils/analytics.js - ENGINE V3 (SECURED & COMPLIANT)

/**
 * Vérifie si l'utilisateur a donné son consentement explicite.
 * Basé sur le flag stocké dans sessionStorage après validation des CGU.
 */
const hasConsent = () => {
  if (typeof window === 'undefined') return false;
  try {
      return sessionStorage.getItem('nexus_consent_given') === 'true';
  } catch (e) {
      return false;
  }
};

/**
 * Enregistre une vue de stratégie pour le MAB (Multi-Armed Bandit).
 * Persiste dans localStorage pour l'apprentissage sur le long terme.
 * RESTRICTION: Ne fonctionne que si le consentement est validé.
 */
export const recordView = (strategyId) => {
    if (typeof window === 'undefined' || !hasConsent()) return;
    try {
        const stats = JSON.parse(localStorage.getItem('nexus_strategy_stats') || '{"TRUST":{"views":0,"clicks":0},"HOW_TO":{"views":0,"clicks":0},"DIRECT":{"views":0,"clicks":0}}');
        if (stats[strategyId]) {
            stats[strategyId].views += 1;
            localStorage.setItem('nexus_strategy_stats', JSON.stringify(stats));
        }
    } catch(e) {}
};

/**
 * Enregistre un clic (Conversion Intention) pour le MAB.
 * RESTRICTION: Ne fonctionne que si le consentement est validé.
 */
export const recordClick = (strategyId) => {
    if (typeof window === 'undefined' || !hasConsent()) return;
    try {
        const stats = JSON.parse(localStorage.getItem('nexus_strategy_stats') || '{}');
        if (stats[strategyId]) {
            stats[strategyId].clicks += 1;
            localStorage.setItem('nexus_strategy_stats', JSON.stringify(stats));
        }
        
        // INTELLIGENCE : Ajout au scoring de session
        logInteraction('CLICK_CTA', strategyId);
    } catch(e) {}
};

/**
 * INTELLIGENCE : Système de Journalisation des Micro-Interactions
 * Stocke les événements dans sessionStorage pour calculer le score en temps réel.
 * NOTE: Les événements critiques (Navigation) sont autorisés en mémoire volatile,
 * mais le stockage persistant est conditionné.
 */
export const logInteraction = (type, value = null) => {
    if (typeof window === 'undefined') return;
    
    // Mode "Safe" avant consentement : on ne loggue pas si c'est de la donnée perso/comportementale fine
    // On autorise uniquement les événements système essentiels au fonctionnement immédiat
    const isSystemEvent = ['VIEW_DASHBOARD', 'ERROR'].includes(type);
    if (!hasConsent() && !isSystemEvent) return;

    try {
        const sessionData = JSON.parse(sessionStorage.getItem('nexus_ux_events') || '{"events":[], "maxScroll":0, "startTime":' + Date.now() + '}');
        
        // Évite les doublons pour certains événements uniques
        if (['CLICK_CTA', 'COPY_CODE', 'CLICK_PARTNER', 'CLICK_BANK'].includes(type)) {
            if (sessionData.events.some(e => e.type === type)) return;
        }

        // Mise à jour Max Scroll
        if (type === 'SCROLL_DEPTH') {
            if (value > sessionData.maxScroll) sessionData.maxScroll = value;
        } else {
            sessionData.events.push({ type, value, ts: Date.now() });
        }
        
        sessionStorage.setItem('nexus_ux_events', JSON.stringify(sessionData));
    } catch(e) {}
};

/**
 * INTELLIGENCE : Calculateur de Score d'Intention (0-100)
 * Algorithme pondéré pour déterminer si le lead est "Qualifié".
 * Fonctionne en lecture seule sur les données de session volatiles.
 */
export const calculateIntentScore = () => {
    if (typeof window === 'undefined') return 0;
    
    let score = 0;
    let data;
    
    try {
        data = JSON.parse(sessionStorage.getItem('nexus_ux_events') || '{}');
    } catch(e) { return 0; }
    
    if (!data.startTime) return 0;

    // 1. CHRONOMÉTRIE (Temps de Lecture)
    const dwellTime = (Date.now() - data.startTime) / 1000; // secondes
    
    if (dwellTime > 5) score += 5;   // A vu la page
    if (dwellTime > 15) score += 15; // Lecture minimale
    if (dwellTime > 45) score += 20; // Lecture approfondie (Intérêt réel)
    
    // 2. PROFONDEUR DE LECTURE (Scroll)
    // A scrollé plus de 50% de la page (cherche des infos)
    if (data.maxScroll > 50) score += 10;
    if (data.maxScroll > 80) score += 10; // A vu le footer (Mentions légales/Confiance)

    // 3. INTERACTIONS FORTES
    const events = data.events || [];
    
    // A copié le code (Indicateur très fort d'intention)
    if (events.some(e => e.type === 'COPY_CODE')) score += 30;
    
    // A cliqué sur le lien partenaire
    if (events.some(e => e.type === 'CLICK_CTA')) score += 20;
    
    // A fait un "Boomerang" (Revenu sur l'onglet après être parti)
    // C'est le signe qu'il cherche le code pour finaliser
    if (events.some(e => e.type === 'BOOMERANG_RETURN')) score += 40;

    // 4. PÉNALITÉS (Anti-Bot)
    // Trop rapide pour être humain (< 2s et clic)
    if (dwellTime < 2 && events.some(e => e.type === 'CLICK_CTA')) {
        return 0; 
    }

    return Math.min(100, score);
};

/**
 * INTELLIGENCE : Seuil de Qualification
 * Détermine si le crédit SaaS doit être consommé.
 */
export const isQualifiedLead = () => {
    // Seuil fixé à 70 points pour garantir la qualité
    return calculateIntentScore() >= 70;
};