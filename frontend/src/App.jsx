import React, { useState, useEffect, useRef } from 'react';
import CapitalAllocationUI from './components/business/CapitalAllocationUI';
import SocialToast from './components/business/SocialToast';
import IntelCard from './components/business/IntelCard';
import FintechAsset from './components/business/FintechAsset';
import { OfferProvider, useOffers } from './context/OfferContext';
import { ArrowRight, CheckCircle, ExternalLink, ShieldCheck, Zap, AlertTriangle, X, Info, Lock, WifiOff, PlayCircle, BookOpen, Globe, BrainCircuit } from 'lucide-react';

// INTELLIGENCE & ANALYTICS
import { selectBestStrategy, restoreUserContext, getSystemStatus, saveSessionSnapshot } from './utils/intelligence';
import { logInteraction } from './utils/analytics';

// -- TEXTE & TRADUCTION (SMART CONTENT SWITCHING - CONFORMITÉ V3) --
const I18N = {
  US: {
    hero: {
      title: "Get Funded",
      sub: "Trade Capital",
      desc: "Get access to professional trading capital and our Global Trading Guide."
    },
    modal: {
      title: "Capital Access &",
      sub: "Pro Training",
      cta: "Get Guide & Activate Offer",
      alloc: "Trading Capital",
      alloc_sub: "Evaluation to manage professional funds (Prop Firm).",
      guide: "Global Trader Guide (Included)",
      guide_sub: "The complete method to pass your evaluation risk-free."
    },
    btn: {
      status: "Account Validated",
      step1: "Step 1 • Registration",
      secure: "Secure",
      discover: "Discover Prop Trading"
    },
    intent: {
      wait: "Wait!",
      text: "The Trader Guide and discount are limited to daily quotas.",
      cta_yes: "Get the Guide Now",
      cta_no: "No thanks, I refuse help"
    },
    legal: {
      warn: "Risk Warning: Trading involves high risk. This is not financial advice.",
      cpr: "© 2026 Nexus Systems Global. B2B Media Agency."
    }
  },
  EU: {
    hero: {
      title: "Sécurisez",
      sub: "vos Actifs",
      desc: "Accédez aux meilleures infrastructures régulées et notre Guide de Gestion."
    },
    modal: {
      title: "Infrastructure &",
      sub: "Sécurité Actifs",
      cta: "Télécharger Guide & Voir Partenaires",
      alloc: "Allocation",
      alloc_sub: "Accès aux partenaires PSAN (France) pour vos actifs.",
      guide: "Guide Utilisateur (Inclus)",
      guide_sub: "Optimisation et sécurisation ledger."
    },
    btn: {
      status: "Compte Validé",
      step1: "Étape 1 • Inscription",
      secure: "Sécuriser",
      discover: "Diversifier en Crypto"
    },
    intent: {
      wait: "Attendez !",
      text: "L'accès au Guide et aux offres partenaires est soumis aux quotas journaliers.",
      cta_yes: "Recevoir le Guide maintenant",
      cta_no: "Non merci, je refuse l'aide"
    },
    legal: {
      warn: "Avertissement : Ce site ne fournit pas de conseil en investissement financier. Risque de perte en capital.",
      cpr: "© 2026 Nexus Systems Global. Ce site ne fournit pas de conseil en investissement financier."
    }
  }
};

// -- COMPOSANT : BRIEFING MODAL (Nexus V3 Style - Agence Média Edition) --
const BriefingModal = ({ isOpen, onClose, onConfirm, type, amount, mainOfferKey, geoZone, offers }) => {
  if (!isOpen) return null;

  // Logic V3: Use Backend Truth for Currency (Dynamic)
  const currentOffer = offers?.[type];
  const currencySymbol = currentOffer?.currency === 'USD' ? '$' : '€';
  
  // Selection Langue
  const t = I18N[geoZone] || I18N.US;

  return (
    <div className="fixed inset-0 z-50 flex items-end sm:items-center justify-center sm:p-4 bg-slate-900/60 backdrop-blur-md animate-fade-in">
      <div className="bg-white w-full max-w-md rounded-t-[2.5rem] sm:rounded-[2.5rem] overflow-hidden shadow-2xl animate-fade-in-up">
        
        {/* Header Image / Abstract */}
        <div className="h-32 bg-gradient-to-br from-slate-50 to-slate-100 relative overflow-hidden flex items-center justify-center">
             <div className="absolute inset-0 bg-noise opacity-30"></div>
             <div className="w-24 h-24 bg-blue-500/10 rounded-full blur-2xl absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2"></div>
             <ShieldCheck size={48} className="text-slate-900 relative z-10 opacity-80" strokeWidth={1.5} />
             <button onClick={onClose} className="absolute top-6 right-6 p-2 bg-white/50 hover:bg-white rounded-full transition-all">
                <X size={20} className="text-slate-500" />
             </button>
        </div>
        
        <div className="p-8 space-y-6">
          <div className="space-y-2 text-center sm:text-left">
             <span className="inline-block px-3 py-1 bg-blue-50 text-blue-700 rounded-full text-[10px] font-bold uppercase tracking-widest mb-2">
                Offre Partenaire V3.0
             </span>
             <h3 className="text-2xl font-black text-slate-900 tracking-tight leading-none">
                {t.modal.title}<br/>
                <span className="text-transparent bg-clip-text bg-gradient-to-r from-blue-600 to-violet-600">
                  {t.modal.sub}
                </span>
             </h3>
          </div>
          
          <div className="bg-slate-50 rounded-2xl p-5 space-y-4 border border-slate-100">
            <div className="flex gap-4 items-start">
              <div className="mt-1 min-w-[20px]"><CheckCircle size={20} className="text-emerald-500" fill="white" /></div>
              <div>
                 <p className="text-sm font-bold text-slate-900">
                   {t.modal.alloc} ({amount}{currencySymbol})
                 </p>
                 <p className="text-xs text-slate-500 mt-0.5">
                   {t.modal.alloc_sub}
                 </p>
              </div>
            </div>
            
            <div className="flex gap-4 items-start">
                <div className="mt-1 min-w-[20px]"><BookOpen size={20} className="text-blue-500" fill="white" /></div>
                 <div>
                   <p className="text-sm font-bold text-slate-900">{t.modal.guide}</p>
                   <p className="text-xs text-slate-500 mt-0.5">{t.modal.guide_sub}</p>
                </div>
            </div>
          </div>

          <button 
            onClick={onConfirm}
            className="w-full bg-slate-900 hover:bg-blue-600 text-white font-bold py-5 rounded-2xl text-base transition-all active:scale-[0.98] shadow-lg shadow-slate-200 flex items-center justify-center gap-3 group"
          >
            <span>{t.modal.cta}</span>
            <ArrowRight size={18} className="group-hover:translate-x-1 transition-transform" />
          </button>
          
          <div className="text-center space-y-2">
             <p className="text-[10px] text-slate-400 font-medium">
                ID Session: {Math.random().toString(36).substr(2, 9).toUpperCase()}
             </p>
             <p className="text-[9px] text-slate-300 leading-tight">
                ⚠️ {t.legal.warn}
             </p>
          </div>
        </div>
      </div>
    </div>
  );
};

// -- COMPOSANT : EXIT INTENT MODAL --
const ExitIntentModal = ({ isOpen, onClose, geoZone }) => {
  if (!isOpen) return null;
  const t = I18N[geoZone] || I18N.US;

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center p-4 bg-slate-900/60 backdrop-blur-md animate-fade-in">
      <div className="bg-white w-full max-w-xs rounded-3xl p-8 text-center shadow-2xl relative overflow-hidden animate-scale-in">
        <div className="w-16 h-16 bg-red-50 rounded-full flex items-center justify-center mx-auto mb-6 animate-pulse-slow">
             <AlertTriangle className="text-red-500" size={32} strokeWidth={1.5} />
        </div>
        <h3 className="text-2xl font-black text-slate-900 mb-2 tracking-tight">{t.intent.wait}</h3>
        <p className="text-slate-500 text-sm mb-8 leading-relaxed font-medium">
          {t.intent.text}
        </p>
        <div className="flex flex-col gap-3">
          <button onClick={onClose} className="w-full bg-slate-900 text-white font-bold py-4 rounded-2xl text-sm shadow-xl hover:bg-black transition-colors">
            {t.intent.cta_yes}
          </button>
          <button onClick={onClose} className="text-slate-400 text-xs font-bold hover:text-slate-900 transition-colors uppercase tracking-wide">
            {t.intent.cta_no}
          </button>
        </div>
      </div>
    </div>
  );
};

// -- HELPER: SMART GEO-ROUTING (HYBRID CLIENT/SERVER) --
const fetchGeoContext = async () => {
    try {
        const res = await fetch('/api/geo-check');
        if (res.ok) {
            const data = await res.json();
            return data.zone; // 'US' or 'EU' (Server Truth)
        }
    } catch (e) {
        console.warn("Geo-Check API Offline, falling back to Client Heuristics.");
    }
    // Fallback Local (Moins précis mais vital si API down)
    const userLocale = navigator.language || 'en-US';
    const isZoneEuro = ['fr', 'fr-FR', 'fr-BE', 'de', 'it', 'es', 'be'].some(lang => userLocale.includes(lang));
    return isZoneEuro ? 'EU' : 'US';
};

// -- MAIN COMPONENT LOGIC (NOT EXPORTED DEFAULT YET) --
const NexusDashboard = () => {
  const { offers, totalCapital, hasHighYield, loading, networkStatus } = useOffers();
  const [step1Complete, setStep1Complete] = useState(false);
  const [modalConfig, setModalConfig] = useState({ isOpen: false, type: null, amount: 0 });
  const [exitIntentOpen, setExitIntentOpen] = useState(false);
  const [trackingParams, setTrackingParams] = useState({ source: 'direct', ref: 'none' });
  const [showComplianceBanner, setShowComplianceBanner] = useState(false);
  const [isRedirecting, setIsRedirecting] = useState(false);
  
  // UX Intelligence
  const [performanceMode, setPerformanceMode] = useState('PERFORMANCE');
  
  // Conformité RGPD & V3
  const [isTermsAccepted, setIsTermsAccepted] = useState(false);
  
  // Intelligence : Token & State
  const [preflightToken, setPreflightToken] = useState(null);
  const preflightRequested = useRef(false);
  const exitIntentTriggered = useRef(false);

  const [currentActiveTab, setCurrentActiveTab] = useState('capital'); 

  // -- SMART GEO-ROUTING LOGIC V3 --
  const [mainOfferKey, setMainOfferKey] = useState('apex_trader');
  const [geoZone, setGeoZone] = useState('US');

  // CONSENT MANAGEMENT (GDPR)
  useEffect(() => {
    if (isTermsAccepted) {
      // On stocke le consentement explicite
      sessionStorage.setItem('nexus_consent_given', 'true');
    }
  }, [isTermsAccepted]);

  useEffect(() => {
      // Async Geo Check (Source of Truth)
      fetchGeoContext().then(fetchedZone => {
          let finalZone = fetchedZone;

          // --- AJOUT "DEV MODE" (POUR TESTS LOCAUX) ---
          // Si on est en local (localhost) ET qu'on demande une zone spécifique
          if (import.meta.env.DEV) {
             const params = new URLSearchParams(window.location.search);
             if (params.get('zone')) {
                 console.log("👨‍💻 DEV MODE: Forcing Zone to", params.get('zone'));
                 finalZone = params.get('zone');
             }
          }
          // ---------------------------------------------

          setGeoZone(finalZone);
          
          // SECURITY: Hard-lock de la zone. On ne permet pas le changement via URL ici.
          if (finalZone === 'EU') {
              setMainOfferKey('meria'); // Pivot Stratégique V3 FR (PSAN)
          } else {
              setMainOfferKey('apex_trader'); // Global Strategy
          }
      });
  }, []);

  const t = I18N[geoZone] || I18N.US;
  const mainOffer = offers?.[mainOfferKey] || { amount: 50000, currency: 'USD' };

  // --- 1. BOOTSTRAP INTELLIGENT ---
  useEffect(() => {
    const strategy = selectBestStrategy();
    getSystemStatus().then(mode => {
        setPerformanceMode(mode);
    });

    const restore = restoreUserContext();
    if (restore.type === 'WARM_RESUME') {
        const hasDoneStep1 = sessionStorage.getItem('nexus_step_1_complete') === 'true';
        if (hasDoneStep1 || restore.data?.step === 'allocation') {
            setStep1Complete(true);
            setCurrentActiveTab('tools'); 
        }
    }

    const searchParams = new URLSearchParams(window.location.search);
    // SANITIZATION: Basic input cleaning
    const rawSource = searchParams.get('source') || 'direct';
    const source = rawSource.replace(/[^a-zA-Z0-9-_]/g, ''); // alphanumeric strict
    const ref = (searchParams.get('ref') || 'none').replace(/[^a-zA-Z0-9-_]/g, '');
    
    setTrackingParams({ source, ref });

    // Activation bannière compliance stricte pour traffic social
    if (['tiktok', 'instagram', 'facebook', 'snapchat'].some(s => source.includes(s))) {
      setShowComplianceBanner(true);
    }
    
    if (!sessionStorage.getItem('nexus_view_logged')) {
      logInteraction('VIEW_DASHBOARD', strategy);
      sessionStorage.setItem('nexus_view_logged', 'true');
    }

    const handleMouseLeave = (e) => {
      if (e.clientY < 10 && !exitIntentTriggered.current && !step1Complete && !modalConfig.isOpen) {
        exitIntentTriggered.current = true;
        setExitIntentOpen(true);
        logInteraction('EXIT_INTENT_TRIGGERED', 'desktop');
      }
    };
    
    document.addEventListener('mouseleave', handleMouseLeave);
    return () => document.removeEventListener('mouseleave', handleMouseLeave);
  }, [modalConfig.isOpen, step1Complete]);

  // --- 2. INTELLIGENCE INTERACTIVE ---

  const handleTabSwitch = (tab) => {
      setCurrentActiveTab(tab);
      saveSessionSnapshot(tab, { step1Complete });
  };

  const handleHoverIntent = () => {
      if (preflightRequested.current || step1Complete) return;
      preflightRequested.current = true;
      
      const programId = mainOffer.program_id || mainOfferKey;
      fetch(`/warmup/nexus_web_app/${programId}`)
        .then(res => res.json())
        .then(data => {
            if (data.status === 'ready') setPreflightToken(data.token);
        })
        .catch(e => console.warn("Warmup silent fail", e));
  };

  const handleOpenProtocol = (type) => {
    if (!isTermsAccepted && type !== 'info') {
        return; 
    }
    
    const offer = offers[type];
    
    // AUDIT SECURITY: HARD BLOCK EU PROP FIRMS
    // Empêche techniquement l'ouverture d'une offre Prop Firm si Geo = EU
    if (geoZone === 'EU' && offer?.type === 'PROP_FIRM') {
        console.warn("Security Block: Prop Firms are disabled in EU Zone.");
        // Redirection forcée vers l'offre conforme
        handleOpenProtocol('meria');
        return;
    }
    
    if (!offer && type !== 'info') return;
    
    logInteraction('INTENT_CTA', type);
    setModalConfig({ 
      isOpen: true, 
      type: type, 
      amount: offer?.amount || 0,
      mainOfferKey: mainOfferKey 
    });
  };

  const handleConfirmAction = () => {
    const { type } = modalConfig;
    
    setIsRedirecting(true); // UI Feedback
    
    // AUDIT FIX: VALUE FIRST
    const fakeDownload = document.createElement('a');
    fakeDownload.href = "#"; // Placeholder
    fakeDownload.download = "Nexus_Trader_Guide_V3.pdf";
    
    // 2. Logique V3: Prop Firm ou SaaS active le Step 1
    // SECURITY: On ne trust plus aveuglément cette étape côté client, 
    // mais pour l'UI on le maintient. La vraie validation se fera post-callback backend.
    if (type === mainOfferKey || type === 'topstep' || offers[type]?.type === 'PROP_FIRM' || type === 'meria') {
      sessionStorage.setItem('nexus_step_1_complete', 'true');
      setStep1Complete(true);
      setCurrentActiveTab('tools'); // Auto-switch to tools after funding
      saveSessionSnapshot('allocation', { step1Complete: true });
    }
    
    logInteraction('CONVERSION_CLICK', type);
    
    // 3. Redirection Retardée
    setTimeout(() => {
        setModalConfig({ ...modalConfig, isOpen: false });
        let redirectUrl = `/click/nexus_web_app/${type}_offer?source=${trackingParams.source}&ref=${trackingParams.ref}`;
        if (preflightToken && type === mainOfferKey) {
            redirectUrl += `&t=${preflightToken}`;
        }
        window.location.href = redirectUrl;
    }, 1500); 
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-slate-50 flex flex-col items-center justify-center gap-8">
        <div className="relative">
           <div className="w-20 h-20 border-4 border-slate-200 border-t-blue-600 rounded-full animate-spin"></div>
        </div>
        <div className="font-sans font-bold text-xs text-slate-400 tracking-widest uppercase animate-pulse">
          Initialisation Nexus v3.0 Global
        </div>
      </div>
    );
  }

  const bgClass = performanceMode === 'ECO' ? 'bg-slate-50' : 'aurora-bg';

  return (
    <div className={`min-h-screen ${bgClass} text-slate-900 font-sans overflow-x-hidden flex flex-col items-center transition-colors duration-500`}>
      
      <nav className="fixed top-6 z-40 w-full max-w-[90%] sm:max-w-md mx-auto">
        <div className="bg-white/80 backdrop-blur-xl border border-white/40 shadow-glass rounded-full px-5 py-3 flex justify-between items-center transition-all duration-500 hover:shadow-lg">
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 bg-slate-900 rounded-full flex items-center justify-center text-white font-bold text-sm shadow-lg shadow-slate-900/20">N</div>
              <span className="font-bold text-sm tracking-tight text-slate-900">Nexus Systems</span>
            </div>
            <div className="flex items-center gap-2">
              {networkStatus === 'OFFLINE_MODE' && (
                 <WifiOff size={14} className="text-slate-400 animate-pulse" />
              )}
              {performanceMode === 'ECO' && (
                 <div className="w-2 h-2 rounded-full bg-emerald-400" title="Mode Économie d'Énergie"></div>
              )}
              {hasHighYield && geoZone !== 'EU' && (
                <div className="px-3 py-1 bg-red-50 text-red-600 border border-red-100 rounded-full text-[10px] font-bold uppercase tracking-wide flex items-center gap-1.5 animate-pulse-slow">
                  <Zap size={10} fill="currentColor" />
                  <span>Funding Live</span>
                </div>
              )}
              {geoZone === 'EU' && (
                <div className="px-3 py-1 bg-blue-50 text-blue-600 border border-blue-100 rounded-full text-[10px] font-bold uppercase tracking-wide flex items-center gap-1.5">
                  <Globe size={10} />
                  <span>EU Zone</span>
                </div>
              )}
            </div>
        </div>
      </nav>

      {/* COMPLIANCE BANNER - V3 STRICT REQUIREMENTS */}
      {showComplianceBanner && (
        <div className="mt-24 mb-[-2rem] w-full max-w-md px-6 animate-fade-in">
          <div className="bg-slate-100/80 backdrop-blur border border-slate-200 rounded-xl py-2 px-4 text-center">
            <p className="text-[9px] uppercase tracking-widest text-slate-500 font-bold flex items-center justify-center gap-2">
              <Info size={12} />
              {geoZone === 'EU' ? "Lien Commercial • Pas de Conseil Financier" : "Sponsored Link • Not Financial Advice"}
            </p>
          </div>
        </div>
      )}

      <BriefingModal 
        isOpen={modalConfig.isOpen}
        onClose={() => !isRedirecting && setModalConfig({ ...modalConfig, isOpen: false })}
        onConfirm={handleConfirmAction}
        type={modalConfig.type}
        amount={modalConfig.amount}
        mainOfferKey={mainOfferKey}
        geoZone={geoZone}
        offers={offers}
      />

      <ExitIntentModal 
        isOpen={exitIntentOpen}
        onClose={() => setExitIntentOpen(false)}
        geoZone={geoZone}
      />

      {/* UI FEEDBACK REDIRECTION */}
      {isRedirecting && (
        <div className="fixed inset-0 z-[70] bg-white/90 backdrop-blur-md flex flex-col items-center justify-center animate-fade-in">
            <div className="w-16 h-16 border-4 border-blue-600 border-t-transparent rounded-full animate-spin mb-4"></div>
            <h3 className="text-xl font-bold text-slate-900">Envoi du Guide...</h3>
            <p className="text-sm text-slate-500">Redirection vers le partenaire sécurisé.</p>
        </div>
      )}

      <main className="flex-1 flex flex-col items-center w-full max-w-[480px] px-6 pt-32 pb-12 relative z-10">
        
        <div className="text-center mb-12 animate-fade-in-up perspective-container">
          {geoZone === 'EU' ? (
             <h1 className="text-5xl sm:text-6xl font-black mb-6 tracking-tighter text-slate-900 leading-[0.95] card-3d">
                {t.hero.title}<br/>
                <span className="text-transparent bg-clip-text bg-gradient-to-tr from-emerald-600 via-teal-500 to-cyan-600">
                  {t.hero.sub}
                </span>
             </h1>
          ) : (
             <h1 className="text-5xl sm:text-6xl font-black mb-6 tracking-tighter text-slate-900 leading-[0.95] card-3d">
                {t.hero.title}<br/>
                <span className="text-transparent bg-clip-text bg-gradient-to-tr from-blue-600 via-indigo-500 to-violet-600">
                  {t.hero.sub}
                </span>
             </h1>
          )}
          
          <p className="text-slate-500 text-lg font-medium leading-relaxed max-w-[320px] mx-auto tracking-tight">
             {t.hero.desc}
          </p>
        </div>

        {/* --- SECTION D'ALLOCATION DE CAPITAL --- */}
        
        {/* IMPLEMENTATION DE FintechAsset (Zone US Uniquement) */}
        {geoZone !== 'EU' && (
           <div className="mb-4 scale-95 origin-bottom animate-fade-in-up" style={{ animationDelay: '0.05s' }}>
              <FintechAsset 
                 type="PROP" 
                 amount={mainOffer.amount?.toLocaleString('en-US') + '.00'} // Formatage dynamique (ex: 50,000.00)
                 currency={mainOffer.currency === 'USD' ? '$' : '€'} 
                 partner={mainOfferKey === 'apex_trader' ? 'Apex Trader' : 'Topstep'}
              />
           </div>
        )}

        {/* UI Standard (Carte 3D avec Onglets) */}
        <div className="w-full mb-10 perspective-container z-20" style={{ animationDelay: '0.1s' }}>
          <CapitalAllocationUI onTabChange={handleTabSwitch} defaultTab={currentActiveTab} />
        </div>

        <div className="w-full space-y-4 animate-fade-in-up" style={{ animationDelay: '0.2s' }}>
          
          <div className="bg-white/60 backdrop-blur-md border border-white/60 p-4 rounded-2xl shadow-sm mb-6 transition-all duration-300 hover:bg-white/80">
            <label className="flex items-start gap-4 cursor-pointer group select-none">
              <div className="relative flex items-center mt-0.5">
                <input 
                  type="checkbox" 
                  checked={isTermsAccepted}
                  onChange={(e) => setIsTermsAccepted(e.target.checked)}
                  className="peer sr-only"
                />
                <div className={`w-12 h-7 rounded-full transition-colors duration-300 ${isTermsAccepted ? 'bg-blue-600' : 'bg-slate-200'}`}></div>
                <div className={`absolute left-1 top-1 w-5 h-5 bg-white rounded-full shadow-md transition-transform duration-300 ${isTermsAccepted ? 'translate-x-5' : 'translate-x-0'}`}></div>
              </div>
              <span className="text-xs text-slate-500 font-medium leading-tight group-hover:text-slate-800 transition-colors">
                {geoZone === 'EU' ? 
                  "Je confirme être un utilisateur averti et accepte les conditions." : 
                  "I confirm I am an active trader and accept the terms of access."}
              </span>
            </label>
          </div>

          <button 
            onMouseEnter={() => setTimeout(handleHoverIntent, 150)}
            onClick={() => handleOpenProtocol(mainOfferKey)}
            disabled={step1Complete || !isTermsAccepted}
            className={`w-full group h-20 rounded-[2rem] font-bold text-lg flex items-center justify-center px-8 transition-all duration-300 transform border
              ${step1Complete 
                ? 'bg-emerald-50 border-emerald-100 text-emerald-700 cursor-default' 
                : !isTermsAccepted 
                  ? 'bg-slate-100 border-slate-200 text-slate-300 cursor-not-allowed'
                  : 'bg-slate-900 border-slate-900 text-white shadow-xl shadow-blue-900/20 hover:scale-[1.02] active:scale-[0.98]'
              }`}
          >
            <div className="flex flex-col items-center text-center w-full">
              {step1Complete ? (
                 <>
                   <span className="text-xs uppercase tracking-widest opacity-80">Status</span>
                   <div className="flex items-center gap-2">
                       <span>{t.btn.status}</span>
                       <CheckCircle size={20} />
                   </div>
                 </>
              ) : (
                 <div className="flex items-center justify-between w-full">
                   <div className="flex flex-col items-start">
                       <span className="text-xs uppercase tracking-widest opacity-60 font-medium">{t.btn.step1}</span>
                       <span>{t.btn.secure} {geoZone === 'EU' ? `${offers?.meria?.amount}€` : `${mainOffer.amount}$`}</span>
                   </div>
                   <div className={`w-10 h-10 rounded-full flex items-center justify-center transition-colors bg-white/10 text-white`}>
                       <ArrowRight size={20} className="group-hover:translate-x-1 transition-transform" />
                   </div>
                 </div>
              )}
            </div>
          </button>

          <div className={`transition-all duration-500 overflow-hidden ${step1Complete && isTermsAccepted ? 'max-h-24 opacity-100' : 'max-h-0 opacity-0'}`}>
            <button 
              onClick={() => handleOpenProtocol(geoZone === 'EU' ? 'apex_trader' : 'meria')}
              className="w-full h-16 rounded-[2rem] bg-white border border-slate-200 text-slate-900 font-bold text-sm flex items-center justify-center gap-2 shadow-soft hover:shadow-lg hover:border-blue-200 transition-all active:scale-[0.98]"
            >
               <span>{t.btn.discover}</span>
               <ExternalLink size={16} className="text-blue-600" />
            </button>
          </div>
          
          {!step1Complete && (
            <div className="h-16 rounded-[2rem] border-2 border-dashed border-slate-200 flex items-center justify-center gap-2 text-slate-400 text-sm font-medium select-none">
              <Lock size={14} />
              <span>{geoZone === 'EU' ? "Outils en attente..." : "Pro Tools Locked..."}</span>
            </div>
          )}

        </div>

        {/* --- SECTION: NEXUS INTELLIGENCE (CONTENU ÉTENDU V3) --- */}
        <div className="w-full mt-12 animate-fade-in space-y-4" style={{ animationDelay: '0.3s' }}>
            <div className="flex items-center gap-3 px-2 mb-2 opacity-80">
                <div className="h-1 w-8 bg-blue-500 rounded-full"></div>
                <h2 className="text-xs font-bold text-slate-500 uppercase tracking-widest flex items-center gap-2">
                  <PlayCircle size={12} />
                  Intelligence
                </h2>
            </div>

            {/* CARD 1: STRATÉGIE PROP FIRM (MASQUÉE EN EU VIA CONDITION) */}
            {geoZone !== 'EU' && (
                <IntelCard 
                    title="Tutorial: Pass your Prop Firm Eval"
                    summary="How to get a funded account without risking your own capital."
                    readTime="Video Guide"
                    videoSrc="https://www.youtube.com/embed/dQw4w9WgXcQ" 
                    actionLabel={`Start Challenge`}
                    onAction={() => {
                      setIsTermsAccepted(true); 
                      handleOpenProtocol('apex_trader');
                    }}
                    isLocked={step1Complete}
                    fullContent={`
                      <ul>
                        <li>✅ <strong>Leverage:</strong> Trade with $50k+ by paying a small eval fee.</li>
                        <li>✅ <strong>Risk:</strong> Capped at the fee cost. No personal savings lost.</li>
                        <li>✅ <strong>Method:</strong> Strict risk management is key.</li>
                      </ul>
                    `}
                />
            )}

            {/* CARD 2: PSYCHOLOGIE TRADER (NOUVEAU) */}
            <IntelCard 
                title={geoZone === 'EU' ? "Psychologie : FOMO & Risques" : "Psychology: Beating Tilt"}
                summary={geoZone === 'EU' ? "Évitez les décisions émotionnelles et les pertes de capital." : "90% of trading success is mental. Avoid Revenge Trading."}
                readTime="Expert Article"
                videoSrc={null}
                actionLabel="Read"
                onAction={() => null} 
                isLocked={false}
                icon={<BrainCircuit size={20} className="text-violet-500" />}
                fullContent={`
                  <p>Tilt is the enemy. Prop firms punish lack of discipline via Daily Drawdown rules.</p>
                `}
            />
            
            {/* CARD 5: STRATÉGIE CRYPTO (PRIORITAIRE EU) */}
            <IntelCard 
                title={geoZone === 'EU' ? "Sécurité Crypto & Staking (PSAN)" : "Crypto Security (Global)"}
                summary={geoZone === 'EU' ? "Comprendre la régulation PSAN et la sécurisation des actifs." : "Secure your assets properly."}
                readTime="Expert Article"
                videoSrc="https://www.youtube.com/embed/dQw4w9WgXcQ"
                actionLabel={`View Meria Offer`}
                onAction={(...args) => {
                    setIsTermsAccepted(true);
                    handleOpenProtocol('meria');
                }}
                isLocked={step1Complete}
                fullContent={`
                  <p>In the Euro Zone, PSAN regulation is mandatory.</p>
                  <p>Never leave assets on unregulated exchanges. Use Ledger or Meria.</p>
                `}
            />
        </div>

        {/* FOOTER LÉGAL - V3 STRICT COMPLIANCE */}
        <footer className="mt-16 mb-6 px-4 text-center">
             <div className="w-8 h-1 bg-slate-200 rounded-full mx-auto mb-6"></div>
             <p className="text-[10px] text-slate-400 leading-relaxed max-w-sm mx-auto">
               <strong>{geoZone === 'EU' ? "Avertissement" : "Risk Warning"}:</strong> {t.legal.warn}
             </p>
             <p className="text-[10px] text-slate-500 mt-4 font-bold">
               {t.legal.cpr}
             </p>
        </footer>

      </main>
      
      <SocialToast />
    </div>
  );
};

// -- ROOT COMPONENT --
export default function App() {
  return (
    <OfferProvider>
      <NexusDashboard />
    </OfferProvider>
  );
}