import React, { createContext, useContext, useState, useEffect } from 'react';
import mockData from '../data/mock_offers.json';

const OfferContext = createContext();

export const useOffers = () => {
  const context = useContext(OfferContext);
  if (!context) {
    throw new Error('useOffers must be used within an OfferProvider');
  }
  return context;
};

// Helper pour Deep Equal simple
const isEquivalent = (a, b) => JSON.stringify(a) === JSON.stringify(b);

// Helper pour Retry Policy avec Backoff Exponentiel
const fetchWithRetry = async (url, retries = 3, delay = 1000) => {
    try {
        const res = await fetch(url);
        if (!res.ok) throw new Error(`HTTP Error ${res.status}`);
        return await res.json();
    } catch (err) {
        if (retries > 0) {
            await new Promise(r => setTimeout(r, delay));
            return fetchWithRetry(url, retries - 1, delay * 2);
        }
        throw err;
    }
};

export const OfferProvider = ({ children }) => {
  // 1. Initialisation Optimiste (Stale State)
  // On tente de récupérer le cache immédiatement pour un affichage instantané
  const getInitialState = (key, fallback) => {
    try {
      const saved = localStorage.getItem(key);
      return saved ? JSON.parse(saved) : fallback;
    } catch (e) {
      return fallback;
    }
  };

  const [offers, setOffers] = useState(() => getInitialState('nexus_offers_cache', mockData.offers));
  const [marketStatus, setMarketStatus] = useState(() => getInitialState('nexus_market_cache', mockData.market_status));
  
  // Loading est false par défaut si on a du cache (Optimistic UI), true sinon (Cold Start)
  const hasCache = !!localStorage.getItem('nexus_offers_cache');
  const [loading, setLoading] = useState(!hasCache);
  const [networkStatus, setNetworkStatus] = useState('ONLINE'); // 'ONLINE' | 'OFFLINE_MODE'

  // 2. Intelligence & Résilience (Stale-While-Revalidate)
  useEffect(() => {
    let isMounted = true;

    const synchronizeData = async () => {
      try {
        // Stratégie "Network First" avec Retry ou "Cache First" implicite via l'initial state
        const liveData = await fetchWithRetry('/api/offers', 2, 800);
        
        if (!isMounted) return;

        let newDataOffers = mockData.offers;

        if (liveData.status === 'live' || liveData.status === 'static') {
           // LOGIQUE DE FAILOVER INTELLIGENTE (V3 STRATEGY)
           // Agnosticisme: On ne cherche plus de noms spécifiques (Bourso), mais des catégories.
           const rawOffers = liveData.offers || [];
           
           // 1. Trouver la meilleure Prop Firm (High Ticket)
           const propFirms = rawOffers.filter(o => o.type === 'PROP_FIRM' || o.program.includes('apex') || o.program.includes('topstep'));
           const primaryProp = propFirms.sort((a, b) => b.balance_available - a.balance_available)[0];

           // 2. Trouver le meilleur outil SaaS / Crypto (Recurring)
           const saasTools = rawOffers.filter(o => o.type === 'SAAS' || o.type === 'CRYPTO_PSAN' || o.program.includes('tradingview') || o.program.includes('meria'));
           const primarySaaS = saasTools.sort((a, b) => b.balance_available - a.balance_available)[0];

           if (primaryProp) {
               newDataOffers = {
                   // Allocation / Capital (Remplace l'ancienne "Banque")
                   apex_trader: {
                       amount: primaryProp.amount > 1000 ? primaryProp.amount : 50000, // Fallback visuel 50k
                       currency: 'USD', // Standard Prop Firm
                       is_boosted: true,
                       program_id: primaryProp.program,
                       type: 'PROP_FIRM'
                   },
                   // Outils / Yield
                   tradingview: {
                       amount: primarySaaS ? (primarySaaS.amount || 30) : 30,
                       currency: primarySaaS && primarySaaS.type === 'CRYPTO_PSAN' ? 'EUR' : 'USD',
                       is_boosted: false,
                       program_id: primarySaaS ? primarySaaS.program : 'tradingview',
                       type: primarySaaS ? primarySaaS.type : 'SAAS'
                   }
               };
           }
        }

        // Mise à jour intelligente : Seulement si les données ont changé
        setOffers(prevOffers => {
          if (!isEquivalent(prevOffers, newDataOffers)) {
            localStorage.setItem('nexus_offers_cache', JSON.stringify(newDataOffers));
            console.log("⚡ Nexus Data Synced: Updated from Network (V3 Pivot)");
            return newDataOffers;
          }
          return prevOffers;
        });
        
        setNetworkStatus('ONLINE');

      } catch (error) {
        if (!isMounted) return;
        console.warn("Nexus Sync Failed, using cached/fallback data (Resilience Mode).", error);
        setNetworkStatus('OFFLINE_MODE');
      } finally {
        if (isMounted) setLoading(false);
      }
    };

    synchronizeData();

    return () => { isMounted = false; };
  }, []);

  // Calcul du Capital Total Disponbile (Dynamic Yield)
  const totalCapital = Object.values(offers).reduce((acc, offer) => {
      // On additionne seulement le capital Prop Firm pour l'affichage "Power", pas le coût SaaS
      return offer.type === 'PROP_FIRM' ? acc + offer.amount : acc;
  }, 0);
  
  // Détection d'opportunité de marché (Boost)
  const hasHighYield = Object.values(offers).some(offer => offer.is_boosted);

  const value = {
    offers,
    marketStatus,
    totalCapital,
    hasHighYield,
    loading,
    networkStatus
  };

  return (
    <OfferContext.Provider value={value}>
      {children}
    </OfferContext.Provider>
  );
};

export default OfferContext;