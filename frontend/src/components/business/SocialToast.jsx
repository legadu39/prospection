import React, { useState, useEffect } from 'react';
import { CheckCircle, ShieldCheck, TrendingUp, Globe } from 'lucide-react';

// INTELLIGENCE : Version "Clean Sheet" - Preuve Sociale Statique & Vérifiable
const SocialToast = ({ currentTab = 'liquidite' }) => {
  const [visible, setVisible] = useState(false);
  const [notification, setNotification] = useState(null);
  const [cursor, setCursor] = useState(0);

  // V3 COMPLIANCE: Données statiques basées sur l'infrastructure réelle.
  // Plus de "Faux Utilisateur X vient de gagner Y".
  const trustEvents = [
    { 
        icon: <ShieldCheck size={16} className="text-white" />,
        title: "Infrastructure Auditée", 
        desc: "Partenaires certifiés.",
        tag: "SECURE"
    },
    { 
        icon: <Globe size={16} className="text-white" />,
        title: "Global Access", 
        desc: "Trading Desk actif 24/7.",
        tag: "LIVE"
    },
    { 
        icon: <TrendingUp size={16} className="text-white" />,
        title: "Performance Monitor", 
        desc: "Latence réseau optimisée.",
        tag: "OPTIMAL"
    }
  ];

  useEffect(() => {
    // Délai initial
    const initialDelay = setTimeout(() => {
      triggerNotification();
    }, 4000);

    const triggerNotification = () => {
      // Rotation cyclique simple
      setCursor(prev => {
          const nextIndex = (prev + 1) % trustEvents.length;
          setNotification(trustEvents[nextIndex]);
          return nextIndex;
      });
      
      setVisible(true);

      // Durée d'affichage
      setTimeout(() => {
        setVisible(false);
      }, 5000);

      // Intervalle fixe et calme (pas d'urgence artificielle)
      setTimeout(triggerNotification, 15000);
    };

    return () => clearTimeout(initialDelay);
  }, []);

  if (!notification) return null;

  return (
    <div 
      className={`fixed bottom-8 left-1/2 -translate-x-1/2 sm:left-auto sm:right-8 sm:translate-x-0 z-50 transition-all duration-700 cubic-bezier(0.175, 0.885, 0.32, 1.275) ${
        visible 
          ? 'translate-y-0 opacity-100 scale-100' 
          : 'translate-y-12 opacity-0 scale-90'
      }`}
    >
      {/* Design "Capsule" : Fond sombre pour contraste max sur fond clair */}
      <div className="bg-slate-900/90 backdrop-blur-xl border border-white/10 text-white pl-3 pr-5 py-3 rounded-full shadow-2xl flex items-center gap-4 min-w-[260px] max-w-[320px] group cursor-default hover:scale-105 transition-transform">
        
        {/* Icone : Style "Token" Gradient */}
        <div className="relative shrink-0">
          <div className="w-10 h-10 rounded-full bg-gradient-to-tr from-brand-blue to-brand-violet flex items-center justify-center shadow-lg shadow-brand-blue/30 border border-white/20">
            {notification.icon}
          </div>
          {/* Badge de succès */}
          <div className="absolute -bottom-1 -right-1 bg-white rounded-full p-0.5 shadow-sm">
             <CheckCircle size={12} className="text-emerald-500" fill="currentColor" fillOpacity={0.2} />
          </div>
        </div>

        <div className="flex flex-col gap-0.5">
          <div className="flex items-baseline justify-between gap-3">
            <span className="font-bold text-sm tracking-tight text-white">{notification.title}</span>
          </div>
          
          <div className="text-xs text-slate-300 font-medium flex items-center gap-1.5">
            <span>{notification.desc}</span>
            <span className="px-1.5 py-0.5 bg-white/10 rounded text-[9px] font-bold text-emerald-400 tracking-wide">
               {notification.tag}
            </span>
          </div>
        </div>

        {/* Effet de brillance au survol */}
        <div className="absolute inset-0 rounded-full bg-gradient-to-r from-transparent via-white/10 to-transparent -translate-x-full group-hover:translate-x-full transition-transform duration-1000 ease-in-out pointer-events-none"></div>

      </div>
    </div>
  );
};

export default SocialToast;