import React, { useState, useEffect } from 'react';
// IMPORT SÉCURISÉ DE L'ASSET (Vite/React Best Practice)
// Le chemin remonte de components/business vers src/assets
import deviceImg from '../../assets/titanium_device_flatlay.png'; 

export default function FintechAsset({ 
  type = "PROP", 
  amount = "100,000.00", 
  currency = "$",
  partner = "Apex Trader" 
}) {
  const [showNotif, setShowNotif] = useState(false);
  const [animateGraph, setAnimateGraph] = useState(false);

  useEffect(() => {
    // Séquence d'animation psychologique
    setTimeout(() => setAnimateGraph(true), 500); // Le graph démarre vite
    setTimeout(() => setShowNotif(true), 2000);   // La notif arrive après lecture du solde
  }, []);

  // Couleurs dynamiques selon le type d'offre
  const accentColor = type === 'PROP' ? 'bg-blue-600' : 'bg-emerald-500';
  const graphColor = type === 'PROP' ? '#2563eb' : '#10b981';

  return (
    // CONTENEUR PRINCIPAL
    <div className="relative w-[340px] mx-auto group hover:scale-[1.01] transition-transform duration-700">
      
      {/* 1. L'IMAGE DU TÉLÉPHONE (Cadre Statique via Import Module) */}
      <div className="relative z-20 pointer-events-none drop-shadow-2xl">
          <img 
            src={deviceImg} 
            alt="Device Frame" 
            className="w-full h-auto"
          />
      </div>

      {/* 2. L'INTERFACE DYNAMIQUE (Overlay Code) */}
      {/* ⚠️ ZONE DE CALIBRAGE : 
         Modifie les valeurs top/bottom/left/right ci-dessous pour que 
         ce carré blanc tombe PILE POIL sur l'écran blanc de ton image.
      */}
      <div className="absolute top-[2.5%] left-[6%] right-[6%] bottom-[2.5%] z-30 bg-white rounded-[2.5rem] overflow-hidden flex flex-col">
          
          {/* --- CONTENU DE L'ÉCRAN --- */}

          {/* Header (Status Bar fictive) */}
          <div className="pt-6 px-6 flex justify-between items-center opacity-80">
              <div className="text-[10px] font-bold text-slate-900">{new Date().toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})}</div>
              <div className="flex gap-1">
                 <div className="w-3 h-3 bg-slate-900 rounded-full opacity-20"></div>
                 <div className="w-3 h-3 bg-slate-900 rounded-full opacity-20"></div>
                 <div className={`w-3 h-3 rounded-full ${accentColor}`}></div>
              </div>
          </div>

          {/* Solde Massif */}
          <div className="px-6 mt-6">
              <div className="text-slate-400 text-[9px] font-extrabold uppercase tracking-widest mb-1">
                  {type === 'PROP' ? 'Buying Power' : 'Total Assets'}
              </div>
              <div className="flex items-start">
                <span className="text-3xl font-bold text-slate-900 mr-1 mt-1">{currency}</span>
                <span className="text-5xl font-extrabold text-slate-900 tracking-tighter">{amount}</span>
              </div>
              <div className="text-xs text-green-500 font-bold mt-1 flex items-center gap-1">
                 <span>▲ +4.2%</span> <span className="text-slate-300 font-normal">this week</span>
              </div>
          </div>

          {/* Graphique "Revolut Style" */}
          <div className="flex-grow relative mt-8 w-full">
              <svg viewBox="0 0 300 120" className="w-full h-full overflow-visible preserve-3d">
                 <defs>
                    <linearGradient id={`grad-${type}`} x1="0" x2="0" y1="0" y2="1">
                        <stop offset="0%" stopColor={graphColor} stopOpacity="0.1" />
                        <stop offset="100%" stopColor="white" stopOpacity="0" />
                    </linearGradient>
                 </defs>
                 {/* Zone remplie */}
                 <path d="M0,120 L0,40 C 50,40 80,70 120,60 C 180,40 220,10 300,20 V 120 Z" fill={`url(#grad-${type})`} />
                 {/* Ligne animée */}
                 <path 
                   d="M0,40 C 50,40 80,70 120,60 C 180,40 220,10 300,20" 
                   fill="none" 
                   stroke={graphColor}
                   strokeWidth="3"
                   strokeLinecap="round"
                   className={`transition-all duration-[2000ms] ease-out ${animateGraph ? 'stroke-dashoffset-0' : 'stroke-dashoffset-[1000]'}`}
                   style={{strokeDasharray: 1000}}
                 />
              </svg>
          </div>

          {/* NOTIFICATION PUSH (L'élément déclencheur) */}
          <div className={`absolute top-4 left-4 right-4 transition-all duration-700 cubic-bezier(0.34, 1.56, 0.64, 1) ${showNotif ? 'translate-y-0 opacity-100' : '-translate-y-10 opacity-0'}`}>
              <div className="bg-white/95 backdrop-blur-md p-3 rounded-2xl shadow-[0_8px_30px_rgb(0,0,0,0.08)] border border-slate-100 flex items-center gap-3">
                <div className={`w-8 h-8 rounded-full flex-shrink-0 flex items-center justify-center ${accentColor} text-white`}>
                    <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>
                </div>
                <div>
                    <div className="font-bold text-xs text-slate-900 leading-tight">Allocation Ready</div>
                    <div className="text-[10px] text-slate-500 font-medium">{partner} confirmed access.</div>
                </div>
              </div>
          </div>
      </div>
    </div>
  );
}