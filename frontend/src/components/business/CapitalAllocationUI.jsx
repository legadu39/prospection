import React, { useState, useEffect } from 'react';
import { Wallet, CheckCircle, Lock, ShieldCheck, TrendingUp, Globe, Monitor, BarChart3, PieChart } from 'lucide-react';
import { useOffers } from '../../context/OfferContext';

const CapitalAllocationUI = ({ onTabChange }) => {
  const { offers, loading } = useOffers();
  const [activeTab, setActiveTab] = useState('capital');
  const [step1Done, setStep1Done] = useState(false);
  const [rotation, setRotation] = useState({ x: 0, y: 0 });
  
  // V3 LOGIC: Priority to Global Prop Firms (Apex/Topstep).
  // Data comes from OfferContext which guarantees PROP_FIRM type defaults.
  const primaryOffer = offers?.apex_trader || offers?.topstep || { amount: 50000, currency: 'USD' };
  const secondaryOffer = offers?.tradingview || offers?.ledger || { amount: 60, currency: '%' };
  
  // Propagateur de changement d'onglet vers le parent (App.jsx)
  const handleTabChange = (tab) => {
      setActiveTab(tab);
      if (onTabChange) onTabChange(tab);
  };
  
  useEffect(() => {
    const checkStatus = () => {
      const done = sessionStorage.getItem('nexus_step_1_complete') === 'true';
      setStep1Done(done);
      
      // -- INTELLIGENCE D'AUTOMATISATION --
      if (done && activeTab === 'capital') {
        setTimeout(() => {
            handleTabChange('tools');
        }, 800);
      }
    };
    
    checkStatus();
    const interval = setInterval(checkStatus, 1000);
    return () => clearInterval(interval);
  }, [activeTab]);

  // Effet Parallax / 3D au mouvement de souris
  const handleMouseMove = (e) => {
    const card = e.currentTarget;
    const box = card.getBoundingClientRect();
    const x = e.clientX - box.left;
    const y = e.clientY - box.top;
    
    const centerX = box.width / 2;
    const centerY = box.height / 2;
    
    const rotateX = ((y - centerY) / centerY) * -5; // Max 5 deg
    const rotateY = ((x - centerX) / centerX) * 5;

    setRotation({ x: rotateX, y: rotateY });
  };

  const handleMouseLeave = () => {
    setRotation({ x: 0, y: 0 });
  };

  if (loading) return <div className="animate-pulse h-[340px] bg-white/50 rounded-[2.5rem] w-full mx-auto border border-white"></div>;

  const currentTabIsLocked = activeTab === 'tools' && !step1Done;

  return (
    <div className="relative w-full max-w-[380px] mx-auto perspective-container">
      
      {/* Background Glows (Ambiance Fintech Pro) */}
      <div className="absolute top-10 right-0 w-64 h-64 bg-emerald-500/20 rounded-full blur-[80px] animate-pulse-slow mix-blend-multiply pointer-events-none"></div>
      <div className="absolute -bottom-10 -left-10 w-64 h-64 bg-blue-500/20 rounded-full blur-[80px] animate-float mix-blend-multiply pointer-events-none"></div>

      {/* CARTE 3D PRINCIPALE */}
      <div 
        className="relative bg-white/70 backdrop-blur-2xl rounded-[3rem] p-8 border border-white/60 shadow-3d transition-transform duration-100 ease-out"
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
        style={{
          transform: `rotateX(${rotation.x}deg) rotateY(${rotation.y}deg)`,
          transformStyle: 'preserve-3d'
        }}
      >
        {/* Shine Effect Overlay */}
        <div className="absolute inset-0 rounded-[3rem] bg-gradient-to-tr from-white/40 to-transparent opacity-50 pointer-events-none z-10"></div>

        {/* --- CONTENU DE LA CARTE --- */}
        <div className="relative z-20 transform translate-z-10">
          
          {/* Header Switcher */}
          <div className="flex bg-slate-100/50 p-1.5 rounded-[1.5rem] mb-8 backdrop-blur-sm border border-white/50">
             <button 
               onClick={() => handleTabChange('capital')}
               className={`flex-1 py-3 rounded-[1.2rem] text-xs font-bold transition-all duration-300 flex items-center justify-center gap-2 ${
                 activeTab === 'capital' 
                   ? 'bg-white text-slate-900 shadow-md transform scale-105' 
                   : 'text-slate-400 hover:text-slate-600'
               }`}
             >
               <span>Allocation</span>
               {step1Done && <CheckCircle size={12} className="text-emerald-500" fill="currentColor" fillOpacity={0.2} />}
             </button>
             <button 
               onClick={() => handleTabChange('tools')}
               className={`flex-1 py-3 rounded-[1.2rem] text-xs font-bold transition-all duration-300 flex items-center justify-center gap-2 ${
                 activeTab === 'tools' 
                   ? 'bg-white text-blue-600 shadow-md transform scale-105' 
                   : 'text-slate-400 hover:text-slate-600'
               }`}
             >
               <span>Software</span>
               {!step1Done && <Lock size={12} className="text-slate-400" />}
             </button>
          </div>

          {/* Icon Flottante */}
          <div className="flex justify-between items-start mb-8">
            <div className={`w-14 h-14 rounded-[1.2rem] flex items-center justify-center shadow-lg transition-colors duration-500 animate-float ${
               activeTab === 'capital' ? 'bg-slate-900 text-white shadow-slate-900/20' : 'bg-gradient-to-br from-blue-500 to-indigo-600 text-white shadow-blue-500/30'
            }`}>
              {activeTab === 'capital' ? <BarChart3 size={24} /> : <PieChart size={24} />}
            </div>
            
            <div className={`px-4 py-2 rounded-full text-[10px] font-bold uppercase tracking-wide border transition-colors ${
              activeTab === 'capital'
                ? 'bg-emerald-50 border-emerald-100 text-emerald-700'
                : 'bg-slate-50 border-slate-100 text-slate-400'
            }`}>
               {activeTab === 'capital' ? 'Funded Account' : 'Pro Suite'}
            </div>
          </div>

          {/* Montant (Typography Statement) */}
          <div className={`mb-8 transition-all duration-500 ${currentTabIsLocked ? 'blur-sm opacity-40 grayscale' : ''}`}>
            <p className="text-slate-400 font-bold text-[10px] uppercase tracking-widest mb-1 pl-1">
              {activeTab === 'capital' ? 'Total Buying Power' : 'Discount Value'}
            </p>
            <div className="flex items-baseline gap-1">
              <span className="text-6xl font-black text-slate-900 tracking-tighter">
                 {/* Auto-Format for large numbers (e.g. 50k, 150k) */}
                 {activeTab === 'capital' 
                    ? (primaryOffer.amount >= 1000 ? (primaryOffer.amount / 1000) + 'k' : primaryOffer.amount) 
                    : secondaryOffer.amount}
              </span>
              <span className="text-3xl font-bold text-slate-300">
                {activeTab === 'capital' ? (primaryOffer.currency === 'USD' ? '$' : '€') : (secondaryOffer.currency === '%' ? '%' : '$')}
              </span>
            </div>
          </div>

          {/* Lock Overlay (Si Verrouillé) */}
          {currentTabIsLocked && (
            <div className="absolute inset-0 top-32 flex flex-col items-center justify-center z-30">
              <div className="bg-white/90 border border-white backdrop-blur-xl p-6 rounded-3xl shadow-2xl flex flex-col items-center text-center max-w-[200px] animate-scale-in">
                <div className="w-10 h-10 rounded-full bg-slate-100 flex items-center justify-center mb-3">
                  <Lock size={18} className="text-slate-400" />
                </div>
                <p className="text-[10px] text-slate-500 leading-relaxed font-bold">
                  Validez le Challenge pour débloquer les outils d'analyse.
                </p>
              </div>
            </div>
          )}

          {/* Footer Card */}
          <div className="pt-6 border-t border-slate-100/50 flex items-center gap-3">
             <Globe size={16} className="text-emerald-500"/>
             <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">
               {activeTab === 'capital' 
                 ? 'Global Prop Firm Partner'
                 : 'Nexus Analyst Tools'}
             </span>
          </div>

        </div>
      </div>
    </div>
  );
};

export default CapitalAllocationUI;