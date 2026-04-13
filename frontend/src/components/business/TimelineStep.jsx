import React from 'react';
import { Check } from 'lucide-react';

const TimelineStep = ({ num, title, text, last = false }) => (
  <div className="flex gap-6 group">
    {/* Colonne de gauche : Indicateur visuel (Tokenized) */}
    <div className="flex flex-col items-center">
      {/* Cercle Numéroté : Style "Bouton physique" avec ombre portée */}
      <div className="relative z-10 w-10 h-10 rounded-full bg-white border border-slate-100 text-slate-900 flex items-center justify-center font-black text-sm shadow-lg shadow-slate-200/50 transition-all duration-300 group-hover:scale-110 group-hover:shadow-xl group-hover:border-brand-blue/30 group-hover:text-brand-blue">
        {/* Effet de profondeur interne */}
        <div className="absolute inset-0 rounded-full bg-gradient-to-b from-transparent to-slate-50 opacity-50 pointer-events-none"></div>
        {num}
      </div>
      
      {/* Ligne connectrice : Gradient discret plutôt que gris plat */}
      {!last && (
        <div className="w-0.5 flex-1 bg-gradient-to-b from-slate-200 via-slate-100 to-transparent my-2 opacity-60 group-hover:opacity-100 group-hover:from-brand-blue/30 transition-all"></div>
      )}
    </div>

    {/* Colonne de droite : Contenu Typographique */}
    <div className="pb-10 pt-1 flex-1">
      <h3 className="font-bold text-slate-900 text-base mb-2 tracking-tight flex items-center gap-2">
        {title}
        {/* Petit indicateur visuel au survol */}
        <Check size={14} className="text-emerald-500 opacity-0 -translate-x-2 group-hover:opacity-100 group-hover:translate-x-0 transition-all duration-300" />
      </h3>
      <p className="text-slate-500 text-sm leading-relaxed font-medium group-hover:text-slate-600 transition-colors">
        {text}
      </p>
    </div>
  </div>
);

export default TimelineStep;