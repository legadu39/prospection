import React from 'react';

/**
 * GlassCard - Version "Neo-Bank"
 * Adaptée pour le fond clair. Utilise des bordures subtiles et une haute transparence.
 */
const GlassCard = ({ children, className = "" }) => {
  return (
    <div 
      className={`
        glass-card 
        relative overflow-hidden 
        transition-all duration-300 hover:shadow-[0_20px_40px_rgb(0,0,0,0.06)] hover:-translate-y-1
        ${className}
      `}
    >
      {/* Reflet spéculaire subtil en haut à gauche */}
      <div className="absolute top-0 left-0 w-full h-full bg-gradient-to-br from-white/60 to-transparent opacity-50 pointer-events-none" />
      
      {/* Contenu */}
      <div className="relative z-10">
        {children}
      </div>
    </div>
  );
};

export default GlassCard;