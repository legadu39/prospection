import React, { useState, useMemo } from 'react';
import GlassCard from '../../ui/GlassCard';
import { ChevronDown, Lock, ExternalLink, Play, FileText } from 'lucide-react';

const IntelCard = ({ 
  title, 
  summary, 
  fullContent, 
  videoSrc, 
  actionLabel, 
  onAction, 
  readTime, 
  isLocked = false 
}) => {
  const [isOpen, setIsOpen] = useState(false);

  // SÉCURITÉ XSS: Sanitization native sans dépendance externe
  const safeContent = useMemo(() => {
    if (typeof window === 'undefined') return { __html: '' };
    
    // Création d'un parser DOM virtuel
    const parser = new DOMParser();
    const doc = parser.parseFromString(fullContent, 'text/html');
    
    // Liste blanche de tags autorisés pour le formatage basique
    const allowedTags = ['b', 'strong', 'i', 'em', 'u', 'p', 'ul', 'li', 'br', 'span'];
    
    // Nettoyage récursif
    const cleanNode = (node) => {
      const children = Array.from(node.childNodes);
      children.forEach(child => {
        if (child.nodeType === 1) { // Element node
           if (!allowedTags.includes(child.tagName.toLowerCase())) {
             // Si tag interdit, on remplace par son contenu texte ou on supprime
             const text = document.createTextNode(child.textContent);
             child.parentNode.replaceChild(text, child);
           } else {
             // Suppression de tous les attributs (on* handlers, javascript:, styles bizarres)
             while (child.attributes.length > 0) {
               child.removeAttribute(child.attributes[0].name);
             }
             cleanNode(child);
           }
        }
      });
    };
    
    cleanNode(doc.body);
    return { __html: doc.body.innerHTML };
  }, [fullContent]);

  return (
    <GlassCard className="group relative transition-all duration-300 hover:shadow-lg hover:border-blue-300/30">
      {/* HEADER VISIBLE (Le Hook "Média") */}
      <div 
        onClick={() => !isLocked && setIsOpen(!isOpen)} 
        className={`p-5 cursor-pointer ${isLocked ? 'opacity-70 cursor-not-allowed' : ''}`}
      >
        <div className="flex justify-between items-start mb-3">
          <span className="inline-flex items-center gap-1.5 text-[10px] font-mono text-blue-600 bg-blue-50 border border-blue-100 px-2 py-1 rounded-md uppercase tracking-wide">
            {videoSrc ? <Play size={10} fill="currentColor" /> : <FileText size={10} />}
            NEXUS MÉDIA • {readTime}
          </span>
          <div className={`transition-transform duration-300 ${isOpen ? 'rotate-180' : ''}`}>
            {isLocked ? <Lock size={14} className="text-slate-400" /> : <ChevronDown size={16} className="text-slate-400" />}
          </div>
        </div>
        
        <h3 className="text-base font-bold text-slate-900 mb-2 leading-tight group-hover:text-blue-700 transition-colors">
          {title}
        </h3>
        
        <p className="text-xs text-slate-500 leading-relaxed font-medium line-clamp-2">
          {summary}
        </p>
      </div>

      {/* CONTENU DÉPLIABLE (Format "Video Capsule") */}
      <div 
        className={`overflow-hidden transition-[max-height,opacity] duration-500 ease-in-out ${isOpen ? 'max-h-[1200px] opacity-100' : 'max-h-0 opacity-0'}`}
      >
        <div className="border-t border-slate-100 bg-slate-50/50 p-0">
          
          {/* ZONE VIDÉO (Format TikTok/Vertical ou 16:9 responsive) */}
          {videoSrc && (
            <div className="w-full bg-black relative flex justify-center aspect-video sm:aspect-auto sm:h-[400px]">
              <iframe 
                src={videoSrc} 
                className="w-full h-full object-cover sm:max-w-[300px]"
                title="Nexus Video Analysis"
                frameBorder="0"
                allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                allowFullScreen
              ></iframe>
            </div>
          )}

          <div className="p-5">
            {/* Résumé Texte ("Key Takeaways") */}
            <div className="mb-6">
                <h4 className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-2">Points Clés de l'analyse</h4>
                {/* Rendu Sécurisé via Sanitizer Native */}
                <div 
                    className="prose prose-sm prose-slate max-w-none text-xs text-slate-600 leading-relaxed space-y-2 marker:text-blue-500"
                    dangerouslySetInnerHTML={safeContent} 
                />
            </div>

            {/* Call To Action "Soft" (Contextuel) */}
            <div className="p-4 bg-white border border-blue-100 rounded-xl shadow-sm flex flex-col items-center text-center">
                <p className="text-xs font-medium text-slate-600 mb-3">
                Pour reproduire la stratégie détaillée dans la vidéo :
                </p>
                <button 
                onClick={(e) => {
                    e.stopPropagation();
                    onAction();
                }}
                className="w-full py-3 px-4 bg-slate-900 hover:bg-blue-600 text-white text-xs font-bold rounded-xl shadow-lg shadow-blue-900/10 transition-all active:scale-[0.98] flex items-center justify-center gap-2"
                >
                <span>{actionLabel}</span>
                <ExternalLink size={12} />
                </button>
                <span className="text-[9px] text-slate-400 mt-2">
                Partenaire vérifié • Lien officiel
                </span>
            </div>
          </div>
        </div>
      </div>
    </GlassCard>
  );
};

export default IntelCard;