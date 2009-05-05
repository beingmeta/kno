(in-module 'knowlets/drules)

(use-module '{texttools fdweb ezrecords varconfig})
(use-module '{knowlets})

(module-export!
 '{kno/drule
   drule-language drule-subject
   drule-cues drule-context+ drule-context+
   drule-threshold})

(defrecord DRULE
  subject
  language
  cues
  context+
  (context- {})
  (threshold 1)
  (knowlet #f))

(defambda (kno/drule subject (language) (cues) (context+)
		     (context- {}) (threshold 1)
		     (knowlet))
  (default! knowlet (get subject 'knowlet))
  (default! language (knowlet-language knowlet))
  (default! cues (get subject language))
  (default! context+ (get subject language))
  (cons-drule subject language cues context+ context- threshold
	      knowlet))

(define (kno/apply-drule drule index (idmap #f))
  )


