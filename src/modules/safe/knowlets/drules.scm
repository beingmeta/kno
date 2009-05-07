(in-module 'knowlets/drules)

(use-module '{texttools fdweb ezrecords varconfig})
(use-module '{knowlets})

(module-export!
 '{kno/drule
   kno/apply-drule! kno/apply-drules!
   drule-language drule-subject
   drule-cues drule-context+ drule-context-
   drule-threshold})

(defrecord DRULE
  subject
  language
  cues
  context+
  (context- {})
  (threshold 1)
  (knowlet #f))

(defambda (kno/drule subject (language) (cues) (context+ {})
		     (context- {}) (threshold 1)
		     (knowlet))
  (default! knowlet (get subject 'knowlet))
  (default! language (knowlet-language knowlet))
  (default! cues (get subject language))
  (cons-drule subject language cues context+ context- threshold
	      knowlet))

(define (run-gauntlet candidate gauntlet threshold (i 0) (count 0))
  (when (< threshold 1)
    (set! threshold (floor (* (length gauntlet) threshold))))
  (or (>= count threshold)
      (and (< i (length gauntlet))
	   (if (overlaps? candidate (elt gauntlet i))
	       (run-gauntlet candidate gauntlet threshold
			     (1+ i) (1+ count))
	       (run-gauntlet candidate gauntlet threshold
			     (1+ i) count)))))

(define (kno/apply-drule! drule index (idmap #f) (slotid 'tags))
  (let* ((matches
	  (find-frames index
	    '{words roots refs} (drule-cues drule)))
	 (excluded
	  (choice (find-frames index
		    '{words roots refs} (pickstrings (drule-context- drule)))
		  (find-frames index
		    'tags (pickoids (drule-context- drule)))))
	 (surviving (difference matches excluded))
	 (gauntlet (map (lambda (val)
			  (if (oid? val) (find-frames index 'tags val)
			      (find-frames index '{words roots refs} val)))
			(sorted (drule-context+ drule))))
	 (elected
	  (if (= (length gauntlet) 0)
	      surviving
	      (if (drule-threshold drule)
		  (filter-choices (c surviving)
		    (run-gauntlet c gauntlet (drule-threshold drule)))
		  (filter-choices (c surviving)
		    (every? (lambda (x) (overlaps? c x)) gauntlet))))))
    (add! (if idmap (get idmap elected) elected)
	  slotid (drule-subject drule))
    (add! index (cons slotid (drule-subject drule))
	  elected)
    (add! index (cons 'has slotid) elected)))

(define (kno/apply-drules! knowlet index (idmap #f) (slotid 'concepts))
  (let* ((words (get (getkeys index) '{words roots refs}))
	 (drules (get (knowlet-drules knowlet) words)))
    (do-choices (drule drules)
      (kno/apply-drule! drule index idmap slotid))))

