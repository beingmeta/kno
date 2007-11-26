;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'brico/indexing)

(define version "$Id$")

(use-module '{brico texttools})

(define %nosubst '{indexinfer default-frag-window})

(define indexinfer #t)

(define indexinfer-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) indexinfer)
	  ((equal? val indexinfer)
	   indexinfer)
	  (else
	   (set! indexinfer val)))))
(config-def! 'indexinfer indexinfer-config)

;;; Indexing functions

;;; This optionally takes a slotmap as an index, where the slotmap
;;;  maps slot values to indices.
(defambda (doindex index frame slotids (values) (inverse #f))
  (if (index? index)
      (if (bound? values)
	  (index-frame index frame slotids values)
	  (index-frame index frame slotids))
      (do-choices (slotid slotids)
	(let ((index (try (get index slotid)
			  (get index '%default))))
	  (if (empty? index)
	      (warning "No index found for " slotid)
	      (if (bound? values)
		  (index-frame index frame slotids values)
		  (index-frame index frame slotids))))))
  (if (and (bound? values) inverse)
      (doindex index values inverse frame)))

(defambda (unindex frame slot value (index #f))
  (do-choices (slot slot)
    (do-choices (value value)
      (if (not index)
	  (do-choices (index (config 'background))
	    (when (%test index (cons slot value) frame)
	      (message "Dropping references from " index)
	      (drop! index  (cons slot value) frame)))
	  (when (%test index (cons slot value) frame)
	    (drop! index  (cons slot value) frame))))))

;;; Slot indexing functions

(define (stem-compound string)
  (seq->phrase (map porter-stem (words->vector string))))

(define (cap-metaphone string)
  (if (capitalized? string)
      (string->packet (capitalize (metaphone string)))
      (metaphone string #t)))

(defambda (index-string index frame slot (value #f) (frag #f))
  (let* ((values (stdspace (if value value (get frame slot))))
	 (expvalues (choice values (basestring values)))
	 (normvalues (capitalize (pick expvalues somecap?))))
    ;; By default, we index strings under their direct values, under
    ;;  their values without diacritics, and under versions with normalized
    ;;  capitalization.  Normalizing capitalization makes all elements of a
    ;;  compound be uppercase and makes oddly capitalized terms (e.g. iTunes)
    ;;  be lowercased.
    (doindex index frame slot (choice expvalues normvalues))
    (doindex index frame slot (cap-metaphone (choice values normvalues)))
    (when frag (index-frags index frame slot values 1 #f))))

(defambda (index-name index frame slot (value #f) (window default-frag-window))
  (let* ((values (downcase (stdspace (if value value (get frame slot)))))
	 (expvalues (choice values (basestring values))))
    (doindex index frame slot expvalues)
    (when window
      (index-frags index frame slot expvalues window))))

(defambda (index-implied-values index frame slot (values))
  (do-choices (index index)
    (do-choices (slot slot)
      (let ((v (if (bound? values) values (get frame slot))))
	(when (exists? v)
	  (doindex index frame slot (choice v (list v)))
	  (if indexinfer
	      (doindex index frame slot (?? impliedby* v))
	      (doindex index frame slot (get v implies*))))))))

(define (index-frame* index frame slot base (inverse #f))
  (do ((g (get frame base) (difference (get g base) seen))
       (seen frame (choice g seen)))
      ((empty? g))
    (doindex index frame slot g inverse)))

(define (index-gloss index frame slotid (value #f))
  (let* ((wordlist (getwords (or value (get frame slotid))))
	 (gloss-words (filter-choices (word (elts wordlist))
			(< (length word) 16))))
    (doindex index frame slotid
	     (choice gloss-words (porter-stem gloss-words)))))

;;; Indexing string fragments

(define default-frag-window #f)

(define fragwindow-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) default-frag-window)
	  ((equal? val default-frag-window) default-frag-window)
	  ((not val) (set! default-frag-window #f))
	  ((and (number? val) (exact? val) (> val 0))
	   (set! default-frag-window #f))
	  (else (warning "Invalid fragment window" val)))))
(config-def! 'fragwindow fragwindow-config)

(define (metaphone1 w)
  (tryif (and (not (uppercase? w)) (> (length w) 3))
	 (metaphone w #t)))
(define (metaphone2 w)
  (tryif (and (not (uppercase? w)) (> (length w) 5))
	 (metaphone (porter-stem w) #t)))

(defambda (index-frags index frame slot values window (phonetic #f))
  (let* ((compounds (pick values compound?))
	 (stdcompounds (basestring compounds))
	 (wordv (words->vector compounds))
	 (swordv (words->vector stdcompounds)))
    (doindex index frame slot (vector->frags (choice wordv swordv) window))
    (when phonetic
      (doindex index frame slot
	       (vector->frags (map metaphone1 wordv)))
      (doindex index frame slot
	       (vector->frags (map metaphone2 wordv))))))

;;; Frame indexing functions

;; These are slotids whose indexed values should include implications
(define implied-slotids
  '{@1/2c274{PARTOF}
    @1/2c277{INGREDIENT-OF}
    @1/2c279{MEMBER-OF}
    @1/2c27e{IMPLIES}})

;; These are slotids whose inverses should also be indexed
(define concept-slotids
  {@1/3f65f{ENTAILS}
   @1/2c272{GENLS}
   @1/2c273{SPECLS}
   @1/2c274{PARTOF}
   @1/2c275{PARTS}
   @1/2c277{STUFFOF}
   @1/2c276{INGREDIENTS}
   @1/2c279{MEMBEROF}
   @1/2c278{MEMBERS}
   @1/2c27d{DISJOINT}
   @1/2d9e9{=IS=}
   @1/2ab4d{DEFTERMS}
   @1/2ab57{REFTERMS}})
;(define asymmetric-slotids {@1/2ab4d{DEFTERMS} @1/2ab57{REFTERMS}})

;; These are various other slotids which are useful to index
(define misc-slotids
  '{PERTAINYM REGION COUNTRY})

(define (index-concept index concept)
  (index-brico index concept)  
  (index-words index concept)
  (index-relations index concept)
  (index-lattice index concept))

(define wordform-slotids '{word of language rank type})

(define (index-core index frame)
  (doindex index frame '{type sensecat fips-code source})
  (when (and (or (%test frame 'words) (%test frame '%words))
	     (ambiguous? (get frame 'sensecat)))
    (doindex index frame 'sensecat 'ambiguous))
  (when (and (test frame '{%words words word type})
	     (not (test frame 'sensecat)))
    (doindex index frame 'sensecat 'senseless))
  (when (test frame '%index) (doindex index frame (get frame '%index)))
  (doindex index frame '%id (get frame '{%mnemonics %mnemonic}))
  (when (or (test frame 'type 'slot) (test frame 'type 'get-methods))
    (doindex index frame '%id))
  (doindex index frame '%ids)
  (doindex index frame '%mnemonic)
  (doindex index frame '%mnemonics)
  (doindex index frame 'has (getslots frame))
  ;; Special case 'has' indexing
  (when (test frame 'gloss)
    (doindex index frame 'has english-gloss))
  (when (test frame '%glosses)
    (doindex index frame 'has
	     (get gloss-map (car (get frame '%glosses)))))
  (when (test frame '%words)
    (doindex index frame 'has
	     (get language-map (car (get frame '%words)))))
  (when (test frame '%norm)
    (doindex index frame 'has
	     (get norm-map (car (get frame '%norm)))))
  (when (test frame '%indices)
    (doindex index frame 'has
	     (get index-map (car (get frame '%indices))))))

(define (index-wordform index frame)
  (when (test frame 'type 'wordform)
    (index-frame index frame wordform-slotids)))

(define (index-brico index frame)
  (if (empty? (getslots frame))
      (index-frame index frame 'status 'deleted)
      (if (test frame 'source @1/1)
	  ;; We minimally index the Roget frames, since they tend
	  ;;  to mostly get in the way and often be archaic
	  (begin (doindex index frame '{type source})
		 (doindex index frame 'has (getslots frame)))
	  (begin
	    (index-core index frame)
	    (when (test frame 'type 'language)
	      (index-frame index frame
		'{langid language iso639/1 iso639/B iso639/T}))
	    (when (test frame '{get-methods test-methods add-effects drop-effects})
	      (index-frame index frame
		'{get-methods test-methods add-effects drop-effects
			      key through derivation inverse closure-of slots
			      primary-slot index %id}))))))

(define (index-words index concept)
  (index-string index concept english (get concept 'words))
  (index-name index concept 'names (get concept 'names))
  (index-name index concept 'names
	      (pick  (cdr (get concept '%words)) capitalized?))
  (do-choices (xlation (get concept '%words))
    (let ((lang (get language-map (car xlation))))
      (index-string index concept lang (cdr xlation))))
  (do-choices (xlation (get concept '%norm))
    (let ((lang (get norm-map (car xlation))))
      (index-string index concept lang (cdr xlation))))
  (do-choices (xlation (get concept '%indices))
    (let ((lang (get index-map (car xlation))))
      (index-string index concept lang (cdr xlation)))))

;; (define (index-fragments index concept (window 1))
;;   (index-frags index concept (get frag-map english) (get concept 'words) window)
;;   (index-frags index concept 'namefrags (get concept 'names) window)
;;   (index-frags index concept 'namefrags
;; 	       (pick  (cdr (get concept '%words)) capitalized?) window)
;;   (do-choices (xlation (get concept '%words))
;;     (let ((lang (get frag-map (get language-map (car xlation)))))
;;       (index-frags index concept lang (cdr xlation) window))))

(define (getallvalues concept slotids)
  (choice (%get concept slotids)
	  (let ((next (difference (%get (pick slotids oid?) 'slots) slotids)))
	    (tryif (exists? next)
		   (getallvalues concept (qc next))))))

(define (index-relations index concept)
  (do-choices (slotid implied-slotids)
    (index-implied-values index concept slotid (getallvalues concept slotid)))
  (do-choices (slotid concept-slotids)
    (doindex index concept slotid
	     (get concept slotid)
	     (and (oid? slotid)
		  (try (get slotid 'inverse) #f))))
  (do-choices (slotid misc-slotids)
    (doindex index concept slotid (get concept slotid)))
  ;; This handles the case of explicit inverse pointers.
  ;;  If we want to add a pointer R from X to Y and
  ;;   we can't or don't want to modify X, we store
  ;;   an explicit inverse ((inv R) Y)=X which will be
  ;;   found by the inverse inference methods.
  ;; Otherwise, we don't index the @?defines and @?referenced
  ;;  slots because it is easier to just get @?defterms
  ;;  and @?refterms.
  (when (%test concept defines)
    (doindex index (%get concept defines) defterms concept))
  (when (%test concept referenced)
    (doindex index (%get concept referenced) refterms concept)))

(define (index-lattice index concept)
  (index-frame* index concept genls* genls specls*)
  (index-frame* index concept partof* partof parts*)
  (index-frame* index concept memberof* memberof members*)
  (index-frame* index concept ingredientof* ingredientof ingredients*))

(define (indexer index concept (slotids) (values))
  (if (bound? slotids)
      (if (bound? values)
	  (doindex index concept slotids values)
	  (doindex index concept slotids))
      (index-concept index concept)))

(define (next-expansion expansions visited)
  (let ((oids (get expansions (getkeys expansions))))
    (prefetch-oids! oids)
    (hashset-add! visited oids)
    (let ((table (make-hashtable)))
      (do-choices (slotid (getkeys expansions))
	(prefetch-keys! (cons (get slotid inverse) (get expansions slotid)))
	(let ((next (reject (get (get expansions slotid) slotid)
			    visited)))
	  (when (exists? next)
	    (add! table slotid next))))
      (if (exists? (getkeys table)) table (fail)))))

(define (prefetch-expansions oids slotids)
  (let ((visited (choice->hashset oids))
	(next (make-hashtable)))
    (prefetch-oids! oids)
    (prefetch-keys! (cons (get (pick slotids oid?) 'inverse) oids))
    (do-choices (slotid slotids)
      (add! next slotid (get oids slotid)))
    (do ((scan next (next-expansion (qc scan) visited)))
	((fail? scan)))))

(define (indexer-slotid-prefetch)
  (prefetch-oids! (choice implied-slotids concept-slotids
			  (get language-map (getkeys language-map))
			  (get norm-map (getkeys norm-map))
			  (get index-map (getkeys index-map))
			  (get gloss-map (getkeys gloss-map)))))

(define (indexer-prefetch oids)
  (prefetch-oids! oids)
  (prefetch-keys! (cons (choice refterms referenced) oids))
  (let ((kovalues (%get oids implied-slotids)))
    (prefetch-expansions (qc kovalues) genls)))

(define (indexer-lattice-prefetch oids)
  (prefetch-oids! oids)
  (prefetch-expansions
   (qc oids) (qc genls partof memberof ingredientof)))

;;; EXPORTS

;;; These are helpful for indexing data BRICO-style or indexing
;;;  data which use BRICO.
(module-export!
 '{
   doindex
   indexer unindex
   index-string index-name index-frags index-frame*
   index-implied-values})

;;; These all support indexing BRICO itself
(module-export!
 '{index-brico
   index-core index-brico index-wordform
   index-words index-relations index-lattice
   index-concept
   indexer-prefetch
   indexer-lattice-prefetch
   indexer-slotid-prefetch})


