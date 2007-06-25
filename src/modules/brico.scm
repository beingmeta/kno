(in-module 'brico)

(define %notighten '{freqfns bricosource brico-pool brico-index})

;; For index-name, at least
(use-module 'texttools)
;; When BRICOSOURCE is ".db"
(use-module 'usedb)

(module-export!
 '{brico-pool
   brico-index doindex
   english english-gloss spanish french get-norm
   genls genls* kindof kindof* specls specls*
   parts parts* partof partof*
   members members* memberof memberof*
   ingredients ingredients* ingredientof ingredientof*
   isa inverse =is= disjoint implies implies*
   make%id make%id! cap%wds cap%frame!
   get-gloss get-single-gloss get-short-gloss get-expstring gloss
   language-map gloss-map norm-map index-map frag-map
   index-string index-name index-frags index-gloss index-genls index-frame*
   indexer index-concept
   ;; Specialized versions
   index-core index-brico index-words index-fragments index-relations index-lattice
   basic-concept-frequency concept-frequency use-corpus-frequency
   brico-prefetch! brico-prefetch})

(define bricosource #f)
(define brico-pool {})
(define brico-index {})
(define english @1/2c1c7"English")
(define english-gloss @1/2ffbd"Gloss (English)")
(define spanish @1/2c1fc"Spanish")
(define french @1/2c122"French")
(define genls @1/2c272"SubClassOf")
(define genls* @1/2c27b{GENLS*})
(define kindof @1/2c272"SubClassOf")
(define kindof* @1/2c27b{GENLS*})
(define specls @1/2c273"SubClasses")
(define specls* @1/2c27c{KINDS*})
(define parts @1/2c275{PARTS})
(define parts* @1/2c282{PARTS*})
(define partof @1/2c274{PARTOF})
(define partof* @1/2c281{PARTOF*})
(define members @1/2c278{MEMBERS})
(define members* @1/2c284{MEMBERS*})
(define memberof @1/2c279{MEMBER-OF})
(define memberof* @1/2c283{MEMBER-OF*})
(define ingredients @1/2c276{INGREDIENTS})
(define ingredients* @1/2c286{INGREDIENTS*})
(define ingredientof @1/2c277{INGREDIENTOF})
(define ingredientof* @1/2c285{INGREDIENTOF*})
(define defterms @1/2ab4d{DEFTERMS})
(define defines @1/2ab55{DEFINES})
(define refterms @1/2ab57{REFTERMS})
(define referenced @1/2ab5a{REFERENCED})
(define isa @1/2c27e{ISA})
(define =is= @1/2d9e9{=IS=})
(define sameas @1/2d9e9{SAMEAS})
(define inverse @1/2c27a{INVERSE})
(define disjoint @1/2c27d{DISJOINT})
(define implies @1/3f65f{IMPLIES})
(define implies* @1/2f201{IMPLIES*})


(define brico-slotids
  (choice genls genls* kindof kindof*
	  specls specls*
	  parts parts* partof partof*
	  members members* memberof memberof*
	  ingredients ingredients* ingredientof ingredientof*
	  isa =is= sameas inverse disjoint implies implies*
	  defterms defines refterms referenced))

(define language-map (file->dtype (get-component "langmap.table")))
(define gloss-map (file->dtype (get-component "glossmap.table")))
(define norm-map (file->dtype (get-component "normmap.table")))
(define index-map (file->dtype (get-component "indexmap.table")))
(define frag-map (file->dtype (get-component "fragmap.table")))

;;; This is how these tables where generated
(comment
 (do-choices (l (?? 'type 'language))
   (store! language-map (get l 'key) l)
   (store! language-map (get l 'langid) l))
 (do-choices (l (?? 'type 'gloss))
   (store! gloss-map (get l 'key) l))
 (do-choices (l (?? 'type 'norm))
   (store! norm-map (get l 'key) l))
 (do-choices (l (?? 'type 'indices))
   (store! index-map (get l 'key) l))
 (do-choices (l (?? 'type 'fragments))
   (store! index-map (get l 'key) l)))

(set+! %constants
       '{english-gloss
	 english spanish french
	 genls genls* kindof kindof*
	 specls specls*
	 parts parts* partof partof*
	 members members* memberof memberof*
	 ingredients ingredients* ingredientof ingredientof*
	 isa =is= sameas inverse disjoint implies implies*
	 defterms defines refterms referenced
	 language-map gloss-map norm-map index-map})


;;; Mostly the roots of WordNet....
(define default-language english)

(define (get-norm concept (language default-language))
  (try (pick-one (largest (get (get concept '%norm) language)))
       (pick-one (largest (get concept language)))
       (pick-one (largest (get concept english)))
       (pick-one (largest (get concept 'names)))
       (pick-one (largest (cdr (get concept '%words))))))

(define (get-gloss concept (language default-language))
  (try (tryif language (get concept (get gloss-map language)))
       (tryif language (get (get concept '%glosses) language))
       (get concept english-gloss)
       (get concept 'gloss)))
(define (get-short-gloss concept (language #f))
  (let ((s (get-gloss concept language)))
    (if (position #\; s)
	(subseq s 0 (position #\; s))
	s)))
(define (get-single-gloss concept (language #f))
  (if (or (not language) (eq? language english))
      (try (pick-one (get concept 'gloss))
	   (pick-one (get-gloss concept language)))
      (pick-one  (get-gloss concept language))))
(define (get-expstring concept (language english))
  (stringout
      (let ((words (get concept (choice language 'names))))
	(if (fail? words) (set! words (get concept (choice english 'names))))
	(if (fail? words) (printout "Je n'ai pas les mots.")
	    (do-choices (word words i)
	      (if (position #\Space word)
		  (printout (if (> i 0) " . ") "\"" word "\"")
		  (printout (if (> i 0) " . ") word)))))
    (printout " < ")
    (forgraph (lambda (x)
		(do-choices (word (get x (choice language 'names)) i)
		  (if (position #\Space word)
		      (printout (if (> i 0) " . " " ") "\"" word "\"")
		      (printout (if (> i 0) " . " " ") word))))
	      (get concept implies) implies)))

(define (make%id f (lang default-language))
  `(,(pick-one (try (difference (get f 'sensecat) 'NOUN.TOPS)
		    (get f 'sensecat)
		    'VAGUE))
    ,(get-norm f lang)
    ,(cond ((and (test f 'sensecat 'noun.location)
		 (%test f partof))
	    'PARTOF)
	   ((%test f 'hypernym) 'GENLS)
	   ((%test f genls) 'GENLS)
	   ((%test f ISA) 'ISA)
	   ((%test f partof) 'PARTOF)
	   (else 'TOP))
    ,@(map get-norm
	   (choice->list
	    (try (tryif (test f 'sensecat 'noun.location)
			(%get f partof))
		 (%get f 'hypernym)
		 (%get f genls)
		 (%get f isa)
		 (%get f partof))))))
(define (make%id! f (lang default-language))
  (store! f '%id (make%id f lang)))

;;; Capitalizing word entries

(define (cap%wds e (cautious #f))
  (if (pair? e)
      (if (capitalized? (cdr e)) e
	  (if cautious
	      (cons (car e) (choice (cdr e) (capitalize (cdr e))))
	      (cons (car e) (capitalize (cdr e)))))
      e))

(define (cap%frame! f (cautious #f))
  (let* ((words (get f '%words))
	 (norm (get f '%norm))
	 (cwords (cap%wds words cautious))
	 (cnorm (cap%wds norm cautious))
	 (changed #f))
    (unless (identical? words cwords)
      (store! f '%words cwords)
      (set! changed #t))
    (unless (identical? norm cnorm)
      (store! f '%norm cnorm)
      (set! changed #t))
    changed))

;;; Configuring bricosource

(define bricosource-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) bricosource)
	  ((equal? val bricosource)
	   bricosource)
	  ((exists? brico-pool)
	   (message "Redundant configuration of BRICOSOURCE "
		    "which is already provided from " brico-pool)
	   #f)
	  ((and (string? val)
		(or (has-suffix val ".db")
		    (file-exists? (string-append val ".db"))))
	   (set! brico-index (usedb val))
	   (set! brico-pool (name->pool "brico.framerd.org"))
	   (if (exists? brico-pool) #t
	       (begin (set! brico-index {})
		      #f)))
	  (else
	   (set! bricosource val)
	   (use-pool val)
	   (set! brico-index (onerror (use-index val) #f))
	   (set! brico-pool (name->pool "brico.framerd.org"))
	   (if (exists? brico-pool) #t
	       (begin (set! brico-index {})
		      #f))))))
(config-def! 'bricosource bricosource-config)

;;; Generic prefetching

;;; These functions do generic prefetching for BRICO concepts,
;;;  retrieving both OIDs and the inverted slotid keys used by
;;;  inference.

(define brico-prefetch!
  (ambda (concepts)
    (prefetch-oids! concepts)
    (prefetch-keys! (cons brico-slotids concepts))
    (prefetch-oids! (?? (choice specls* parts*) concepts))))
  
(define brico-prefetch
  (ambda (concepts)
    (brico-prefetch! concepts)
    concepts))

;;; Indexing functions

;;; This optionally takes a slotmap as an index, where the slotmap
;;;  maps slot values to indices.
(define doindex
  (ambda (index frame slotids (values) (inverse #f))
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
	(doindex index values inverse frame))))

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

(define index-frags
  (ambda (index frame slot values window)
    (let* ((compounds (pick values compound?))
	   (stdcompounds (basestring compounds)))
      (doindex index frame slot
	       (vector->frags
		(words->vector
		 (choice compounds stdcompounds))
		window)))))

(define index-string
  (ambda (index frame slot (value #f) (window default-frag-window))
    (let* ((values (stdspace (if value value (get frame slot))))
	   (expvalues (choice values (basestring values))))
      (doindex index frame slot expvalues)
      (when window
	(index-frags index frame slot expvalues window)))))

(define index-name
  (ambda (index frame slot (value #f) (window default-frag-window))
    (let* ((values (downcase (stdspace (if value value (get frame slot)))))
	   (expvalues (choice values (basestring values))))
      (doindex index frame slot expvalues)
      (when window
	(index-frags index frame slot expvalues window)))))

(define (index-genls index frame slot (values))
  (let ((v (if (bound? values) values (get frame slot))))
    (when (exists? v)
      (doindex index frame slot (list v))
      (doindex index frame slot (get v genls*)))))

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

(define genls*-slotids
  '{@1/2c274{PARTOF}
    @1/2c277{INGREDIENT-OF}
    @1/2c279{MEMBER-OF}
    @1/2c27e{ISA}})

(define concept-slotids
  {@1/2ab4d{DEFTERMS}
   @1/2ab57{REFTERMS}
   @1/2b74c{INCLUDES}
   @1/2c275{PARTS}
   @1/2c272{GENLS}
   @1/2c273{SPECLS}
   @1/2c276{INGREDIENTS}
   @1/2c278{MEMBERS}
   @1/2c281{PARTOF*}})

(define (index-concept index concept)
  (index-brico index concept)  
  (index-words index concept)
  (index-relations index concept)
  (index-lattice index concept))

(define wordform-slotids '{word of language rank type})

(define (index-core index frame)
  (doindex index frame '{type sensecat fips-code})
  (when (ambiguous? (get frame 'sensecat))
    (doindex index frame 'sensecat 'vague))
  (when (test frame '%index) (doindex index frame (get frame '%index)))
  (doindex index frame '%id (get frame '%mnemonic))
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

(define (index-brico index frame)
  (cond ((empty? (getslots frame))
	 (index-frame index frame 'status 'deleted))
	((test frame 'type 'wordform)
	 (index-frame index frame wordform-slotids))
	((exists? (get frame 'sensecat))
	 (index-core index frame))
	((test frame 'type 'language)
	 (index-frame index frame
	   '{langid language iso639/1 iso639/B iso639/T})
	 (index-frame index frame '%id (get frame 'language))
	 (index-frame index frame
	   '%id
	   (intern (upcase (get frame '{english-names noms-français}))))
	 (index-core index frame)
	 (index-frame index frame
	   '{get-methods test-methods add-effects drop-effects
			 key through derivation inverse closure-of slots
			 primary-slot index %id}))
	((test frame '{get-methods test-methods add-effects drop-effects})
	 (index-core index frame)
	 (index-frame index frame
	   '{get-methods test-methods add-effects drop-effects
			 key through derivation inverse closure-of slots
			 primary-slot index %id}))))

(define (index-words index concept (window #f))
  (index-string index concept english (get concept 'words) window)
  (index-name index concept 'names (get concept 'names))
  (index-name index concept 'names
	      (pick  (cdr (get concept '%words)) capitalized?))
  (do-choices (xlation (get concept '%words))
    (let ((lang (get language-map (car xlation))))
      (index-string index concept lang (cdr xlation) window)))
  (do-choices (xlation (get concept '%norm))
    (let ((lang (get norm-map (car xlation))))
      (index-string index concept lang (cdr xlation))))
  (do-choices (xlation (get concept '%indices))
    (let ((lang (get index-map (car xlation))))
      (index-string index concept lang (cdr xlation)))))

(define (index-fragments index concept (window 1))
  (index-frags index concept (get frag-map english) (get concept 'words) window)
  (index-frags index concept 'namefrags (get concept 'names) window)
  (index-frags index concept 'namefrags
	       (pick  (cdr (get concept '%words)) capitalized?) window)
  (do-choices (xlation (get concept '%words))
    (let ((lang (get frag-map (get language-map (car xlation)))))
      (index-frags index concept lang (cdr xlation) window))))

(define (index-relations index concept)
  (do-choices (slotid genls*-slotids)
    (index-genls index concept slotid
		 (qc (%get concept slotid)
		     (tryif (oid? slotid)
			    (%get concept (get slotid 'slots))))))
  (do-choices (slotid concept-slotids)
    (doindex index concept slotid
	     (choice (%get concept slotid)
		     (tryif (oid? slotid)
			    (%get concept (get slotid 'slots))))))
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
  (prefetch-oids! (choice brico-slotids
			  genls*-slotids concept-slotids
			  (get language-map (getkeys language-map))
			  (get norm-map (getkeys norm-map))
			  (get index-map (getkeys index-map))
			  (get gloss-map (getkeys gloss-map)))))

(define (indexer-prefetch oids)
  (prefetch-oids! oids)
  (prefetch-keys! (cons (choice refterms referenced) oids))
  (let ((kovalues (%get oids genls*-slotids)))
    (prefetch-expansions (qc kovalues) genls)))

(define (indexer-lattice-prefetch oids)
  (prefetch-oids! oids)
  (prefetch-expansions
   (qc oids) (qc genls partof memberof ingredientof)))

(module-export!
 '{index-brico
   index-concept
   indexer-prefetch
   indexer-lattice-prefetch
   indexer-slotid-prefetch
   prefetch-expansions})

;;; Displaying glosses

(define (gloss f (slotid english-gloss))
  (lineout f "\t" (get f slotid)))

;;; Getting concept frequency information

(define (basic-concept-frequency concept (language #f) (term #f))
  (let ((sum 0) (language (or language english)))
    (do-choices (wf (if term (?? 'of concept 'word term 'language language)
			(?? 'of concept)))
      (set! sum (+ sum (try (get wf 'freq) 0))))
    ;; If there is no frequency data, use the native language occurence
    ;;  as a single instance, providing that it is actually applicable.
    ;; This doesn't do any term morphology, which is handled elsewhere.
    (if (zero? sum) 
	(if (or (not term) (test concept language term)
		(and (capitalized? term)
		     (or (test concept 'names term)
			 (overlaps? (stdstring term)
				    (stdstring (get concept 'names))))))
	    ;; Weight brico concepts higher than external concepts
	    (if (in-pool? concept brico-pool) 2 1)
	    0)
	sum)))

;; This is a list of functions to get concept/term frequency information.
;;  Each item is a car of a name (which is an arbitrary value) and a function
;;  of three arguments (concept, language, term) that returns a count of
;;  either co-occurences of term in language with concept or the absolute
;;  frequency of the concept if language and term are false.
(define freqfns (list (cons 'basic basic-concept-frequency)))

(define (concept-frequency concept (language #f) (term #f))
  (let ((sum 0))
    (dolist (method freqfns)
      (let ((freq ((cdr method) concept language term)))
	(when (and (exists? freq) (number? freq))
	  (set! sum (+ sum freq)))))
    sum))

;;; Configuring freqfns

(define (edit-freqfns name fn scan)
  (if (null? scan) (fail)
      (if (and name (equal? (car (car scan)) name))
	  (cons (cons name fn) (cdr scan))
	  (cons (car scan) (edit-freqfns name fn (cdr scan))))))

(define (freqfns-config var (val))
  (if (bound? val)
      (if (pair? val)
	  (set! freqfns
		(try (edit-freqfns (car val) (cdr val) freqfns)
		     (cons val freqfns)))
	  (set! freqfns (cons (cons #f val) freqfns)))
      disambigfns))
(config-def! 'freqfns freqfns-config)

;; This returns a method for using indexed corpora to provided
;; absolute or contingent concept frequency information.
(define use-corpus-frequency
  (ambda (corpus conceptslot wordslot (langslot #f))
    (cons
     (vector (qc corpus) conceptslot wordslot)
     (lambda (concept language word)
       (if (not language)
	   (choice-size (find-frames corpus conceptslot concept))
	   (if langslot
	       (choice-size (find-frames corpus
			      conceptslot concept
			      wordslot word
			      langslot language))
	       (choice-size (find-frames corpus
			      conceptslot concept
			      wordslot word))))))))

(comment
 (define (check-prefetch index f)
   (clearcaches)
   (indexer-slotid-prefetch)
   (indexer-prefetch (qc f))
   (trackrefs (lambda () (index-concept index f)))))

