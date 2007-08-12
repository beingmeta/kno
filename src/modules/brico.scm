;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'brico)

(define version "$Id:$")

(define %notighten '{freqfns bricosource brico-pool brico-index})

(define indexinfer #t)

;; For index-name, at least
(use-module '{texttools reflection})
;; When BRICOSOURCE is ".db"
(use-module 'usedb)
;; For various custom methods, etc.
(use-module 'rulesets)

;;; Configuring bricosource

(define bricosource #f)
(define brico-pool {})
(define brico-index {})

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
	   (set! bricosource val)
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

(define indexinfer-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) indexinfer)
	  ((equal? val indexinfer)
	   indexinfer)
	  (else
	   (set! indexinfer val)))))
(config-def! 'indexinfer indexinfer-config)

;;; Common tables

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

;;; Common BRICO frames

(define english @1/2c1c7"English")
(define english-gloss @1/2ffbd"Gloss (English)")
(define english-norm @1/44896)
(define enorm @1/44896)
(define spanish @1/2c1fc"Spanish")
(define french @1/2c122"French")

(define wordnet-source @1/0"WordNet 1.6 Copyright 1997 by Princeton University.  All rights reserved.")
(define roget-source @1/1"Derived from 1911 Roget Thesaurus")
(define brico-source @1/2d9ea{BRICO10})

;; Due to a range of factors, including the roots of WordNet, English is the most complete language, 
;;  so we make it the default.
(define default-language english)

(define genls @1/2c272{GENLS})
(define genls* @1/2c27b{GENLS*})
(define kindof @1/2c272{GENLS})
(define kindof* @1/2c27b{GENLS*})
(define specls @1/2c273{SPECLS})
(define specls* @1/2c27c{SPECLS*})
(define implies @1/2c27e{IMPLIES})
(define implies* @1/2f201{IMPLIES*})
(define impliedby @1/2c27f{IMPLIEDBY})
(define impliedby* @1/44c99{IMPLIEDBY*})

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
(define entails @1/3f65f{ENTAILS})
(define entailedby @1/2b74c{ENTAILEDBY})
(define entails* @1/44c9a{ENTAILS*})
(define entailedby* @1/44c9b{ENTAILEDBY*})

(define defterms @1/2ab4d{DEFTERMS})
(define defines @1/2ab55{DEFINES})
(define refterms @1/2ab57{REFTERMS})
(define referenced @1/2ab5a{REFERENCED})
(define =is= @1/2d9e9{=IS=})
(define sameas @1/2d9e9{SAMEAS})
(define inverse @1/2c27a{INVERSE})
(define disjoint @1/2c27d{DISJOINT})

(define brico-slotids
  (choice genls genls* kindof kindof*
	  specls specls*
	  parts parts* partof partof*
	  members members* memberof memberof*
	  ingredients ingredients* ingredientof ingredientof*
	  =is= sameas inverse disjoint 
	  defterms defines refterms referenced))

;;; Custom data sources

(define custom-norms #f)
(define custom-glosses #f)

;;; A custom entry is a vector of the form:
;;;  #(name lang table)
;;; and rulesets are used to manage the replacement of entries
;;; with the same name.

(define custom-map-name first)
(define custom-map-language second)
(define custom-map-table third)

(define (custom-get-list key language custom)
  (tryif (pair? custom)
	 (let ((custom-entry (car custom)))
	   (try
	    (if (custom-map-language custom-entry)
		(tryif (eq? language (custom-map-language custom-entry))
		       (get (custom-map-table custom-entry) key))
		(get (custom-map-table custom-entry) (cons language key)))
	    (custom-get-list key language (cdr custom))))))

(defambda (custom-get key language custom)
  (if (or (fail? custom) (not custom) (null? custom))
      (fail)
      (if (pair? custom)
	  (custom-get-list key language custom)
	  (choice (get (custom-map-table (pick custom custom-map-language language))
		       key)
		  (get (custom-map-table (pick custom custom-map-language #f))
		       (cons language key))))))

(defslambda (config-custom-norms var (value))
  (if (bound? value)
      custom-norms
      (set! custom-norms
	    (rulset+ (cond ((vector? value) value)
			   ((and (pair? value) (not (table? (cdr value))))
			    (error 'config "Invalid custom table initialization: " value))
			   ((and (pair? value) (oid? (car value)))
			    (vector #f (car value) (cdr value)))
			   ((and (pair? value) (symbol? (car value)))
			    (vector (car value) #f (cdr value)))
			   ((pair? value)
			    (error 'config "Invalid custom table initialization: " value))
			   ((table? value)
			    (vector #f #f value)))
		     custom-norms))))
(config-def! 'CUSTOMNORMS config-custom-norms)

(defslambda (config-custom-glosses var (value))
  (if (bound? value)
      custom-glosses
      (set! custom-glosses
	    (rulset+ (cond ((vector? value) value)
			   ((and (pair? value) (not (table? (cdr value))))
			    (error 'config "Invalid custom table initialization: " value))
			   ((and (pair? value) (oid? (car value)))
			    (vector #f (car value) (cdr value)))
			   ((and (pair? value) (symbol? (car value)))
			    (vector (car value) #f (cdr value)))
			   ((pair? value)
			    (error 'config "Invalid custom table initialization: " value))
			   ((table? value)
			    (vector #f #f value)))
		     custom-glosses))))
(config-def! 'CUSTOMGLOSSES config-custom-glosses)

;;; Getting norms, glosses, etc.

(define (get-norm concept (language default-language))
  (try (tryif custom-norms
	      (pick-one (largest (custom-get concept language custom-norms))))
       (pick-one (largest (get (get concept '%norm) language)))
       (pick-one (largest (get concept language)))
       (pick-one (largest (get concept english)))
       (pick-one (largest (get concept 'names)))
       (pick-one (largest (cdr (get concept '%words))))))

(define (get-gloss concept (language default-language))
  (try (tryif custom-glosses (custom-get concept language custom-glosses))
       (tryif language (get concept (get gloss-map language)))
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

;;; Looking up words

;;; There are two ways of customizing lookup: overrides and overlays.
;;;   overrides replace normal lookup with specific meanings
;;;   overlays augment normal lookup with additional meanings
;;;  An override value of false #f causes lookup to fail (return {}).

(define word-overrides #f)
(define word-overlays #f)

(define (override-get word language)
  (custom-get word language word-overrides))
(define (word-override? word language)
  (exists? (override-get word language)))
(define (overlay-get word language)
  (custom-get word language word-overlays))

(defslambda (config-custom-overrides var (value))
  (if (bound? value)
      word-overrides
      (set! word-overrides
	    (rulset+ (cond ((vector? value) value)
			   ((and (pair? value) (not (table? (cdr value))))
			    (error 'config "Invalid custom table initialization: " value))
			   ((and (pair? value) (oid? (car value)))
			    (vector #f (car value) (cdr value)))
			   ((and (pair? value) (symbol? (car value)))
			    (vector (car value) #f (cdr value)))
			   ((pair? value)
			    (error 'config "Invalid custom table initialization: " value))
			   ((table? value)
			    (vector #f #f value)))
		     word-overrides))))
(config-def! 'WORDOVERRIDE config-custom-overrides)

(defslambda (config-custom-overlay var (value))
  (if (bound? value)
      word-overlays
      (set! word-overlays
	    (rulset+ (cond ((vector? value) value)
			   ((and (pair? value) (not (table? (cdr value))))
			    (error 'config "Invalid custom table initialization: " value))
			   ((and (pair? value) (oid? (car value)))
			    (vector #f (car value) (cdr value)))
			   ((and (pair? value) (symbol? (car value)))
			    (vector (car value) #f (cdr value)))
			   ((pair? value)
			    (error 'config "Invalid custom table initialization: " value))
			   ((table? value)
			    (vector #f #f value)))
		     word-overlays))))
(config-def! 'WORDOVERLAY config-custom-overlay)

;;; Looking up words

;;; This code looks up words in BRICO, potentially applying
;;;  some entirely textual cleverness.  Note that this doesn't
;;;  do any morphological regularization because that lives in
;;;  a different module and this module (BRICO) should be pretty
;;;  freestanding.

;;; It also allows applications to provide OVERRIDES and OVERLAYS
;;;  for word lookup.  OVERRIDES keep searches from going to the
;;;  background index; OVERLAYS are just combined with the background
;;;  index.

;;; The TRYHARD argument indicates how hard to try when doing a lookup.
;;; The values are roughly interpreted as follows:
;;;  #f don't try anything but basestring normalization (Malmö ==> Malmo)
;;;  #t or 1 strips out internal punctuation (e.g. set-up ==> setup)
;;;  >1 looks for misspellings (using metaphone) and also combines
;;;     porter-stemming with metaphone.  Note that these are probably
;;;     not really portable between languages, but that's a TODO.
;;;  >2 look for compounds with overlapping words and picks the
;;;      highest ranking concepts that have an overlap greater 3,
;;;      where a correctly spelled word gets 2 points and a possible
;;;      variant gets 1.
;;;  >4 just like above but accepts any overlap grather than 2
;;;  Warning: this compound finding algorithm has a potential issue
;;;   in that BRICO's fragment indexing doesn't distinguish fragments
;;;   from the same compound, so a concept with terms "George Bush" and
;;;   "Scourge of Washington" would match a fragment-based search for
;;;   "George Washington".

(define (lookup-word word (language default-language) (tryhard #f))
  (if (has-prefix word "~")
      (lookup-word (subseq word 1) language #t)
      (if word-overrides
	  (let ((override (override-get word language)))
	    (if (exists? override) (or override (fail))
		(lookup-word-core word language tryhard)))
	  (lookup-word-core word language tryhard))))

(define (lookup-word-core word language tryhard)
  (try (choice (?? language (stdspace word))
	       (tryif word-overlays (overlay-get word language)))
       (let ((sbword (stdspace (basestring word))))
	 (choice (?? language sbword)
		 (tryif word-overlays (overlay-get sbword language))))
       (tryif (and tryhard
		   (or (position #\- word)
		       (position #\Space word)
		       (position #\_ word)))
	      (?? language (depunct word)))
;;        (tryif tryhard
;; 	      (?? language (choice (downcase word)
;; 				   (capitalize word)
;; 				   (capitalize1 word))))
;;        (tryif (and tryhard (uppercase? word))
;; 	      (?? language (choice (downcase word)
;; 				   (capitalize (downcase word))
;; 				   (capitalize1 (downcase word)))))
       ;; Find misspellings, etc
       (tryif (and (number? tryhard) (> tryhard 1))
 	      (?? language
 		  (choice (metaphone word #t)
 			  (metaphone (porter-stem word) #t))))
       ;; Find concept which have 
       (tryif (and (number? tryhard) (> tryhard 2))
	      (let* ((table (make-hashtable))
		     (wordv (words->vector word))
		     (words (elts wordv))
		     (altwords (choice (metaphone words #t)
				       (metaphone (porter-stem words) #t)))
		     (minscore (min (if (> tryhard 3) 3 4) (length wordv))))
		(prefetch-keys! (list language (choice words altwords)))
		(do-choices (word words)
		  (hashtable-increment! table (?? language (list word))))
		(do-choices (word words)
		  (hashtable-increment! table (overlay-get (list word) language)))
		(do-choices (word altwords)
		  (hashtable-increment! table (overlay-get (list word) language)))
		(do-choices (word altwords)
		  (hashtable-increment! table (?? language (list word))))
		(tryif (and (exists? (table-maxval table))
			    (> (table-maxval table) minscore))
		       (table-max table))))))

;;; Looking up combos

(define default-combo-slotids (choice partof* genls* implies*))

(define (lookup-combo base cxt (slotid #f) (language default-language))
  ;; (message "Looking up combo of " base " and " cxt " linked by " slotid " in " language)
  (let ((sym (pick cxt symbol?)))
    (if (empty? sym)
	(choice
	 (intersection (lookup-word base language)
		       (lookup-word (pick cxt string?) language))
	 (intersection (lookup-word base language)
		       (?? (or slotid default-combo-slotids)
			   (choice (lookup-word (pick cxt string?) language)
				   (pick cxt oid?)))))
	(choice
	 (intersection (lookup-word base language)
		       (lookup-word (pick cxt string?) language)
		       (?? '{type sensecat} sym))
	 (intersection (lookup-word base language)
		       (?? (or slotid default-combo-slotids)
			   (choice (lookup-word (pick cxt string?) language)
				   (pick cxt oid?)))
		       (?? '{type sensecat} sym))))))

;;; Smart utility lookup function
;;;  This figures out the function from the arguments.
;;;  If you're trying to run fast, don't bother with this.

(define (brico/lookup base . args)
  (if (not (string? base))
      (error 'type "Word is not an string")
      (let* ((args (elts args))
	     (strings (pick args string?))
	     (languages (try (pick (pickoids args) 'type 'language) default-language))
	     (slotids (pick (pickoids args) 'type 'slot))
	     (symbols (pick args symbol?))
	     (others (difference args (choice strings languages slotids))))
	(if (exists? symbols)
	    (intersection
	     (if (or (exists? slotids) (exists? strings))
		 (lookup-combo base (qc strings) (qc (try slotids default-combo-slotids))
			       (qc languages))
		 (if (exists? strings)
		     (lookup-word (qc base strings) languages)
		     (lookup-word base languages)))
	     (?? '{sensecat type} symbols))
	    (if (or (exists? slotids) (exists? strings))
		(lookup-combo base (qc strings) (qc (try slotids default-combo-slotids))
			      (qc languages))
		(if (exists? strings)
		    (lookup-word (qc base strings) languages)
		    (lookup-word base languages)))))))

;;; Displaying glosses

(define (gloss f (slotid english-gloss))
  (if (applicable? slotid)
      (lineout f "\t" (slotid f))
      (lineout f "\t" (get f slotid))))

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

;;; Generic prefetching

;;; These functions do generic prefetching for BRICO concepts,
;;;  retrieving both OIDs and the inverted slotid keys used by
;;;  inference.

(define default-brico-slotids
  (choice genls specls partof))

(defambda (brico-prefetch! concepts (slotids default-brico-slotids))
  (prefetch-oids! concepts)
  (prefetch-keys! (cons (choice (get slotids 'inverse)
				(get (get slotids 'slots) 'inverse))
			concepts)))

(defambda (brico-prefetch concepts (slotids default-brico-slotids))
  (brico-prefetch! concepts)
  concepts)

;;; Display an expansion string for a concept in a language

(define (get-expstring concept (language english) (slotid implies))
  "Returns an expansion string indicating a concept and its 'superiors'"
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
	      (get concept slotid) slotid)))

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

(defambda (index-string index frame slot (value #f)
			(window default-frag-window) (phonetic #f))
  (let* ((values (stdspace (if value value (get frame slot))))
	 (expvalues (choice values (basestring values))))
    (doindex index frame slot expvalues)
    (when phonetic
      (let* ((tohash (reject values uppercase?))
	     (tostem (reject tohash length {1 2 3 4}))
	     (tocompoundstem (pick tostem compound?)))
	(doindex index frame slot (metaphone tohash #t))
	(doindex index frame slot (metaphone (porter-stem tostem) #t))
	(doindex index frame slot
		 (metaphone (stem-compound tocompoundstem) #t))))
    (when window
      (index-frags index frame slot expvalues window phonetic))))

(defambda (index-name index frame slot (value #f) (window default-frag-window))
  (let* ((values (downcase (stdspace (if value value (get frame slot)))))
	 (expvalues (choice values (basestring values))))
    (doindex index frame slot expvalues)
    (when window
      (index-frags index frame slot expvalues window))))

(defambda (index-implied index frame slot (values))
  (do-choices (index index)
    (do-choices (slot slot)
      (let ((v (if (bound? values) values (get frame slot))))
	(when (exists? v)
	  (doindex index frame slot (list v))
	  (if indexinfer
	      (begin (doindex index frame slot v)
		     (doindex index frame slot (?? impliedby* v)))
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

(defambda (index-frags index frame slot values window (phonetic #f))
  (let* ((compounds (pick values compound?))
	 (stdcompounds (basestring compounds))
	 (wordv (words->vector compounds))
	 (swordv (words->vector stdcompounds)))
    (doindex index frame slot (vector->frags (choice wordv swordv) window))
    (when phonetic
      (doindex index frame slot
	       (vector->frags (map (lambda (w) (metaphone w #t)) wordv)))
      (doindex index frame slot
	       (vector->frags (map (lambda (w) (metaphone (porter-stem w) #t))
				   wordv))))))

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
  '{PERTAINYM COUNTRY})

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
  (when (and (or (%test frame 'words) (%test frame '%words))
	     (not (test frame 'sensecat)))
    (doindex index frame 'sensecat 'senseless))
  (when (test frame '%index) (doindex index frame (get frame '%index)))
  (doindex index frame '%id (get frame '%mnemonics))
  (doindex index frame '%mnemonic)
  (doindex index frame '%mnemonics)
  (doindex index frame 'has (getslots frame))
  (doindex index frame 'source)
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
      (begin
	(index-core index frame)
	(when (test frame 'type 'language)
	  (index-frame index frame
	    '{langid language iso639/1 iso639/B iso639/T}))
	(when (test frame '{get-methods test-methods add-effects drop-effects})
	  (index-frame index frame
	    '{get-methods test-methods add-effects drop-effects
			  key through derivation inverse closure-of slots
			  primary-slot index %id})))))
  
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
    (index-implied index concept slotid
		   (getallvalues concept slotid)))
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

(define (index-implies index concept slotid)
  (let ((values (get concept slotid)))
    (index-frame index concept slotid (list values))
    (index-frame index concept slotid
		 (get (get concept slotid) implies*))))

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
			  implied-slotids concept-slotids
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

(module-export!
 '{index-brico
   index-concept
   indexer-prefetch
   indexer-lattice-prefetch
   indexer-slotid-prefetch
   prefetch-expansions})

;;; Making %ID slots for BRICO concepts

(define (make%id f (lang default-language))
  (if (test f 'source @1/1) (make-roget-id f lang)
      (if (test f 'type 'wordform)
	  (make-wordform-id f)
	  `(,(pick-one (try (difference (get f 'sensecat) 'NOUN.TOPS)
			    (get f 'sensecat)
			    'VAGUE))
	    ,(get-norm f lang)
	    ,(cond ((and (test f 'sensecat 'noun.location)
			 (%test f partof))
		    'PARTOF)
		   ((%test f 'hypernym) 'GENLS)
		   ((%test f genls) 'GENLS)
		   ((%test f implies) 'ISA)
		   ((%test f partof) 'PARTOF)
		   (else 'TOP))
	    ,@(map get-norm
		   (choice->list
		    (try (tryif (test f 'sensecat 'noun.location)
				(%get f partof))
			 (%get f 'hypernym)
			 (%get f genls)
			 (%get f implies)
			 (%get f partof))))))))

(define (make%id! f (lang default-language))
  (store! f '%id (make%id f lang)))

(define (make-wordform-id f)
  `(WORDFORM ,(get f 'word)
	     ,(try (get (get f 'language) '%mnemonic)
		   (get (get f 'language) 'iso639/1)
		   (get (get f 'language) 'iso639/b)
		   '??)
	     ,(get f 'of)))

(define (make-roget-id f (lang default-language))
  `(ROGET
    ,(get-norm f lang)
    WITHIN
    , (try (get-norm (pick-one (get f 'roget-within)) lang)
	   (get (pick-one (get f 'roget-within)) '%id))))

;;; Getting concept frequency information

;; This is a list of functions to get concept/term frequency information.
;;  Each item is a sequence whose first element is a name (typically a symbol)
;;  and whose remaining elements specify a way to get frequency information.
;;  These can be 
;;  of three arguments (concept, language, term) that returns a count of
;;  either co-occurences of term in language with concept or the absolute
;;  frequency of the concept if language and term are false.
(define freqfns '())

(define (concept-frequency concept (language #f) (term #f))
  (let ((sum 0))
    (dolist (method-spec freqfns)
      (let ((method (second method-spec)))
	(if (applicable? method)
	    (set! sum (+ sum (or (try (method concept language term) 0) 0)))
	    (if (slotid? method)
		(let ((wordslot
		       (and (> (length method-spec) 2) (third method-spec)))
		      (index (and (> (length method-spec) 3) (fourth method-spec))))
		  (set! sum
			(+ sum
			   (if (and wordslot term)
			       (if index
				   (choice-size (find-frames index method concept wordslot term))
				   (choice-size (?? method concept wordslot term)))
			       (if index
				   (choice-size (find-frames index method concept))
				   (choice-size (?? method concept)))))))))))
      sum))

(define use-wordforms #t)

(define (wordform-concept-frequency concept language term)
  (and use-wordforms (in-pool? concept brico-pool) (eq? language english)
       (try (tryif term
		   (get (?? 'of concept 'word term 'language language) 'freq))
	    (reduce-choice + (?? 'of concept) 0 'freq)
	    ;; Otherwise, this make a wild guess, biasing norms
	    (if term
		(if (overlaps? concept (?? (get norm-map language) term)) 3 1)
		1))))

(define usewordforms-config
  (slambda (var (value))
    (if (bound? value)
	(set! use-wordforms value)
	use-wordforms)))
(config-def! 'usewordforms usewordforms-config)

(set! freqfns (list (vector 'wordform wordform-concept-frequency)
		    (vector 'defterms defterms)
		    (vector 'refterms refterms)))

(defambda (concept-frequency-prefetch concepts language words)
  (prefetch-keys! (cons (pick (elts (map second freqfns)) oid?)
			concepts))
  (when (and use-wordforms (eq? language english))
    (prefetch-keys! (choice (cons 'of (pick concepts brico-pool))
			    (cons 'word words)))))

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
(defambda (use-corpus-frequency corpus conceptslot wordslot (langslot #f))
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
			    wordslot word)))))))

(comment
 (define (check-prefetch index f)
   (clearcaches)
   (indexer-slotid-prefetch)
   (indexer-prefetch (qc f))
   (trackrefs (lambda () (index-concept index f)))))

;;; Exports

(module-export!
 '{brico-pool
   brico-index
   ;; Maps for particular languages
   language-map gloss-map norm-map index-map frag-map
   ;; Prefetchers for OIDs and inverted index slotids
   brico-prefetch! brico-prefetch})

(module-export!
 ;; OIDs by name
 '{english
   english-gloss enorm
   spanish french
   implies implies* impliedby impliedby*
   entails entails* entailedby entailedby*
   genls genls* kindof kindof* specls specls*
   parts parts* partof partof*
   members members* memberof memberof*
   ingredients ingredients* ingredientof ingredientof*
   inverse =is= disjoint
   refterms defterms
   wordnet-source roget-source brico-source})

;; Getting glosses, norms, etc.
(module-export! {get-gloss get-single-gloss get-short-gloss get-norm  get-expstring})

;; Looking up words
(module-export! {lookup-word lookup-combo word-override?})

;; UI-ish functions
(module-export! '{gloss brico/lookup})

(module-export!
 { ;; Indexing functions
  doindex
  index-string index-name index-frags index-gloss index-implied index-frame*
  index-core index-brico index-wordform
  index-words index-relations index-lattice index-implies
  indexer index-concept unindex})

(module-export!
 { ;; Getting frequency information
  concept-frequency
  concept-frequency-prefetch})

;;;; For the compiler/optimizer

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

