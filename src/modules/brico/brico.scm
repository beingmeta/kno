;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'brico)

(define version "$Id$")

(define %nosubst '{bricosource
		   brico-pool brico-index
		   xbrico-pool names-pool places-pool
		   freqfns custom-norms custom-glosses
		   use-wordforms})

;; For index-name, at least
(use-module '{texttools reflection})
;; When BRICOSOURCE is ".db"
(use-module 'usedb)
;; For custom methods
(use-module 'rulesets)

;;; Configuring bricosource

(define bricosource #f)
(define brico-pool {})
(define brico-index {})

(define xbrico-pool {})
(define names-pool {})
(define places-pool {})

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
	   (set! xbrico-pool (name->pool "xbrico.beingmeta.com"))
	   (set! names-pool (name->pool "namedb.beingmeta.com"))
	   (set! places-pool (name->pool "placedb.beingmeta.com"))
	   (if (exists? brico-pool) #t
	       (begin (set! brico-index {})
		      #f)))
	  (else
	   (set! bricosource val)
	   (use-pool val)
	   (set! brico-index (onerror (use-index val) #f))
	   (set! brico-pool (name->pool "brico.framerd.org"))
	   (set! xbrico-pool (name->pool "xbrico.beingmeta.com"))
	   (set! names-pool (name->pool "namedb.beingmeta.com"))
	   (set! places-pool (name->pool "placedb.beingmeta.com"))
	   (if (exists? brico-pool) #t
	       (begin (set! brico-index {})
		      #f))))))
(config-def! 'bricosource bricosource-config)

;;; A custom entry is a vector of the form:
;;;  #(name lang table)
;;; and rulesets are used to manage the replacement of entries
;;; with the same name.

(define custom-map-name first)
(define custom-map-language second)
(define custom-map-handler third)

(define (custom-get-list key language custom)
  (tryif (pair? custom)
	 (let* ((entry (car custom))
		(handler (custom-map-handler entry)))
	   (try
	    (if (custom-map-language entry)
		(tryif (eq? language (custom-map-language entry))
		       (if (table? handler) (get handler key)
			   (tryif (applicable? handler)
				  (handler key))))
		(if (table? handler)
		    (get handler (cons language key))
		    (tryif (applicable? handler)
			   (handler key language))))
	    (custom-get-list key language (cdr custom))))))

(defambda (custom-get key language custom)
  (if (or (fail? custom) (not custom) (null? custom))
      (fail)
      (if (pair? custom)
	  (custom-get-list key language custom)
	  (choice (get (custom-map-handler
			(pick custom custom-map-language language))
		       key)
		  (get (custom-map-handler
			(pick custom custom-map-language #f))
		       (cons language key))))))

;; This is used by config functions and massages a variety
;;  of specifications into a custom-map
(define (conform-maprule value)
  (if (vector? value) value
      (let* ((items (if (pair? value) (choice (car value) (cdr value))
			value))
	     (name (try (pick items symbol?) #f))
	     (language (try (pick items oid?) #f))
	     (map (try (difference items (choice name language)) #f)))
	(cond ((not (or (table? map) (applicable? map)))
	       (error 'config "Invalid map: " map))
	      ((and language (applicable? map)
		    (not (= (fcn-min-arity map) 1)))
	       (error 'config "map function requires too many arguments: "
		      map))
	      ((and (not language) (applicable? map)
		    (> (fcn-arity map) 1)
		    (not (= (fcn-min-arity map) 2)))
	       (error 'config "map function has wrong number of arguments: "
		      map))
	      (else (vector name language map))))))

;;; Common tables

(define all-languages (file->dtype (get-component "languages.dtype")))
(define language-map (file->dtype (get-component "langmap.table")))
(define norm-map (file->dtype (get-component "normmap.table")))
(define gloss-map (file->dtype (get-component "glossmap.table")))
(define index-map (file->dtype (get-component "indexmap.table")))
(define frag-map (file->dtype (get-component "fragmap.table")))

(define all-languages (get language-map (getkeys language-map)))
(define all-norms (get norm-map (getkeys norm-map)))
(define all-glosses (get gloss-map (getkeys gloss-map)))

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

(config-def! 'CUSTOMNORMS
	     (ruleset-configfn custom-norms conform-maprule))
(config-def! 'CUSTOMGLOSSES
	     (ruleset-configfn custom-glosses conform-maprule))

;;; Getting IDs

(define (getid concept (language default-language))
  (try (tryif (eq? language english) (get concept '%id))
       (get (get concept '%ids) language)
       (get concept '%id)
       (oid->string concept)))

;;; Getting norms, glosses, etc.

(define (get-norm concept (language default-language) (tryhard #t))
  (try (tryif custom-norms
	      (pick-one (largest (custom-get concept language custom-norms))))
       (pick-one (largest (get (get concept '%norm) language)))
       (pick-one (largest (get concept language)))
       (tryif tryhard
	      (try (pick-one (largest (get concept english)))
		   (pick-one (largest (cdr (get concept '%words))))))))

(define (%get-norm concept (language default-language))
  (try (pick-one (largest (get (get concept '%norm) language)))
       (tryif (eq? language english) (pick-one (largest (get concept 'words))))
       (pick-one (largest (get (get concept '%words) (get language 'key))))
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
  (prefetch-keys!
   (cons (choice (get slotids 'inverse)
		 (get (pick (get slotids 'slots) oid?) 'inverse))
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
	     ,(try (get (get f 'lang) '%mnemonic)
		   (get (get f 'lang) 'iso639/1)
		   (get (get f 'lang) 'iso639/b)
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
  (when (and use-wordforms (eq? language english) (exists? words))
    (prefetch-keys! (choice (cons 'of (pick concepts brico-pool))
			    (cons 'word words))))
  (prefetch-oids! (?? 'of (pick concepts brico-pool)
		      'word words)))

;;; Configuring freqfns

(define (edit-freqfns name fn scan)
  (if (null? scan) (list fn)
      (if (and name (equal? (first (car scan)) name))
	  (cons fn (cdr scan))
	  (cons (car scan) (edit-freqfns name fn (cdr scan))))))

(define (freqfns-config var (val))
  (if (bound? val)
      (set! freqfns (edit-freqfns (first val) val freqfns))
      freqfns))
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

;;; Apply ISA

(define (assign-genls f genl)
  (assert! f genls genl)
  (assert! f 'sensecat (get genl 'sensecat))
  (make%id! f))

(define (fixcaps wdpair wds)
  (cons (car wdpair)
	(let ((wd (cdr wdpair)))
	  (if (capitalized? wd) wd
	      (let ((pos (largest (search wds wd))))
		(if pos
		    (string-append (capitalize (subseq wd 0 pos))
				   (subseq wd pos))
		    (capitalize1 wd)))))))

(define (assign-isa f isa (fixcase #t))
  (assert! f implies isa)
  (assert! f 'sensecat (get isa 'sensecat))
  (when fixcase
    (let ((isa-terms (get isa english)))
      (store! f '%words (fixcaps (get f '%words) (qc isa-terms)))
      (store! f '%norm (fixcaps (get f '%norm) (qc isa-terms)))))
  (make%id! f))

;;; EXPORTS

(module-export!
 '{brico-pool
   brico-index
   xbrico-pool names-pool places-pool
   default-language all-languages
   ;; Maps for particular languages
   language-map gloss-map norm-map index-map frag-map
   all-languages all-glosses all-norms
   ;; Prefetchers for OIDs and inverted index slotids
   brico-prefetch! brico-prefetch})

(module-export!
 '{
   custom-get
   custom-map-name custom-map-language custom-map-handler
   conform-maprule})

(module-export!
 ;; OIDs by name
 '{english
   english-gloss english-norm
   spanish french
   implies implies* impliedby impliedby*
   entails entails* entailedby entailedby*
   genls genls* kindof kindof* specls specls*
   parts parts* partof partof*
   members members* memberof memberof*
   ingredients ingredients* ingredientof ingredientof*
   inverse =is= disjoint
   refterms defterms referenced defines
   wordnet-source roget-source brico-source})

;; Getting glosses, norms, etc.
(module-export!
 '{getid
   get-gloss get-norm get-expstring
   get-single-gloss get-short-gloss gloss})

;; Getting frequency information
(module-export! '{concept-frequency concept-frequency-prefetch})

;;; Miscellaneous functions
(module-export! '{make%id make%id! cap%wds cap%frame! assign-isa assign-genls})

;;;; For the compiler/optimizer

(set+! %constants
       '{english-gloss english-norm
	 english spanish french
	 genls genls* kindof kindof*
	 specls specls*
	 parts parts* partof partof*
	 members members* memberof memberof*
	 ingredients ingredients* ingredientof ingredientof*
	 isa =is= sameas inverse disjoint implies implies*
	 defterms defines refterms referenced
	 language-map gloss-map norm-map index-map})

