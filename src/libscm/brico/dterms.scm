;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'brico/dterms)
;;; Computing disambiguated terms (e.g. term+disambig) for human
;;; readable concepts

;;; FIND-DTERM and friends try to compute dterms using the ontology
(module-export!
 '{find-dterm
   displayterm 
   get-dterm get-dterm/cached
   cached-dterm/prefetch! find-dterm/prefetch!
   dterm-caches})

(use-module '{brico brico/lookup brico/analytics morph/en})

(define dterm-caches '())
(define usesumterms #f)

;;; Top level functions

(define sensecathints
  (file->dtype (get-component "data/sensecathints.table")))

(define (get-dterm concept (language default-language) (norm #f) (tryhard #f))
  (or
   (if (not norm)
       (try (cachecall get-cached-dterm concept language)
	    (get-dterm concept language #t))
       (if (string? norm)
	   (try (cachecall find-dterm concept language norm)
		(try-choices (alt (difference (get concept language) norm))
		  (cachecall find-dterm concept language alt)))
	   (get-dterm concept language (get-norm concept language))))
   (fail)))

(define (get-cached-dterm concept language)
  (tryseq (dtc dterm-caches)
    (if (pair? dtc)
	(tryif (overlaps? (car dtc) language)
	       (get (cdr dtc) concept))
	(get dtc (cons language concept)))))

(define (get-dterm/cached concept language)
  (try (tryseq (dtc dterm-caches)
	 (if (pair? dtc)
	     (tryif (overlaps? (car dtc) language)
		    (get (cdr dtc) concept))
	     (get dtc (cons language concept))))
       #f))

(defambda (dtermcache-prefetch! concepts languages)
  (doseq (cache dterm-caches)
    (when (index? cache)
      (prefetch-keys! cache (cons concepts languages)))))

;;; Finding dterms

(define (termnorm concept language)
  (if (and (test concept 'type 'verb) (eq? language english))
      (try (gerund (get-norm concept language)) (get-norm concept language))
      (get-norm concept language)))

(define (find-dterm concept (language default-language) (norm))
  (default! norm (termnorm concept language))
  ;; (singleton? (?? language norm))
  (if (identical? concept (lookup-term norm language))
      norm
      (try
       (if (test concept 'type '{individual name})
	   (if (test concept 'sensecat 'noun.location)
	       (find-location-dterm concept language norm)
	       (find-individual-dterm concept language norm))
	   (try (find-generic-dterm concept language norm)
		(try-choices (word (difference (get concept language) norm))
		  (find-generic-dterm concept language word))))
       (tryif usesumterms
	      (try-choices (df (get concept sumterms))
		(tryif (singleton? (?? language norm sumterms df))
		       (string-append norm " (:" (get-norm df language) ")"))))
       (try-choices (alt (difference (get concept language) norm))
	 (tryif (search norm alt)
		(try
		 (tryif (singleton? (?? language alt)) alt)
		 (tryif (singleton? (?? language norm language alt))
			(string-append norm ":" alt)))))
       (try-choices (alt (difference (get concept language) norm))
	 (tryif (not (search norm alt))
		(tryif (singleton? (?? language norm language alt))
		       (string-append norm ":" alt))))
       #f)))

(define (probe-location concept term1 term2 language (isaterm #f))
  (let ((norm (get norm-map language)))
    (if isaterm
	(try (?? norm term1 partof* (?? norm term2)
		 implies (list (?? norm isaterm)))
	     (?? norm term1 partof* (?? norm term2)
		 implies (?? norm isaterm))
	     (?? language term1 partof* (?? norm term2)
		 implies (list (?? language isaterm)))
	     (?? language term1 partof* (?? norm term2)
		 implies (?? language isaterm))
	     (?? language term1 partof* (?? language term2)
		 implies (?? language isaterm))
	     (?? language term1 partof* (?? language term2)
		 implies (list (?? language isaterm))))
	(try (?? norm term1 partof* (?? norm term2))
	     (?? language term1 partof* (?? norm term2))
	     (?? language term1 partof* (?? language term2))))))

(define (find-location-dterm concept language norm)
  (let* ((country (get concept 'country))
	 (country-norm (get-norm country language))
	 (region (get concept 'region))
	 (region-norm (get-norm region language))
	 (isa (get concept implies)))
    (try (try-choices country-norm
	   (tryif (identical? concept (probe-location concept norm country-norm language))
		  (stringout norm ", " country-norm)))
	 (try-choices region-norm
	   (tryif (identical? concept (probe-location concept norm region-norm language))
		  (stringout norm ", " region-norm)))
	 (try-choices (isa (get concept implies))
	   (try-choices (isaterm (get-norm isa language))
	     (tryif (identical? concept (probe-location concept norm region-norm language isaterm))
		    (stringout norm ", " region-norm
			       " (" isaterm ")"))))
	 (tryif (test concept 'fips-code)
		(stringout norm "(FIPS-CODE=" (smallest (get concept 'fips-code) length) ")")))))

(define (probeisa concept norm isaterm language normlang)
  (identical? concept
	      (try (?? language norm
		       (choice sometimes always) (list (?? normlang isaterm)))
		   (?? language norm
		       (choice sometimes always) (?? normlang isaterm)))))

(define (find-individual-dterm concept language norm)
  (let* ((isa (get concept implies))
	 (defisa (intersection isa (get concept sumterms)))
	 (otherisa (difference isa defisa))
	 (normslot (get norm-map language)))
    (try (try-choices defisa
	   (try-choices (defisaterm (get-norm defisa language))
	     (tryif (probeisa concept norm defisaterm language normslot)
		    (stringout norm " (" defisaterm ")"))))
	 (try-choices otherisa
	   (try-choices (otherisaterm (get-norm otherisa language))
	     (tryif (probeisa concept norm otherisaterm language normslot)
		    (stringout norm " (" otherisaterm ")")))))))

(define (langterm term language dlang)
  (if (eq? language dlang) term
      (string-append (get dlang 'iso639/1) "$" term)))

(define (find-generic-dterm concept lang norm)
  (let ((sensecat (get concept 'sensecat))
	(normslot (get norm-map lang))
	(meanings (lookup-word norm lang)))
    (try ;; (tryif (singleton? (?? normslot norm)) norm)
	 (tryif (singleton? (?? lang norm)) norm)
	 (try-choices (term (get sensecathints (cons sensecat lang)))
	   (tryif (singleton? (probe-colon-dterm norm lang term lang))
		  (string-append norm ":" term)))
	 (try-choices (gn (get-norm (get concept always) lang))
	   (tryif (singleton? (probe-colon-dterm norm lang gn lang))
		  (string-append norm ":"  gn)))
	 (try-choices (gn (get-norm (get+ concept always) lang))
	   (tryif (singleton? (probe-colon-dterm norm lang gn lang))
		  (string-append norm ":"  gn)))
	 (try-choices (pn (get-norm (get concept @1/5{sometimes}) lang))
	   (tryif (singleton? (probe-paren-dterm norm lang pn lang))
		  (string-append norm " ("  pn ")")))
	 (try-choices (pn (get-norm (get concept @1/2c274{PART-OF}) lang))
	   (tryif (singleton? (probe-paren-dterm norm lang pn lang))
		  (string-append norm " ("  pn ")")))
	 (tryif usesumterms
		(try-choices
		    (d (difference (get-norm (get concept sumterms) lang)
				   norm))
		  (tryif (singleton? (intersection meanings (?? sumterms (?? lang d))))
			 (string-append norm " (:"  d ")")))))))

;;; Prefetching

(defambda (find-dterm/prefetch! concept (language default-language) (norm))
  (default! norm (get-norm concept language))
  (prefetch-oids! concept)
  (prefetch-oids! (%get concept '{region country @1/2c274{PARTOF} @1/2c27e{IMPLIES}}))
  (prefetch-keys!
   (cons (choice language (get norm-map language))
	 (choice (get sensecathints (cons (get concept 'sensecat) language))
		 norm
		 (get (%get concept '{region country
					     @1/2c274{PARTOF}
					     @1/2c27e{IMPLIES}})
		      (get norm-map language))))))
(define find-dterm-prefetch! find-dterm/prefetch!)

;;; Getting display terms

(defambda (displayterm concept language concepts (suffix #f))
  "This gets a term to describe CONCEPT in LANGUAGE which is unique \
   among CONCEPTS.  If it can't find a unique term or generate a dterm,
   returns the concepts norm term with SUFFIX appended."
  (try (try-choices (norm (get-norm concept language))
	 (tryif (singleton? (intersection (lookup-word norm language) concepts))
		norm))
       (get-dterm concept language)
       (if suffix
	   (string-append (get-norm concept language) " "
			  (if (string? suffix) suffix "(alt)"))
	   (get-norm concept language))))

;;; Configuring DTERMCACHES

(defslambda (dtermcaches-config var (value))
  (if (bound? value)
      (if (string? value)
	  (if (file-exists? value)
	      (let ((index (open-index value)))
		(unless (position index dterm-caches)
		  (set! dterm-caches (cons index dterm-caches))))
	      (warning "Non-existent DTERMCACHE file " (write value)))
	  (if (table? value)
	      (unless (position value dterm-caches)
		(set! dterm-caches (cons value dterm-caches)))
	      (error "Invalid DTYPE cache value")))
      dterm-caches))

(config-def! 'dtermcaches dtermcaches-config)


