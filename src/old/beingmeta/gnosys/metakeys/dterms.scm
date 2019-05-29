;;; Parsing dterms

(define (parse-dterm-inner string (language default-language) (narrow #f))
  (cond ((= (length string) 0) (fail))
	((eqv? (elt string 0) #\@)
	 (string->lisp string))
	((position #\( string)
	 (let* ((open (position #\( string))
		(close (position #\) string open))
		(prefix (stdspace (subseq string 0 open)))
		(disambig (stdspace (subseq string (1+ open) close)))
		(candidates (lookup-term prefix language narrow))
		(disambiguator (parse-dterm disambig language 1)))
	   (try
	    (intersection candidates disambiguator)
	    (intersection candidates (?? implies disambiguator))
	    (intersection candidates (?? partof disambiguator))
	    (intersection candidates (?? partof* disambiguator))
	    (intersection candidates (?? kindof* disambiguator))
	    (intersection candidates (?? defterms disambiguator))
	    (intersection candidates (?? refterms disambiguator)))))
	((position #\, string)
	 (let* ((split (position #\, string))
		(before (subseq string 0 split))
		(after (stdspace (subseq string (1+ split))))
		(afterplace (if (position #\, after)
				(lookup-norm after language #f)
				(lookup-norm after language #t)))
		(candidates1 (lookup-term before language #f))
		(candidates2 (lookup-term before language #t)))
	   (try
	    (intersection candidates1 (?? partof* afterplace))
	    (intersection candidates2 (?? partof* afterplace)))))
	((position #\: string)
	 (let* ((split (position #\: string))
		(before (stdspace (subseq string 0 split)))
		(after (stdspace (subseq string (1+ split)))))
	   (intersection (lookup-term before language #t)
			 (lookup-norm after language #f))))
	((position #\= string)
	 (let* ((split (position #\= string))
		(slotid (string->lisp (subseq string 0 split)))
		(valstring (subseq string (1+ split))))
	   (if (position #\Space string split)
	       (try (?? slotid valstring)
		    (?? slotid (stdstring valstring)))
	       (try (?? slotid valstring)
		    (?? slotid (string->lisp valstring))))))
	((eqv? (elt string 0) #\$)
	 (if (textmatcher #("$" (isalpha) (isalpha) ":") string)
	     (parse-dterm (subseq string 4) (?? 'iso639/1 (subseq string 1 3))
			  narrow)
	     (parse-dterm (subseq string 1) english narrow)))
	(narrow (lookup-norm string language #f))
	(else (lookup-term string language #t))))

(define (parse-dterm string (language default-language) (narrow #f))
  (cachecall parse-dterm-inner string language narrow))
;; Comment out to enable caching
;;(define parse-dterm parse-dterm-inner)

;;; Utility functions

(define (get-wordlist concept (language default-language) (hint #f))
  (let ((cpairs (for-choices (c concept) (cons c (get c language)))))
    (append
     (or hint '())
     (map cdr
	  (rsorted cpairs
		   (lambda (p)
		     (concept-frequency (car p)  language (cdr p)))))
     (sorted (get concept 'names) length))))
(define (get-norm-wordlist concept (language default-language) (hint #f))
  (let ((cpairs (for-choices (c concept)
		  (cons c (get (get c '%norm) language)))))
    (append
     (or hint '())
     (map cdr
	  (rsorted cpairs
		   (lambda (p)
		     (concept-frequency (car p)  language (cdr p)))))
     (sorted (get concept 'names) length))))

(unless (bound? map->table)
  (define map->table
    (ambda (values fns)
      (for-choices (fn fns)
	(let ((f (frame-create #f)))
	  (cond ((or (oid? fn) (symbol? fn))
		 (do-choices (value values)
		   (store! f value (get value fn))))
		((applicable? fn)
		 (do-choices (value values)
		   (store! f value (fn value))))
		((table? fn)
		 (do-choices (value values)
		   (store! f value (get fn value)))))
	  f)))))

;;; DTERM functions

(module-export!
 ;; Term and dterm parsing, lookup, and generation
 '{
   parse-dterm
   get-dterm cached-dterm dterm-base
   get-term get-unique-term
   prefetch-dterms! get-dterm-prefetch!
   clear-metakey-caches!})

(define (dterm-base string)
  (let* ((len (length string))
	 (break (smallest
		 (choice (or (position #\: string) len)
			 (or (position #\, string) len)
			 (or (position #\( string) len)))))
    (if (and (exists? break) (< break len))
	(subseq string 0 break)
	string)))

;;; GET-DTERM

;;; GET-DTERM is the main entry point for getting dterms that uses
;;;  a set of dterm caches and then calls compute-dterm, adding a crude
;;;  disambiguator if it fails to be unique

(define dterm-caches {})

(define (get-dterm concept (language default-language) (hint #f) (crude #f))
  (try (cached-dterm concept language hint)
       (cachecall compute-dterm concept language hint)
       (tryif crude (crude-dterm concept language hint))))

(define (cached-dterm concept (language default-language) (hint #f))
  "Simply returns a dterm from the cache and fails otherwise.  This checks \
that the result starts with hint, if provided."
  (if hint
      (pick-one (pick (get-dterm-from-cache dterm-caches concept language)
		      has-prefix hint))
      (pick-one (get-dterm-from-cache dterm-caches concept language))))

(define dterm-cache-config
  (slambda (var (val))
    (cond ((not (bound? val)) dterm-caches)
	  ((not val) (set! dterm-caches {}))
	  ((index? val) (set+! dterm-caches val))
	  ((string? val) (set+! dterm-caches (open-index val)))
	  (else (set+! dterm-caches val)))))
(config-def! 'dterm-cache dterm-cache-config)

(define (get-dterm-from-cache cache concept language)
  (if (pair? cache)
      (tryif (overlaps? language (car cache))
	     (get (cdr cache) concept))
      (get cache (cons language concept))))

(define (prefetch-dterms! concept (language default-language))
  (prefetch-oids! concept)
  (prefetch-keys!
   (pick (get (pick dterm-caches pair?) language) index?) concept)
  (prefetch-keys!
   (pick (reject dterm-caches pair?) index?) (cons language concept)))

;;; Generating dterms automatically

(module-export! '{get-dterm
		  cached-dterm
		  compute-dterm compute-dterm-prefetch!})

(define sensecathints
  (file->dtype (get-component "sensecathints.table")))

(define (unique-combo? c word1 word2 language)
  (let ((c1 (?? language word1)) (c2 (?? (get norm-map language) word2)))
    (identical? c (filter-choices (cand c1)
		    (or (overlaps? c2 (?? specls* cand))
			(overlaps? c2 (?? genls cand)))))))

(define (try-combo c1 norm c2 language)
  (string-append norm ":"
		 (pick-one
		  (smallest
		   (filter-choices (wd (get (get c2 '%norm) language))
		     (unique-combo? c1 norm wd language))))))

(define (sensecat-probe concept language)
  (get sensecathints (cons (get concept 'sensecat) language)))
(define (basecat-probe concept language)
  (?? 'type 'basic specls* concept))

(define (compute-dterm concept (language english) (hint #f))
  (try (tryif hint (compute-dterm-from concept language hint))
       (pick-one (smallest (for-choices (norm (get (get concept '%norm) language))
			     (compute-dterm-from concept language norm))))))

(define (compute-dterm-from concept language norm)
  (cond ((singleton? (?? language norm)) norm)
	((test concept 'type 'individual)
	 (if (test concept 'sensecat 'noun.location)
	     (compute-location-dterm concept language norm)
	     (compute-individual-dterm concept language norm)))
	(else
	 (pick-one
	  (smallest
	   (try (let ((senseterms (sensecat-probe concept language)))
		  (for-choices (st senseterms)
		    (tryif (unique-combo? concept norm st language)
			   (string-append norm ":" st))))
		(try-combo concept norm (basecat-probe concept language)
			   language)
	      ;; Try using the genls as disambiguators
		(try-combo concept norm (%get concept {@1/2c272{GENLS} hypernym})
			   language)

		;; Try using the specls as disambiguators
		(try-combo concept norm (%get concept {@1/2c273{SPECLS} hyponym})
			   language)
		
		;; Check for synonym disambiguators
		(for-choices (wd (get concept language))
		  (tryif (singleton? (?? language norm language wd))
			 (string-append norm ":" wd)))))))))

(define (compute-individual-dterm concept language norm)
  (let* ((normslot (get norm-map language))
	 (words (get concept language))
	 (isas (%get concept implies))
	 (tryisas (try (intersection isas (%get concept defterms)) isas)))
    (pick-one
     (largest
      (try (filter-choices (wd words) (singleton? (?? language wd)))
	   (for-choices (isa tryisas)
	     (tryif (singleton?
		     (?? language norm
			 implies (?? normslot (get-norm isa language))))
		    (string-append norm " (" (get-norm isa language) ")")))
	   (for-choices (wd words)
	     (for-choices (isa tryisas)
	       (tryif (singleton?
		       (?? language wd
			   implies (?? normslot (get-norm isa language))))
		      (string-append norm
				     " (" (get-norm isa language) ")")))))))))

(define (compute-location-dterm concept language norm)
  (let* ((normslot (get norm-map language))
	 (words (get concept language))
	 (isas (%get concept implies))
	 (tryisas (try (intersection isas (%get concept defterms)) isas))
	 (country (get concept 'country))
	 (partof (choice country (%get concept partof))))
    ;; (message "Computing location dterm for " concept)
    (pick-one
     (smallest
      (try (for-choices (cterm (get (get country '%norm) language))
	     (tryif (singleton? (?? language norm partof* (?? normslot cterm)))
		    (string-append norm ", " cterm)))
	   (for-choices (cterm (get (get partof '%norm) language))
	     (tryif (singleton? (?? language norm partof* (?? normslot cterm)))
		    (string-append norm ", " cterm)))
	   (for-choices (cterm (get (get partof '%norm) language))
	     (for-choices (isaterm (get (get isas '%norm) language))
	       (tryif (singleton?
		       (?? language norm partof* (?? normslot cterm)
			   implies (?? normslot isaterm)))
		      (string-append norm ", " cterm " (" isaterm ")"))))
	   (filter-choices (wd words) (singleton? (?? language wd))))
      length))))


(define logit comment)
;; (define logit message)

(define (compute-dterm-prefetch! concepts language)
  (logit "Prefetching slotids")
  (prefetch-oids! (choice language (get norm-map language)))
  (logit "Prefetching concepts")
  (prefetch-oids! concepts)
  (logit "Separating concepts")
  (let* ((individuals (pick concepts 'type 'individual))
	 (locations (pick individuals 'sensecat 'noun.location))
	 (others (difference concepts individuals)))
    ;; Now get possible disambiguators
    (logit "Prefetching basic concepts")
    (prefetch-keys! (cons 'type 'basic))
    (logit "Prefetching basic related concepts")
    (prefetch-keys! (cons {@1/2c272{GENLS} @1/2c27c{SPECLS*}} others))
    (logit "Prefetching possible disambiguators")
    (prefetch-oids! (choice (?? 'type 'basic specls* others)
			    (%get individuals implies)
			    (%get locations 'country)
			    (%get locations partof)
			    (%get others '{@1/2c272{GENLS} hypernym})
			    (%get others '{@1/2c273{SPECLS} hyponym})))
    (let ((langid (get language 'key))
	  (normslot (get norm-map language))
	  (senseterms (sensecat-probe concepts language)))
      (logit "Prefetching ambiguous sensecat fields")
      (prefetch-keys! (cons normslot senseterms))
      (logit "Computing ambiguous concept terms")
      (let ((keys (cons (choice language normslot) 
			(if (eq? language english)
			    (choice (get concepts 'words)
				    (get (get concepts '%words) langid))
			    (get (get concepts '%words) langid)))))
	(logit "Prefetching " (choice-size keys) " ambiguous concept terms")
	(prefetch-keys! keys))
      (logit "Computing related disambiguators")
      (let* ((isas (%get individuals implies))
	     (partofs (%get locations '{country @1/2c274{PARTOF}}))
	     (genl (choice (?? normslot senseterms)
			   (?? 'type 'basic specls* others)))
	     (specl (%get others '{hyponym @1/2c273{SPECLS}}))
	     (disambigs (choice concepts genl isas partofs)))
	(logit "Prefetchinga related disambiguators")
	(prefetch-oids! disambigs)
	(logit "Computing related disambiguator norms")
	(let* ((normterms
		(for-choices (f others) (get (get f '%norm) language)))
	       (isaterms
		(for-choices (f isas) (get (get f '%norm) language)))
	       (partofterms
		(for-choices (f partofs) (get (get f '%norm) language)))
	       (genlterms
		(for-choices (f genl) (get (get f '%norm) language)))
	       (speclterms
		(for-choices (f specl) (get (get f '%norm) language)))
	       (disterms (choice isaterms partofterms genlterms)))
	  (logit "Prefetching possible ambiguous refs")
	  (prefetch-keys! (cons normslot disterms))
	  (let ((dframes (choice (?? normslot disterms)
				 (?? language normterms))))
	    (logit "Prefetching possible conflict frames")
	    (prefetch-oids! dframes)
	    (logit "Prefetching keys for conflict resolution")
	    (prefetch-keys!
	     (choice (cons implies (?? normslot isaterms))
		     (cons partof* (?? normslot partofterms))
		     (cons (choice @1/2c27c{SPECLS*} @1/2c272{GENLS})
			   (choice
			    (?? normslot (choice genlterms speclterms))
			    (?? language normterms))))))
	  (logit "Done with prefetch"))))))

;;; Crude DTERM determination

(define (crude-dterm f language (hint #f))
  (string-append
   (pick-one (try (tryif hint hint)
		  (get-norm f language)
		  (get-norm (get f genls) language)
		  (pick-one (get-norm (get f genls) language))
		  (pick-one (get-norm (get (get f genls) genls) language))))
   " ("
   (cond ((test f 'fips-code)
	  (printout "FIPS-CODE=" (get f 'fips-code)))
	 ((test f 'ufi)
	  (printout "UFI=" (get f 'ufi)))
	 (else (oid->string f)))
   ")"))

;;; Simply getting a term

(define (get-term concept (language  @1/2c1c7"English"))
  (try (tryif (test concept 'type 'verb)
	      (try-gerund concept language))
       (inner-get-term concept language)))

(define (try-gerund concept language)
  (get-gerund (inner-get-term concept language) language))

(define (inner-get-term concept language)
  (try (get concept 'normative)
       (get (get concept '%norm) language)
       (if (and (eq? language  @1/2c1c7"English") (test concept 'ranked))
	   (first (get concept 'ranked))
	   (pick-one
	    (try  (get concept 'canonical)
		  (smallest
		   (filter-choices (term (get concept language))
		     (identical? concept (parse-dterm term language)))
		   length)
		  (smallest (get concept 'qnames) length)
		  (smallest
		   (filter-choices (term (get concept 'names))
		     (identical? concept (parse-dterm term language)))
		   length)
		  (smallest (get concept language) length)
		  (smallest (get concept 'names) length)
		  (if (eq? language  @1/2c1c7"English") 
		      (stringout "??" concept)
		      (let ((english-term (get-term concept  @1/2c1c7"English")))
			(if (exists? english-term)
			    (append "$" english-term)
			    (stringout "??" concept)))))))))

(define (get-unique-term concept language)
  (try (smallest (filter-choices (term (get concept language))
		   (singleton? (parse-dterm term language)))
		 length)
       (smallest (pick-one (filter-choices (term (get concept 'names))
			     (singleton? (parse-dterm term language))))
		 length)))
