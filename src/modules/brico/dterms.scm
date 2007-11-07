(in-module 'brico/dterms)

(module-export! '{find-dterm get-dterm displayterm find-dterm-prefetch!})

(use-module '{brico brico/lookup morph/en})

(define sensecathints
  (file->dtype (get-component "sensecathints.table")))

(define (get-dterm concept (language default-language) (norm) (tryhard #f))
  (default! norm (get-norm concept language))
  (try (cachecall find-dterm concept language norm)
       (try-choices (alt (difference (get concept language) norm))
	 (cachecall find-dterm concept language alt))))

;;; Finding dterms

(define (termnorm concept language)
  (if (and (test concept 'type 'verb) (eq? language @?en))
      (try (gerund (get-norm concept language)) (get-norm concept language))
      (get-norm concept language)))

(define (find-dterm concept (language default-language) (norm))
  (default! norm (termnorm concept language))
  (if (singleton? (?? language norm)) norm
      (if (test concept 'type 'individual)
	  (if (test concept 'sensecat 'noun.location)
	      (find-location-dterm concept language norm)
	      (find-individual-dterm concept language norm))
	  (find-generic-dterm concept language norm))))

(define (probe-location concept term1 term2 language (isaterm #f))
  (let ((norm (get norm-map language)))
    (if isaterm
	(try (?? norm term1 @?partof* (?? norm term2) @?implies (?? norm isaterm))
	     (?? language term1 @?partof* (?? norm term2) @?implies (?? language isaterm))
	     (?? language term1 @?partof* (?? language term2) @?implies (?? language isaterm)))
	(try (?? norm term1 @?partof* (?? norm term2))
	     (?? language term1 @?partof* (?? norm term2))
	     (?? language term1 @?partof* (?? language term2))))))

(define (find-location-dterm concept language norm)
  (let* ((country (get concept 'country))
	 (country-norm (get-norm country language))
	 (region (get concept 'region))
	 (region-norm (get-norm region language))
	 (isa (get concept @?implies)))
    (try (tryif (identical? concept (probe-location concept norm country-norm language))
		(stringout norm ", " country-norm))
	 (tryif (identical? concept (probe-location concept norm region-norm language))
		(stringout norm ", " region-norm))
	 (try-choices (isa (get concept @?implies))
	   (try-choices (isaterm (get-norm isa language))
	     (tryif (identical? concept (probe-location concept norm region-norm language isaterm))
		    (stringout norm ", " region-norm
			       " (" isaterm ")"))))
	 (tryif (test concept 'fips-code)
		(stringout norm "(FIPS-CODE=" (get concept 'fips-code) ")")))))

(define (probeisa concept norm isaterm language normlang)
  (identical? concept (?? language norm @?implies (?? normlang isaterm))))

(define (find-individual-dterm concept language norm)
  (let* ((isa (get concept @?implies))
	 (defisa (intersection isa (get concept @?defterms)))
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

(define (find-generic-dterm concept language norm)
  (let ((sensecat (get concept 'sensecat))
	(normslot (get norm-map language))
	(meanings (lookup-word norm language)))
    (try (tryif (singleton? (?? language norm)) norm)
	 (try-choices (term (get sensecathints (cons sensecat language)))
	   (tryif (singleton? (intersection meanings (?? @?genls* (?? normslot term))))
		  (string-append norm ":" term)))
	 (try-choices (gn (get-norm (get concept @?genls) language))
	   (tryif (singleton? (intersection meanings (?? @?genls* (?? language gn))))
		  (string-append norm ":"  gn)))
	 (try-choices (gn (get-norm (?? @?specls* concept) language))
	   (tryif (singleton? (intersection meanings (?? @?genls* (?? language gn))))
		  (string-append norm ":"  gn)))
	 (try-choices (d (difference (get concept language) norm))
	   (tryif (singleton? (intersection meanings (?? language d)))
		  (string-append norm "="  d)))
	 (try-choices (d (difference (get (get concept @?defterms) @?en_norm)
				     norm))
	   (tryif (singleton? (intersection meanings (?? @?defterms (?? normslot d))))
		  (string-append norm " (*"  d ")")))
	 (try-choices (n (get-norm concept language))
	   (tryif (not (equal? n norm))
		  (find-generic-dterm concept language n)))
	 (tryif (not (eq? language english))
		(try-choices (term (get sensecathints (cons sensecat english)))
		  (tryif (singleton? (intersection meanings (?? @?genls* (?? english term))))
			 (string-append norm ":en$" term)))
		(try-choices (gn (get-norm (get concept @?genls) english))
		  (tryif (singleton? (intersection meanings (?? @?genls* (?? english gn))))
			 (string-append norm ":en$"  gn)))
		(try-choices (enorm (get-norm concept english))
		  (tryif (singleton? (?? english enorm))
			 (string-append "en$"  enorm)))
		(try-choices (enorm (get-norm concept english))
		  (try-choices (term (get sensecathints (cons sensecat english)))
		    (tryif (singleton? (intersection (?? english enorm)
						     (?? @?genls* (?? english term))))
			   (string-append "en$" enorm ":en$" term))))
		(try-choices (enorm (get-norm concept english))
		  (try-choices (gn (get-norm (get concept @?genls) english))
		    (tryif (singleton? (intersection meanings (?? @?genls* (?? english gn))))
			   (string-append "en$" enorm ":en$"  gn))))
		(try-choices (enorm (get-norm concept english))
		  (try-choices (gn (get-norm (get concept @?genls) english))
		    (tryif (singleton? (intersection meanings (?? @?genls* (?? english gn))))
			   (string-append "en$" enorm ":en$"  gn))))
		(try-choices (enorm (get-norm concept english))
		  (try-choices (gn (get-norm (?? @?specls* concept) english))
		    (tryif (singleton? (intersection (?? @?en enorm) (?? @?genls* (?? english gn))))
			   (string-append "en$" enorm ":$"  gn))))
		(try-choices (d (get concept english))
		  (tryif (singleton? (intersection meanings (?? language d)))
			 (string-append norm "=en$"  d)))))))


;;; Prefetching

(defambda (find-dterm-prefetch! concept (language default-language) (norm))
  (default! norm (get-norm concept language))
  (prefetch-oids! concept)
  (prefetch-oids! (%get concept '{region country @?partof @?implies}))
  (prefetch-keys!
   (cons (choice language (get norm-map language))
	 (choice (get sensecathints (cons (get concept 'sensecat) language))
		 norm
		 (get (%get concept '{region country @?partof @?implies})
		      @?en_norm)))))

;;; Getting display terms

(defambda (displayterm concept language concepts (suffix #f))
  (try (try-choices (norm (get-norm concept language))
	 (tryif (singleton? (intersection (lookup-word norm language) concepts))
		norm))
       (get-dterm concept language)
       (if suffix
	   (string-append (get-norm concept language) " "
			  (if (string? suffix) suffix "(alt)"))
	   (get-norm concept language))))


