(in-module 'brico/dterms)

(module-export! '{find-dterm get-dterm find-dterm-prefetch!})

(use-module '{brico brico/lookup})

(define sensecathints
  (file->dtype (get-component "sensecathints.table")))

(define (get-dterm concept (language default-language) (norm) (tryhard #f))
  (default! norm (get-norm concept language))
  (try (cachecall find-dterm concept language norm)
       (try-choices (alt (difference (get concept language) norm))
	 (cachecall find-dterm concept language alt))))

;;; Finding dterms

(define (find-dterm concept (language default-language) (norm))
  (default! norm (get-norm concept language))
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
	(normslot (get norm-map language)))
    (try (try-choices (term (get sensecathints (cons sensecat language)))
	   (tryif (singleton? (?? language norm @?genls* (?? normslot term)))
		  (string-append norm ":" term)))
	 (try-choices (d (difference (get concept language) norm))
	   (tryif (singleton? (?? language norm language d))
		  (string-append norm ":"  d)))
	 (try-choices (d (difference (get (get concept @?defterms) @?en_norm)
				     norm))
	   (tryif (singleton? (?? language norm @?defterms (?? normslot d)))
		  (string-append norm " (*"  d ")"))))))

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
