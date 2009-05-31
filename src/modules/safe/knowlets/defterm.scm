(in-module 'knowlets/defterm)

;;; Scheme level knowlet definition, also provides for
;;;  automatic imports from BRICO
(define id "$Id:$")
(define revision "$Revision:$")

(use-module 'texttools)
(use-module '{knowlets knowlets/usebrico})
(use-module '{brico brico/lookup})

(module-export! 'defterm)

(define (defterm term . args)
  (let* ((knowlet default-knowlet)
	 (language (knowlet-language knowlet))
	 (oids (pickoids (elts args)))
	 (dterm (try (kno/probe term knowlet)
		     (singleton (kno/find knowlet 'oid oids))
		     (kno/dterm term knowlet))))
    (when (overlaps? kno/logging '{defterm defs})
      (%watch "DEFTERM" dterm term oids))
    (iadd! dterm 'dterms term)
    (iadd! dterm language term)
    (add! (knowlet-dterms knowlet) term dterm)
    (until (null? args)
      (if (string? (car args))
	  (begin (kno/add! dterm language (car args))
		 (set! args (cdr args)))
	  (if (oid? (car args))
	      (begin (kno/add! dterm 'oid (car args))
		     (kno/copy-brico-terms! (car args) dterm)
		     (set! args (cdr args)))
	      (begin
		(kno/add! dterm (car args) (cadr args))
		(set! args (cddr args))))))
    dterm))

(define (default-lookup term language (freq 0))
  (try (singleton (?? (get norm-map (tryget language-map language))
		      term))
       (lookup-word term language)))

(define (dump-defterms index
		       (knowlet default-knowlet)
		       (lookup default-lookup)
		       (language default-language))
  (let ((terms {})
	(known-terms
	 (choice->hashset (get (getkeys (knowlet-index knowlet)) 'en)))
	(histogram (make-hashtable))
	(stems (make-hashtable))
	(faux-genls (make-hashtable))
	(outcount 0))
    (do-choices (key (getkeys index))
      (when (eq? (car key) 'terms)
	(set+! terms (cdr key))
	(store! histogram (cdr key) (choice-size (get index key)))
	(add! stems (porter-stem (cdr key)) (cdr key))))
    (do-choices (stem (getkeys stems))
      (when (ambiguous? (get stems stem))
	(let* ((base (pick-one
		      (largest (smallest (get stems stem) length))))
	       (variants (reject (get stems stem) downcase (downcase base))))
	  (when (exists? variants)
	    (add! faux-genls base variants)))))
    (doseq (term (rsorted terms histogram))
      (unless (hashset-get known-terms term)
	(let ((m (lookup term language )))
	  (when #f (or (fail? m)
		       (and (ambiguous? m) (< (choice-size m) 7)))
	    (lineout "(defterm " (write term) ") ;; "
		     (get histogram term) " refs; "
		     (choice-size m) " meanings"))
	  (when (singleton? m)
	    (set! outcount (1+ outcount))
	    (printout "(defterm " (write term) "\t;; "
		      (get histogram term) " refs; "
		      (choice-size m) " meanings")
	    (do-choices m
	      (lineout "\n\t;; "
		       (string-subst (get-short-gloss m @?en) "\n" " "))
	      (printout "\t" m))
	    ;; (printout "\t'EN " (write (get faux-genls term)) "\n")
	    (lineout ")"))
	  (when #f (and (not (singleton? m)) (test faux-genls term))
	    (set! outcount (1+ outcount))
	    (printout "(defterm " (write term) "\t;; "
		      (get histogram term) " refs; "
		      (choice-size m) " meanings")
	    (lineout "\n\t'EN " (write (get faux-genls term)) ")")
	    (lineout ")")))))
    (lineout ";; Output " outcount " entries")))

