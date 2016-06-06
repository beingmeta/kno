;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'knodules/defterm)

;;; Scheme level knodule definition, also provides for
;;;  automatic imports from BRICO

(use-module '{texttools logger})
(use-module '{knodules knodules/usebrico})
(use-module '{brico brico/lookup brico/dterms})

(define-init %loglevel %notice%)

(module-export! 'defterm)

(define (defterm term . args)
  (let* ((knodule default-knodule)
	 (language (knodule-language knodule))
	 (oids (pickoids (elts args)))
	 (dterm (if (oid? term)
		    (if (test term 'knodule) term
			(brico->kno term knodule #t))
		    (try (kno/probe term knodule)
			 (singleton (kno/find knodule 'oid oids))
			 (kno/dterm term knodule))))
	 (term (if (oid? term)
		   (try (get term 'dterm)
			(get-dterm term (get language-map language)))
		   term)))
    (info%watch "DEFTERM" dterm term oids)
    (iadd! dterm 'dterms (pickstrings term))
    (iadd! dterm language (pickstrings term))
    (add! (knodule-dterms knodule) (pickstrings term) dterm)
    (add! (knodule-dterms knodule) (stdcap (pickstrings term)) dterm)
    (until (null? args)
      (if (string? (car args))
	  (begin (kno/add! dterm language (car args))
		 (set! args (cdr args)))
	  (if (oid? (car args))
	      (begin (kno/add! dterm 'oid (car args))
		     (kno/copy-brico-terms! (car args) dterm)
		     (kno/copy-brico-links! (car args) dterm)
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
		       (knodule default-knodule)
		       (lookup default-lookup)
		       (language default-language))
  (let ((terms {})
	(known-terms
	 (choice->hashset (get (getkeys (knodule-index knodule)) 'en)))
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
