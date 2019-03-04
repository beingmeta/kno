;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'brico/xdterms)
;;; Extended dterm extraction, especially for other languages

(use-module '{brico brico/dterms brico/lookup brico/analytics})

(module-export! '{check-dterm find-xdterm get-xdterm})

(define (check-dterm dterm language concept)
  (tryif (identical? concept (brico/ref dterm language))
	 dterm))

(define (get-xdterm concept primary (secondary))
  (default! secondary english)
  (try (get-dterm concept primary)
       (if (eq? primary secondary) (fail)
	   (cachecall find-xdterm concept primary secondary))))

(define (probe-xdterm lang1 term1 lang2 term2)
  (singleton? (?? lang1 term1
		  {@1/4{ALWAYS} @1/2c281{PART-OF*} @1/5{SOMETIMES}}
		  (?? lang2 term2))))

(define (find-xdterm concept primary secondary)
  (let ((d1 (choice (get concept '{country region})
		    (get concept @1/4{ALWAYS})))
	(d2 (get concept @1/5{SOMETIMES}))
	(d3 (get+ concept @1/4{ALWAYS}))
	(d4 (get+ concept @1/5{SOMETIMES}))
	(langid (get secondary 'iso639/1)))
    (try
     (try-choices (norm (get-norm concept secondary))
       (try
	(try-choices (dnorm (get-norm d1 primary))
	  (tryif (probe-xdterm secondary norm primary dnorm)
		 (check-dterm (stringout langid "$" norm " (" dnorm ")")
			      primary concept)))
	(try-choices (dnorm (get d1 primary))
	  (tryif (probe-xdterm secondary norm primary dnorm)
		 (check-dterm (stringout langid "$" norm " (" dnorm ")")
			      primary concept)))
	(try-choices (dnorm (get-norm d2 primary))
	  (tryif (probe-xdterm secondary norm primary dnorm)
		 (check-dterm (stringout langid "$" norm " (" dnorm ")")
			      primary concept)))
	(try-choices (dnorm (get d2 primary))
	  (tryif (probe-xdterm secondary norm primary dnorm)
		 (check-dterm (stringout langid "$" norm " (" dnorm ")")
			      primary concept)))
	(try-choices (dnorm (get-norm d3 primary))
	  (tryif (probe-xdterm secondary norm primary dnorm)
		 (check-dterm (stringout langid "$" norm " (" dnorm ")")
			      primary concept)))
	(try-choices (dnorm (get d3 primary))
	  (tryif (probe-xdterm secondary norm primary dnorm)
		 (check-dterm (stringout langid "$" norm " (" dnorm ")")
			      primary concept)))))
     (try-choices (norm (get concept secondary))
       (try
	(try-choices (dnorm (get-norm d1 primary))
	  (tryif (probe-xdterm secondary norm primary dnorm)
		 (check-dterm (stringout langid "$" norm " (" dnorm ")")
			      primary concept)))
	(try-choices (dnorm (get d1 primary))
	  (tryif (probe-xdterm secondary norm primary dnorm)
		 (check-dterm (stringout langid "$" norm " (" dnorm ")")
			      primary concept)))
	(try-choices (dnorm (get-norm d2 primary))
	  (tryif (probe-xdterm secondary norm primary dnorm)
		 (check-dterm (stringout langid "$" norm " (" dnorm ")")
			      primary concept)))
	(try-choices (dnorm (get d2 primary))
	  (tryif (probe-xdterm secondary norm primary dnorm)
		 (check-dterm (stringout langid "$" norm " (" dnorm ")")
			      primary concept)))
	(try-choices (dnorm (get-norm d3 primary))
	  (tryif (probe-xdterm secondary norm primary dnorm)
		 (check-dterm (stringout langid "$" norm " (" dnorm ")")
			      primary concept)))
	(try-choices (dnorm (get d3 primary))
	  (tryif (probe-xdterm secondary norm primary dnorm)
		 (check-dterm (stringout langid "$" norm " (" dnorm ")")
			      primary concept))))))))



