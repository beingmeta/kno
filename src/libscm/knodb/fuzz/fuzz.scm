;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/fuzz)

(use-module '{varconfig texttools logger})
(use-module '{knodb/fuzz/strings knodb/fuzz/phrases knodb/fuzz/terms knodb/fuzz/text})

(module-export! '{knodb/fuzz})

(define-init fuzz-handlers
  `#[term ,(fcn/alias fuzz-terms)
     terms ,(fcn/alias fuzz-terms)
     phrase ,(fcn/alias fuzz-phrases)
     phrases ,(fcn/alias fuzz-phrases)
     string ,(fcn/alias fuzz-strings)
     strings ,(fcn/alias fuzz-strings)
     text ,(fcn/alias fuzz-text)])

(define-init fuzzfns-table (make-hashtable))

(defambda (knodb/fuzz values fuzzfns (opts #f))
  (for-choices (fuzzfn fuzzfns)
    (cond ((symbol? fuzzfn)
	   (cond ((test fuzz-handlers fuzzfn) ((get fuzz-handlers fuzzfn) values opts))
		 (else (logerr |BadFuzzFn| fuzzfn))))
	  ((applicable? fuzzfn) (fuzzfn values opts))
	  ((and (pair? fuzzfn) (symbol? (car fuzzfn)) (test fuzz-handlers (car fuzzfn)))
	   ((get fuzz-handlers (car fuzzfn)) values (cdr fuzzfn)))
	  ((and (pair? fuzzfn) (applicable? (car fuzzfn)))
	   ((car fuzzfn) values (cdr fuzzfn)))
	  (else (logerr |BadFuzzFn| fuzzfn)))))


