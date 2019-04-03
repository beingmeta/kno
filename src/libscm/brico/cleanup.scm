;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'brico/cleanup)
;;; Looking up terms in BRICO

;;; This module provides utilities and end-user functions for mapping
;;;  natural language terms into BRICO concepts.

(use-module 'brico)
(use-module '{texttools reflection engine})
(use-module '{varconfig logger})

(module-export! '{brico/cleanup!})

(define (cleanup-value value)
  (cond ((string? value) (trim-spaces value))
	((not (oid? value)) value)
	((not (table? (oid-value value)))
	 (if (and (exists? (getpool value)) (getpool value))
	     (fail)
	     value))
	((test value 'replacement) (get value 'replacement))
	((or (test value '{type status} '{deleted deprecated})
	     (test value '{deleted deprecated replaced}))
	 (fail))
	(else value)))

(define (cleanup-oid! oid (locked #f))
  (do-choices (slotid {(getkeys oid) (get oid 'has)})
    (let* ((v (get oid slotid))
	   (cv (cleanup-value v)))
      (unless (identical? v cv)
	(unless locked (lock-oid! oid))
	(add! oid slotid (difference cv v))
	(drop! oid slotid (difference v cv))))))

(define (brico/cleanup! arg)
  (if (oid? arg)
      (cleanup-oid! arg)
      (if (pool? arg)
	  (engine/run cleanup-oid! (pool-elts arg)
	    [beforefn engine/lockoids])
	  (irritant arg |NotPoolOrOID|))))
