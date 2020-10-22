;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'knodb/shadow)

(use-module '{reflection ezrecords logger logctl})

(module-export! '{defshadow! shadow/get shadow/probe})

(define %loglevel %warn%)

(define-init shadows (make-hashtable))

(defrecord (shadow)
  slotid generator table (pool #f) (lock (make-mutex)))

(define (defshadow! slotid generator (pool #f))
  (unless (and (applicable? generator)
	       (= (procedure-arity generator) 1))
    (irritant generator |InvalidShadowGenerator|
      "Not a function of one argument")
    (let ((table (try (if pool
			  (car (get shadows (cons slotid pool)))
			  (car (get shadows slotid))))))
      (when (fail? table)
	(set! table (make-hashtable))
	(if pool
	    (use-adjunct table slotid pool)
	    (use-adjunct table slotid)))
      (store! shadows (if pool (cons slotid pool) slotid) 
	(cons-shadow slotid generator table pool)))))

(define (shadow/get obj slotid)
  (try (get obj slotid)
       (let ((shadow (try (get shadows (cons slotid (getpool obj)))
			  (get shadows slotid))))
	 (if (fail? shadow) 
	     (irritant slotid |NoShadow|
	       "The slotid " slotid " isn't a declared shadow slot")
	     (let ((result ((shadow-generator shadow) obj)))
	       (with-lock (shadow-lock shadow)
		 (if (test (shadow-table shadow) obj)
		     (set! result (get (shadow-table shadow) obj))
		     (store! (shadow-table shadow) obj result))
		 result))))))

(define (shadow/probe obj slotid)
  (let ((shadow (try (get shadows (cons slotid (getpool obj)))
		     (get shadows slotid))))
    (if (fail? shadow) #f
	(test (shadow-table shadow) obj))))
