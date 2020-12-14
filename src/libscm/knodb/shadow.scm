;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'knodb/shadow)

(use-module '{kno/reflect ezrecords logger logctl defmacro})

(module-export! '{defshadow shadow/get shadow/probe shadow/ref
		  defshadow!})

(define %loglevel %warn%)

(define-init shadows (make-hashtable))

(defrecord (shadow)
  slotid generator table (pool #f) (lock (make-mutex)))

(define (defshadow slotid generator (pool #f))
  (unless (and (applicable? generator)
	       (= (procedure-arity generator) 1))
    (irritant generator |InvalidShadowGenerator|
      "Not a function of one argument"))
  (let* ((shadow (try (tryif pool (get shadows (cons slotid pool)))
		      (get shadows slotid)))
	 (table (shadow-table shadow)))
    (when (fail? table)
      (set! table (make-hashtable))
      (if pool
	  (use-adjunct table slotid pool)
	  (use-adjunct table slotid)))
    (store! shadows (if pool (cons slotid pool) slotid) 
      (cons-shadow slotid generator table pool))
    table))
(define defshadow! (fcn/alias defshadow))

(define (shadow/get obj slotid)
  (try (get obj slotid)
       (let ((shadow (try (get shadows (cons slotid (getpool obj)))
			  (get shadows slotid))))
	 (if (fail? shadow) 
	     (irritant slotid |NoShadow|
	       "The slotid " slotid " isn't a declared shadow slot")
	     (begin
	       (with-lock (shadow-lock shadow)
		 (unless (test (shadow-table shadow) obj)
		   (store! (shadow-table shadow) obj ((shadow-generator shadow) obj))))
	       (get (shadow-table shadow) obj))))))

(define (shadow/probe obj slotid)
  (let ((shadow (try (get shadows (cons slotid (getpool obj)))
		     (get shadows slotid))))
    (if (fail? shadow) #f
	(test (shadow-table shadow) obj))))

(defmacro (shadow/ref obj slotid)
  `(try (get ,obj ,slotid) (,shadow/get ,obj ,slotid)))

