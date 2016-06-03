;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'whocalls)

;;; Code analyzer for FramerD fdscript

(use-module '{reflection codewalker ezrecords})

(module-export!
 '{get-whocalls
   whocalls? whocalls-callers whocalls-callees whocalls-module?})
(module-export! '{getcallers getcallees})
(module-export! '{find-deadwood})

(define sysvars '{%loadstamp %rewrite %volatile %moduleid})

(defrecord WHOCALLS module callers callees)

(define (get-refs arg)
  (tryif (or (compound-procedure? arg) (environment? arg))
	 (let ((refs (make-hashset)))
	   (codewalker (lambda (mod expr bound env)
			 (when (and (symbol? expr)
				    (not (position expr bound)))
			   (hashset-add! refs expr)))
		       arg)
	   (hashset-elts refs))))

(define (compute-whocalls module)
  (let ((callers (make-hashtable))
	(callees (make-hashtable)))
    (do-choices (symbol (module-bindings module))
      (let ((refs (get-refs (get module symbol))))
	(add! callees symbol refs)
	(add! callers  refs symbol)))
    (cons-whocalls module callers callees)))

(define (get-whocalls module (recompute #f))
  (if (whocalls? module) module
      (if (module? module)
	  (cachecall compute-whocalls module)
	  (cachecall compute-whocalls (get-module module)))))

;;; External functions

(define (getcallers arg symbol)
  (get (whocalls-callers (get-whocalls arg)) symbol))
(define (getcallees arg symbol)
  (get (whocalls-callees (get-whocalls arg)) symbol))

;;; Common requests

(define (find-deadwood module (roots #f))
  (let* ((wc (get-whocalls module))
	 (vars (difference (module-bindings (whocalls-module wc)) sysvars))
	 (roots (or roots (module-exports (whocalls-module wc))))
	 (callers (deep-copy (whocalls-callers wc)))
	 (callees (deep-copy (whocalls-callees wc))))
    (let ((allunused {})
	  (unused (difference (reject vars callers) roots)))
      (while (exists? unused)
	(set+! allunused unused)
	(do-choices (var vars) (drop! callers var unused))
	(set! unused (difference (reject vars callers) roots allunused)))
      allunused)))
