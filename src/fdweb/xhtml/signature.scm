;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2012 beingmeta, inc. All rights reserved

(in-module 'xhtml/signature)

(use-module '{fdweb texttools})
(use-module '{varconfig logger rulesets ezrecords})

(module-export! '{sig/make sig/check})

(define-init %loglevel %notify!)
;;(define %loglevel %debug!)

(define (makesigtext . args)
  (let ((table (if (odd? (length args)) (deep-copy (car args)) `#[]))
	(params (if (odd? (length args)) (cdr args) args)))
    (do ((scan params (cddr scan)))
	((null? scan))
      (add! table (car scan) (cadr scan)))
    (stringout
      (doseq (key (lexsorted (getkeys table)) i)
	(if (string? key)
	    (printout (if (> i 0) "&")
		      (uriencode key) "=")
	    (printout (if (> i 0) "&")
		      (uriencode (lisp->string key)) "="))
	(doseq (v (lexsorted (get table key)) j)
	  (printout (if (> j 0) " ") v))))))

(define (sig/make key . args)
  (let ((text (apply makesigtext args)))
    (debug%watch (hmac-sha1 text key) "MAKESIG" text key args)))
(define (sig/check sig key . args)
  (let ((text (apply makesigtext args)))
    (debug%watch "CHECKSIG" sig key text (hmac-sha1 text key))
    (when (string? sig) (set! sig (base64->packet sig)))
    (or (equal? sig (hmac-sha1 text key))
	(begin (warn%watch "CHECKSIG/FAILED"
			   sig key text (hmac-sha1 text key))))))




