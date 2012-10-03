;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2012 beingmeta, inc. All rights reserved

(in-module 'signature)

(use-module '{fdweb texttools})
(use-module '{varconfig logger rulesets ezrecords})

(module-export! '{sig/make sig/check})

(define-init %loglevel %notify!)
(varconfig! sbooks:signature:loglevel %loglevel)

(defambda (makesigtext . args)
  (let ((table (if (odd? (length args)) (deep-copy (car args)) `#[]))
	(params (if (odd? (length args)) (cdr args) args)))
    (do ((scan params (cddr scan)))
	((null? scan))
      (unless (fail? (cadr scan))
	(add! table (car scan) (cadr scan))))
    (stringout
      (doseq (key (lexsorted (getkeys table)) i)
	(if (string? key)
	    (printout (if (> i 0) "&")
		      (uriencode key) "=")
	    (printout (if (> i 0) "&")
	      (if (oid? key)
		  (uriencode (oid->string key))
		  (if (uuid? key)
		      (uuid->string key)
		      (uriencode (lisp->string key))))
	      "="))
	(doseq (v (lexsorted (get table key)) j)
	  (printout (if (> j 0) "&")
	    (uriencode (if (string? v) v
			   (if (oid? v) (oid->string v)
			       (if (uuid? v)
				   (uuid->string v)
				   (lisp->string v)))))))))))

(defambda (sig/make key . args)
  (let ((text (apply makesigtext args)))
    (debug%watch (hmac-sha1 text key) "SIG/MAKE" text key args)))
(defambda (sig/check sig key . args)
  (let ((text (apply makesigtext args)))
    (debug%watch "SIG/CHECK" args)
    (debug%watch "SIG/CHECK" sig key text (hmac-sha1 text key))
    (when (string? sig) (set! sig (base16->packet sig)))
    (and sig (or (equal? sig (hmac-sha1 text key))
		 (begin (warn%watch (hmac-sha1 text key)
				    "SIG/CHECK/FAILED" sig key text)
		   #f)))))
