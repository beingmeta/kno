;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'signature)

(use-module '{fdweb texttools packetfns})
(use-module '{varconfig logger})
(define %used_modules 'varconfig)

(module-export! '{sig/make sig/check sig/check/})

(define-init %loglevel %notice%)
(varconfig! sbooks:signature:loglevel %loglevel)
;;(set! %loglevel %debug%)

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
    ;; debug%watch
    (info%watch (hmac-sha1 text key) "SIG/MAKE" text key args)))

(defambda (sig/check sig key . args)
  (let ((text (apply makesigtext args)))
    (debug%watch "SIG/CHECK" sig key text (hmac-sha1 text key) args)
    (when (string? sig) (set! sig (base16->packet sig)))
    (and sig (or (equal? sig (hmac-sha1 text key))
		 (begin (logwarn |SIG/CHECK/Failed|
			  " with text " (write text)
			  "\n\tsigned with " key
			  "\n\tyielding " (hmac-sha1 text key)
			  "\n\trather than " sig)
		   #f)))))

(defambda (sig/check/ sigarg key condense . args)
  (let* ((text (apply makesigtext args))
	 (hash (hmac-sha1 text key))
	 (usesig (if condense (packet/condense hash condense) hash))
	 (sig (if (string? sigarg) (base16->packet sigarg) sigarg)))
    (debug%watch "SIG/CHECK/"
      sigarg sig usesig condense hash text args)
    (and sig (or (equal? sig usesig) (equal? sig hash)
		 (begin (warn%watch (hmac-sha1 text key)
				    "SIG/CHECK//FAILED" sig key text)
		   #f)))))

