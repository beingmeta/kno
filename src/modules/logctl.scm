;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'logctl)

;;; Provides module-level logging control together with the LOGGER file
(define version "$Id$")
(define revision "$Revision$")

(use-module 'logger)

(module-export! 'logctl!)

(define (convert-log-arg arg)
  (if (and (pair? arg) (eq? (car arg) 'quote) (pair? (cdr arg))
	      (symbol? (cadr arg)))
      (getloglevel (cadr arg))
      (getloglevel arg)))

(define logctl!
  (macro expr
    (let ((modname (second expr))
	  (loglevel (convert-log-arg (third expr))))
      (if (and (pair? modname) (eq? (car modname) 'quote)
	       (pair? (cdr modname)))
	  (set! modname (cadr modname)))
      (when (fail? loglevel) (error "Bad LOGLEVEL" expr))
      (if (symbol? modname)
	  `(within-module ',modname (define %loglevel ,loglevel))
	  `(within-module ,modname (define %loglevel ,loglevel))))))




