;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'logctl)

;;; Provides module-level logging control together with the LOGGER file
(define version "$Id$")
(define revision "$Revision$")

(use-module 'logger)

(module-export! 'logctl!)

(define levels {})

(define (convert-log-arg arg)
  (if (and (pair? arg) (eq? (car arg) 'quote) (pair? (cdr arg))
	      (symbol? (cadr arg)))
      (getloglevel (cadr arg))
      (getloglevel arg)))

(define (record-loglevel! modname level)
  (let ((entry (pick levels modname)))
    (if (exists? entry)
	(set-cdr! entry level)
	(set+! levels (cons modname level)))))

(define logctl!
  (macro expr
    (let ((modname (second expr))
	  (loglevel (convert-log-arg (third expr))))
      (if (and (pair? modname) (eq? (car modname) 'quote)
	       (pair? (cdr modname)))
	  (set! modname (cadr modname)))
      (when (fail? loglevel) (error "Bad LOGLEVEL" expr))
      (let ((modref (if (symbol? modname) `',modname modname)))
	`(begin (within-module ,modref (define %loglevel ,loglevel))
	   (,record-loglevel! ,modref ,loglevel))))))

(define (logctl-config var (val))
  (if (not (bound? val)) levels
      (if (pair? val)
	  (eval `(within-module ',(car val) (define %loglevel ,(cdr val))))
	  (if (string? val)
	      (let ((split (position #\: val)))
		(if (not position)
		    (error "Invalid LOGCTL spec" val)
		    (let ((module (get-module (string->lisp (slice val 0 split))))
			  (level (getloglevel (string->lisp (slice val (1+ split))))))
		      (eval `(within-module ',module (define %loglevel ,level))))))))))
(config-def! 'logctl logctl-config)



