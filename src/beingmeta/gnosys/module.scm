;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'gnosys)

(define version "$Id$")

;;; The GNOSYS module is the core of beingmeta's metadata framework.

(use-module '{brico texttools tagger})

(define default-language english)

(module-export! 'default-language)

;; This pool and index contains the GNOSYS slotids.

(use-pool (get-component "gnosys"))
(use-index (get-component "gnosys"))

;;; This defines the tagger used for text analysis.
;;;  It defaults to tagging locally, but you can specify
;;;  a remote parse server.

(define tagger-host #f)
(define tagger tagtext)

(module-export! 'tagger)

(define tagger-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) tagger-host)
	  ((equal? tagger-host val))
	  (else (message "Setting TAGGER to access " val)
		(let ((proc (dtproc 'tagger val)))
		  (proc "This is a simple example natural language sentence")
		  (set! tagger-host val)
		  (set! tagger val))))))
(config-def! 'tagger tagger-config)

;;; This controls tracing of document analysis (for debugging)

(define trace-analysis #t)

(define trace-analysis-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) trace-analysis)
	  ((equal? trace-analysis val))
	  (else (message "Setting analysis trace level to " val)
		(set! trace-analysis val)))))
(config-def! 'trace-analysis trace-analysis-config)

(module-export! 'trace-analysis)