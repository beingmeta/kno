;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'gnosys/refdb)

;;; The GNOSYS/REFDB module provides a database for controlled vocabulary
;;;  terms (refs) from various contexts.  It basically creates a frame
;;;  for each term in a particular context (typically a web site) and ensures
;;;  that there is only ref (frame) for any such pair.

(define version "$Id$")

(use-module 'gnosys)

(module-export! '{getref testref refpool refindex})

(define refpool #f)
(define refindex #f)
(define refdb #f)
(define normalizers (make-hashtable))

(define (makeref x y z)
  (error "REFDB not configured"))

(set+! %volatile 'makeref)

(define (makeref-handler x y z)
  (error "REFDB not configured"))

(define refdb-config
  (slambda (var (val))
    (cond ((not (bound? val)) refdb)
	  ((equal? refdb val))
	  (else (when refdb (warning "REFDB already set to " refdb))
		(message "Setting REF db to " val)
		(set! refpool (use-pool val))
		(set! refindex (open-index val))
		(set! refdb val)
		(if (position #\@ val)
		    (set! makeref-handler (dtproc 'makeref refdb))
		    (set! makeref-handler makeref-local))))))
(config-def! 'refdb refdb-config)

;;; Getting canonical references

(define (makeref x y z)
  (makeref-handler x y z))
(define (getref site field string)
  (try (find-frames refindex 'site site 'field field 'value string)
       (find-frames refindex 'site site 'field field 'value (downcase string))
       (makeref site field string)))
(define (testref site field string)
  (try (find-frames refindex 'site site 'field field 'value string)
       (find-frames refindex
	 'site site 'field field 'value (downcase string))))

(define makeref-local
  (slambda (site field string)
    (try (find-frames refindex 'site site 'field field 'value string)
	 (let ((f (frame-create refpool
		    'type 'ref
		    'field field
		    'site site
		    'value string
		    '%id (stringout field "=" string "@"
				    (try (get site 'hostname)
					 (get site '%id))))))
	   (index-frame refindex f '{type field site value})
	   (index-frame refindex f 'value (downcase (pickstrings string)))
	   f))))
;(define makeref makeref-local)
