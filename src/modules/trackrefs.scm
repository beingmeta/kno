;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'trackrefs)

;;; This code helps debugging prefetching by executing a prefetch function
;;;  and then executing a thunk and tracking OID/background loads during
;;;  its execution.

(define version "$Id$")

(define lastrefs #f)

(module-export!
 '{trackrefs test-prefetch getlastrefs})

(define (getlastrefs) lastrefs)

(define (trackrefs thunk (trackfn #f))
   (let ((preoids (cached-oids))
	 (prekeys (cached-keys)))
     (let ((value (thunk)))
       (let ((loaded-oids (difference (cached-oids) preoids))
	     (loaded-keys (difference (cached-keys) prekeys)))
	 (cond (trackfn (trackfn (qc loaded-oids) (qc loaded-keys)))
	       ((eq? (config 'trackrefs) 'sparse)
		(set! lastrefs (vector (qc loaded-oids) (qc loaded-keys)))
		(message "Fetched " (choice-size loaded-oids) " oids and "
			 (choice-size loaded-keys) " keys"))
	       (else
		(when (exists? loaded-oids)
		  (message "Loaded " (choice-size loaded-oids) " oids: "
			   loaded-oids))
		(when (exists? loaded-keys)
		  (message "Loaded " (choice-size loaded-keys) " keys: "
			   loaded-keys)))))
       value)))

(define (test-prefetch prefetcher proc . args)
  (message "Testing prefetch on" (dolist (arg args) (printout " " arg)))
  (clearcaches)
  (let ((start (elapsed-time)))
    (apply prefetcher args)
    (message "Prefetch took " (- (elapsed-time) start) " seconds"))
  (trackrefs
   (lambda ()
     (let ((start (elapsed-time)))
       (stringout (begin (apply proc args) #f))
       (message "Execution took " (- (elapsed-time) start) " seconds")
       #f))))

