;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'trackrefs)

;;; This code helps debugging prefetching by executing a prefetch function
;;;  and then executing a thunk and tracking OID/background loads during
;;;  its execution.

(use-module 'varconfig)

(define lastrefs #f)

(module-export!
 '{trackrefs test-prefetch getlastrefs})

(define stdprefetch {})

(varconfig! stdprefetch stdprefetch #f choice)

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
  (let* ((raw-start (elapsed-time))
	 (raw-result (apply proc args))
	 (raw-end (elapsed-time)))
    (message "Raw call took " (- raw-end raw-start) " seconds")
    (clearcaches)
    (do-choices (prefetch stdprefetch) (prefetch))
    (let ((fetch-start (elapsed-time))
	  (fetch-end #f))
      (apply prefetcher args)
      (set! fetch-end (elapsed-time))
      (message "Prefetch took " (- fetch-end fetch-start) " seconds")
      (trackrefs
       (lambda ()
	 (let* ((run-start (elapsed-time))
		(result (apply proc args))
		(run-end (elapsed-time)))
	   (if (and (bound? result) (bound? raw-result))
	       (unless (identical? result raw-result)
		 (message "Direct and prefetch results differ"))
	       (when (or (bound? result) (bound? raw-result))
		 (message "Direct and prefetch results differ (one is void)")))
	   (message "Execution took " (- run-end run-start) " seconds")
	   (message "direct=" (- raw-end raw-start)
		    " fetch+run=" (+ (- run-end run-start)  (- fetch-end fetch-start)))
	   #f))))))





