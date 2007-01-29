(in-module 'meltcache)

(use-module 'reflection)

;;; Meltcaches are caches whose values decay selectively, so that
;;;  fast changing values decay more quickly and slower changing values
;;;  decay more slowly.

(module-export! '{meltcache/get meltcache/probe meltcache/accumulate melted?})

;; Default threshold in seconds: 15 minutes
(define meltcache-threshold (* 15 60))
(define meltcache-threshold-table (make-hashtable))
;; Good value for debugging
;; (define meltcache-threshold 2)

(define meltcache-threshold-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) meltcache-threshold)
	  ((equal? meltcache-threshold val))
	  ((integer? val)
	   (set! meltcache-threshold val))
	  (else (error 'typeerror 'meltcache-threshold-config
		       "Not a valid API key value" val)))))
(config-def! 'meltcache-threshold meltcache-threshold-config)

(define (set-meltcache-threshold! fcn value)
  (store! meltcache-threshold-table fcn value))
(module-export! 'set-meltcache-threshold!)

;; A meltcache entry knows when it will expire and when it was created.
(define meltcache-entry-expiration first)
(define meltcache-entry-creation second)
(define meltcache-entry-value third)

;; This is the core of the algorithm.  If a cache entry has expired, we
;; compute it again.  If it hasn't changed, we reset the expiration date
;; to be the average of the previous span and how long the value has survived.
;; If it has changed, we set the expiration date to be half of the previous
;; span.
(define (meltcache-entry value previous (threshold #f))
  (if (exists? previous)
      (let ((span (inexact->exact
		   (ceiling
		    (difftime (timestamp)
			      (meltcache-entry-creation previous)))))
	    (lastspan
	     (inexact->exact
	      (ceiling
	       (difftime (meltcache-entry-expiration previous)
			 (meltcache-entry-creation previous))))))
	(if (identical? value (meltcache-entry-value previous))
	    (vector (timestamp+ (quotient (+ span lastspan) 2))
		    (meltcache-entry-creation previous)
		    (qc (meltcache-entry-value previous)))
	    (vector (timestamp+ (quotient lastspan 2))
		    (timestamp)
		    (qc value))))
      (vector (timestamp+ (or threshold meltcache-threshold))
	      (timestamp)
	      (qc value))))

;;; This is a version which accumulates returned values,
;;;  rather than using them directly.
(define (meltcache-accumulate-entry value previous (threshold #f))
  (if (exists? previous)
      (let ((span (inexact->exact
		   (ceiling
		    (difftime (timestamp)
			      (meltcache-entry-creation previous)))))
	    (lastspan
	     (inexact->exact
	      (ceiling
	       (difftime (meltcache-entry-expiration previous)
			 (meltcache-entry-creation previous))))))
	(unless (empty? (difference (meltcache-entry-value previous) value))
	  (message "New results for " previous))
	(if (empty? (difference (meltcache-entry-value previous) value))
	    ;; If there aren't any new values, we extend the timestamp
	    (vector (timestamp+ (quotient (+ span lastspan) 2))
		    (meltcache-entry-creation previous)
		    (qc (meltcache-entry-value previous)))
	    ;; If there are, we reduce the last span
	    (vector (timestamp+ (quotient lastspan 2))
		    (timestamp)
		    (qc (meltcache-entry-value previous) value))))
      (vector (timestamp+ (or threshold meltcache-threshold))
	      (timestamp)
	      (qc value))))

;; This is called when calling the inner function returned an error.
;;  In this case, we average the default threshold and the last span
;;  for the entry.
(define (meltcache-errentry error previous threshold)
  (if (fail? previous)
      (vector (timestamp+ (or threshold meltcache-threshold))
	      (timestamp)
	      (qc)
	      error)
      (let ((lastspan 
	     (inexact->exact
	      (ceiling
	       (difftime (meltcache-entry-expiration previous)
			 (meltcache-entry-creation previous))))))
	(vector (timestamp+ (quotient (+ lastspan meltcache-threshold) 2))
		(meltcache-entry-creation previous)
		(qc (meltcache-entry-value previous))
		error))))

(define (melted? entry)
  (time-earlier? (meltcache-entry-expiration entry)))

(define (meltcache/get cache fcn . args)
  (let* ((procname (procedure-name fcn))
	 (prockey (if (index? cache) procname fcn))
	 (threshold (try (get meltcache-threshold-table fcn)
			 (get meltcache-threshold-table procname)
			 #f))
	 (cached (get cache (cons prockey args))))
    (if (or (fail? cached) (melted? cached))
	(onerror (apply fcn args)
		 (lambda (value)
		   (let ((entry (meltcache-errentry (qc value) (qc cached)
						    threshold)))
		     (store! cache (cons prockey args) entry)
		     (meltcache-entry-value entry)))
		 (lambda (value)
		   (let ((entry (meltcache-entry (qc value) (qc cached)
						 threshold)))
		     (store! cache (cons prockey args) entry)
		     (meltcache-entry-value entry))))
	(meltcache-entry-value cached))))
(define (meltcache/probe cache fcn . args)
  (let* ((procname (procedure-name fcn))
	 (prockey (if (index? cache) procname fcn))
	 (cached (get cache (cons prockey args))))
    (if (or (fail? cached) (melted? cached))
	(fail)
	(meltcache-entry-value cached))))

(define (meltcache/accumulate cache fcn . args)
  (let* ((procname (procedure-name fcn))
	 (prockey (if (index? cache) procname fcn))
	 (cached (get cache (cons prockey args)))
	 (threshold (try (get meltcache-threshold-table fcn)
			 (get meltcache-threshold-table procname)
			 #f)))
    (if (or (fail? cached) (melted? cached))
	(onerror (apply fcn args)
		 (lambda (value)
		   (let ((entry (meltcache-errentry (qc value) (qc cached)
						    threshold)))
		     (store! cache (cons prockey args) entry)
		     (meltcache-entry-value entry)))
		 (lambda (value)
		   (let ((entry (meltcache-accumulate-entry
				 (qc value) (qc cached) threshold)))
		     (store! cache (cons prockey args) entry)
		     (meltcache-entry-value entry))))
	(meltcache-entry-value cached))))

