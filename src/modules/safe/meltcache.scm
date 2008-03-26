;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'meltcache)

(use-module '{reflection ezrecords logger fifo})

;;; Meltcaches are caches whose values decay selectively, so that
;;;  fast changing values decay more quickly and slower changing values
;;;  decay more slowly.

(define version "$Id$")

(define %loglevel 4)

(module-export!
 '{meltcache/get
   meltcache/entry
   meltcache/probe meltcache/accumulate
   meltcache/bypass meltcache/bypass+
   melted? meltentry? meltentry/ttl
   cons-meltentry meltentry-value meltvalue})

(define (now) (gmtimestamp 'seconds))
(define (now+ delta) (timestamp+ (gmtimestamp 'seconds) delta))

;;;; Implementation

;; Default threshold in seconds: 15 minutes
(define meltcache-threshold (* 15 60))
(define meltcache-threshold-table (make-hashtable))
;; Good value for debugging
;; (define meltcache-threshold 2)

; (define (set-meltcache-threshold! fcn value)
;   (store! meltcache-threshold-table fcn value))
; (module-export! 'set-meltcache-threshold!)

;; A meltcache entry knows when it will expire and when it was created.
;; Meltcache entries are immutable
(defrecord meltentry value creation expiration (err #f))

#|
(module-export! '{meltentry-creation meltentry-expiration meltentry-err})
|#

(define (roundup x) (inexact->exact (ceiling x)))

(define (melted? entry)
  (time-earlier? (meltentry-expiration entry)))
(define (meltentry/ttl entry)
  (difftime (meltentry-expiration entry) (gmtimestamp 'seconds)))
(define (meltvalue v) (if (meltentry? v) (meltentry-value v) v))

(define (meltcache/get cache fcn . args)
  (let* ((procname (procedure-name fcn))
	 (prockey (if (index? cache) procname fcn))
	 (threshold (try (get meltcache-threshold-table fcn)
			 (get meltcache-threshold-table procname)
			 #f))
	 (cached (try (get cache (cons prockey args)) #f)))
    (logdebug "Call to " fcn " on args " args ": "
	      (if cached (if (melted? cached) "cached but melted" "cached and valid")
		  "not cached"))
    (if (or (not cached) (melted? cached))
	(onerror (apply fcn args)
		 (lambda (value)
		   (let ((entry (error-meltentry (qc value) cached threshold)))
		     (if (fail? entry)
			 (logdebug "Not caching error result from " fcn " on args " args)
			 (logdebug "Caching error result from " fcn " on args " args))
		     (onerror (store! cache (cons prockey args) entry)
			      (lambda (ex) (warning "Can't cache value for " prockey " in " cache
						    ": " ex)))
		     (meltentry-value entry)))
		 (lambda (value)
		   (let ((entry (new-meltentry (qc value) cached threshold)))
		     (if (fail? entry)
			 (logdebug "Not caching routine result from " fcn " on args " args)
			 (logdebug "Caching routine result from " fcn " on args " args))
		     (onerror (store! cache (cons prockey args) entry)
			      (lambda (ex) (warning "Can't cache value for " prockey " in " cache
						    ": " ex)))
		     (meltentry-value entry))))
	(meltentry-value cached))))

(define (meltcache/entry cache fcn . args)
  (let* ((procname (procedure-name fcn))
	 (prockey (if (index? cache) procname fcn))
	 (threshold (try (get meltcache-threshold-table fcn)
			 (get meltcache-threshold-table procname)
			 #f))
	 (cached (and cache (try (get cache (cons prockey args)) #f))))
    (if (or (not cached) (melted? cached))
	(onerror (apply fcn args)
		 (lambda (value)
		   (let ((entry (error-meltentry (qc value) cached threshold)))
		     (when cache (store! cache (cons prockey args) entry))
		     entry))
		 (lambda (value)
		   (let ((entry (new-meltentry (qc value) cached threshold)))
		     (when cache (store! cache (cons prockey args) entry))
		     entry)))
	cached)))

(define (meltcache/probe cache fcn . args)
  (let* ((procname (procedure-name fcn))
	 (prockey (if (index? cache) procname fcn))
	 (cached (get cache (cons prockey args))))
    (if (or (fail? cached) (melted? cached))
	(fail)
	(meltentry-value cached))))

;;; Generating new meltentries

;; This is the core of the algorithm.  If a cache entry has expired, we
;; compute it again.  If it hasn't changed, we reset the expiration date
;; to be the average of the previous span and how long the value has survived.
;; If it has changed, we set the expiration date to be half of the previous
;; span.
(define (new-meltentry value previous (threshold #f))
  (if (and (exists? value) (meltentry? value))
      value
      (if (and (exists? previous) (meltentry? previous))
	  (let ((thisspan (difftime (now) (meltentry-creation previous)))
		(lastspan
		 (difftime (meltentry-expiration previous)
			   (meltentry-creation previous))))
	    (if (identical? value (meltentry-value previous))
		(cons-meltentry
		 (meltentry-value previous)
		 (meltentry-creation previous)
		 (now+ (roundup (/ (+ thisspan lastspan) 2))))
		(cons-meltentry value (now)
				(now+ (roundup (/ lastspan 4))))))
	  (let ((waitfor (if threshold
			     (if (applicable? threshold) (threshold (qc value))
				 threshold)
			     meltcache-threshold)))
	    (cons-meltentry value (now) (now+ waitfor))))))

;;; This is a version which accumulates returned values,
;;;  rather than using them directly.
(define (accumulate-meltentry value previous (threshold #f))
  (if (and (exists? value) (meltentry? value))
      value
      (if (and (exists? previous) (meltentry? previous))
	  (let ((thisspan (difftime (now) (meltentry-creation previous)))
		(lastspan
		 (difftime (meltentry-expiration previous)
			   (meltentry-creation previous))))
	    ;; If there aren't any new values, just extend the meltentry
	    (if (empty? (difference value (meltentry-value previous)))
		(cons-meltentry
		 (meltentry-value previous)
		 (meltentry-creation previous)
		 (now+ (roundup (/ (+ thisspan lastspan) 2))))
		;; Otherwise, add the new value(s) to the previous values
		(cons-meltentry (choice value (meltentry-value previous))
				(now)
				(now+ (roundup (/ lastspan 4))))))
	  (let ((waitfor (if threshold
			     (if (applicable? threshold) (threshold (qc value))
				 threshold)
			     meltcache-threshold)))
	    (cons-meltentry value (now) (now+ waitfor))))))

;; This is called when calling the inner function returned an error.
;;  In this case, we use the last value and set an expiration
;;  which averages the default threshold and the last span
;;  for the entry.
(define (error-meltentry error previous threshold)
  (if (fail? previous)
      (cons-meltentry {} (now)
		      (now+ (or threshold meltcache-threshold))
		      error)
      (let ((lastspan 
	     (inexact->exact
	      (ceiling
	       (difftime (meltentry-expiration previous)
			 (meltentry-creation previous))))))
	(cons-meltentry
	 (meltentry-value previous) (meltentry-creation previous)
	 (now+ (/ (+ lastspan meltcache-threshold) 2))
	 error))))

;;; Variant meltcache functions

(define (meltcache/accumulate cache fcn . args)
  (let* ((procname (procedure-name fcn))
	 (prockey (if (index? cache) procname fcn))
	 (threshold (try (get meltcache-threshold-table fcn)
			 (get meltcache-threshold-table procname)
			 #f))
	 (cached (try (get cache (cons prockey args)) #f)))
    (if (or (not cached) (melted? cached))
	(onerror (apply fcn args)
		 (lambda (value)
		   (let ((entry (error-meltentry (qc value) cached threshold)))
		     (onerror (store! cache (cons prockey args) entry)
			      (lambda (ex) (warning "Can't cache value for " prockey " in " cache
						    ": " ex)))
		     (meltentry-value entry)))
		 (lambda (value)
		   (let ((entry (accumulate-meltentry
				 (qc value) cached threshold)))
		     (onerror (store! cache (cons prockey args) entry)
			      (lambda (ex) (warning "Can't cache value for " prockey " in " cache
						    ": " ex)))
		     (meltentry-value entry))))
	(meltentry-value cached))))

(define (meltcache/bypass cache fcn . args)
  (let* ((procname (procedure-name fcn))
	 (prockey (if (index? cache) procname fcn))
	 (threshold (try (get meltcache-threshold-table fcn)
			 (get meltcache-threshold-table procname)
			 #f))
	 (cached (try (get cache (cons prockey args)) #f)))
    (onerror (apply fcn args)
	     (lambda (value)
	       (let ((entry (error-meltentry (qc value) cached threshold)))
		 (onerror (store! cache (cons prockey args) entry)
			  (lambda (ex) (warning "Can't cache value for " prockey " in " cache
						    ": " ex)))
		 (meltentry-value entry)))
	     (lambda (value)
	       (let ((entry (new-meltentry (qc value) cached threshold)))
		 (onerror (store! cache (cons prockey args) entry)
			  (lambda (ex) (warning "Can't cache value for " prockey " in " cache
						": " ex)))
		 (store! cache (cons prockey args) entry)
		 (meltentry-value entry))))))

(define (meltcache/bypass+ cache fcn . args)
  (let* ((procname (procedure-name fcn))
	 (prockey (if (index? cache) procname fcn))
	 (threshold (try (get meltcache-threshold-table fcn)
			 (get meltcache-threshold-table procname)
			 #f))
	 (cached (try (get cache (cons prockey args)) #f)))
    (if (or (fail? cached) (melted? cached))
	(onerror (apply fcn args)
		 (lambda (value)
		   (let ((entry (meltcache-errentry (qc value) (qc cached)
						    threshold)))
		     (onerror (store! cache (cons prockey args) entry)
			      (lambda (ex) (warning "Can't cache value for " prockey " in " cache
						    ": " ex)))
		     (meltentry-value entry)))
		 (lambda (value)
		   (let ((entry (meltcache-accumulate-entry
				 (qc value) (qc cached) threshold)))
		     (onerror (store! cache (cons prockey args) entry)
			      (lambda (ex) (warning "Can't cache value for " prockey " in " cache
						    ": " ex)))
		     (meltentry-value entry))))
	(meltentry-value cached))))

;;; Meltcaches and FIFOs

(define fifo-threads (make-hashtable))

(define (meltcachedaemon fifo i)
  (let ((e (fifo-pop fifo)))
    (while (exists? e) (apply meltcache/entry e))))

(define (meltcache-fifo (nthreads 4) (ntasks 64))
  (let ((fifo (make-fifo ntasks)))
    (dotimes (i nthreads)
      (add! fifo-threads fifo (threadcall meltcachedaemon fifo i)))
    fifo))

(define (mcq/get cache fcn . args)
  (let* ((procname (procedure-name fcn))
	 (prockey (if (index? cache) procname fcn))
	 (threshold (try (get meltcache-threshold-table fcn)
			 (get meltcache-threshold-table procname)
			 #f))
	 (cached (try (get cache (cons prockey args)) #f)))
    (logdebug "Call to " fcn " on args " args ": "
	      (if cached (if (melted? cached) "cached but melted" "cached and valid")
		  "not cached"))
    (if (not cached)
	(onerror (apply fcn args)
		 (lambda (value)
		   (let ((entry (error-meltentry (qc value) cached threshold)))
		     (if (fail? entry)
			 (logdebug "Not caching error result from " fcn " on args " args)
			 (logdebug "Caching error result from " fcn " on args " args))
		     (onerror (store! cache (cons prockey args) entry)
			      (lambda (ex) (warning "Can't cache value for " prockey " in " cache
						    ": " ex)))
		     (meltentry-value entry)))
		 (lambda (value)
		   (let ((entry (new-meltentry (qc value) cached threshold)))
		     (if (fail? entry)
			 (logdebug "Not caching routine result from " fcn " on args " args)
			 (logdebug "Caching routine result from " fcn " on args " args))
		     (onerror (store! cache (cons prockey args) entry)
			      (lambda (ex) (warning "Can't cache value for " prockey " in " cache
						    ": " ex)))
		     (meltentry-value entry))))
	(if (melted? cached)
	    (begin (fifo-push fifo (cons* cache fcn args))
		   (meltentry-value cached))
	    (meltentry-value cached)))))

(module-export! '{meltcache-fifo mcq/get})

;;; Configuring meltcaches

(define meltcache-threshold-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) meltcache-threshold)
	  ((equal? meltcache-threshold val))
	  ((and (pair? val) (or (symbol? (car val)) (applicable? (car val)))
		(fixnum? (cdr val)))
	   (lognotice "Setting meltcache threshold for " (car val)
		      " to " (cdr val) " seconds")
	   (store! meltcache-threshold-table (car val) (cdr val)))
	  ((and (pair? val) (or (symbol? (car val)) (applicable? (car val)))
		(applicable? (cdr val)))
	   (lognotice "Setting meltcache threshold function for " (car val)
		      " to " (cdr  val))
	   (store! meltcache-threshold-table (car val) (cdr val)))
	  ((pair? val)
	   (error 'typeerror 'meltcache-threshold-config
		  "Not a valid meltcache config" val))
	  ((fixnum? val)
	   (lognotice "Setting default meltcache threshold to " val " seconds")
	   (set! meltcache-threshold val))
	  ((or (symbol? val) (applicable? val))
	   (get meltcache-threshold-table val))
	  (else (error 'typeerror 'meltcache-threshold-config
		       "Not a valid meltcache config" val)))))
(config-def! 'meltcache meltcache-threshold-config)

