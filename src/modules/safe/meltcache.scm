;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'meltcache)

(use-module '{reflection ezrecords})

;;; Meltcaches are caches whose values decay selectively, so that
;;;  fast changing values decay more quickly and slower changing values
;;;  decay more slowly.

(define version "$Id$")

(module-export!
 '{meltcache/get
   meltcache/probe meltcache/accumulate
   meltcache/bypass meltcache/bypass+
   melted?})

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

(define (roundup x) (inexact->exact (ceiling x)))

(define (melted? entry)
  (time-earlier? (meltentry-expiration entry)))

(define (meltcache/get cache fcn . args)
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
		     (store! cache (cons prockey args) entry)
		     (meltentry-value entry)))
		 (lambda (value)
		   (let ((entry (new-meltentry (qc value) cached threshold)))
		     (store! cache (cons prockey args) entry)
		     (meltentry-value entry))))
	(meltentry-value cached))))

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
  (if (and (exists? previous) (meltentry? previous))
      (let ((thisspan (difftime (timestamp) (meltentry-creation previous)))
	    (lastspan
	     (difftime (meltentry-expiration previous)
		       (meltentry-creation previous))))
	(if (identical? value (meltentry-value previous))
	    (cons-meltentry
	     (meltentry-value previous)
	     (meltentry-creation previous)
	     (timestamp+ (roundup (quotient (+ thisspan lastspan) 2))))
	    (cons-meltentry value (timestamp)
			    (timestamp+ (roundup (quotient lastspan 4))))))
      (cons-meltentry value (timestamp)
		      (timestamp+ (or threshold meltcache-threshold)))))

;;; This is a version which accumulates returned values,
;;;  rather than using them directly.
(define (accumulate-meltentry value previous (threshold #f))
  (if (and (exists? previous) (meltentry? previous))
      (let ((thisspan (difftime (timestamp) (meltentry-creation previous)))
	    (lastspan
	     (difftime (meltentry-expiration previous)
		       (meltentry-creation previous))))
	;; If there aren't any new values, just extend the meltentry
	(if (empty? (difference value (meltentry-value previous)))
	    (cons-meltentry
	     (meltentry-value previous)
	     (meltentry-creation previous)
	     (timestamp+ (roundup (quotient (+ thisspan lastspan) 2))))
	    ;; Otherwise, add the new value(s) to the previous values
	    (cons-meltentry (choice value (meltentry-value previous))
			    (timestamp)
			    (timestamp+ (roundup (quotient lastspan 4))))))
      (cons-meltentry value (timestamp)
		      (timestamp+ (or threshold meltcache-threshold)))))

;; This is called when calling the inner function returned an error.
;;  In this case, we use the last value and set an expiration
;;  which averages the default threshold and the last span
;;  for the entry.
(define (error-meltentry error previous threshold)
  (if (fail? previous)
      (cons-meltentry {} (timestamp)
		      (timestamp+ (or threshold meltcache-threshold))
		      error)
      (let ((lastspan 
	     (inexact->exact
	      (ceiling
	       (difftime (meltentry-expiration previous)
			 (meltentry-creation previous))))))
	(cons-meltentry
	 (meltentry-value previous) (meltentry-creation previous)
	 (timestamp+ (quotient (+ lastspan meltcache-threshold) 2))
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
		     (store! cache (cons prockey args) entry)
		     (meltentry-value entry)))
		 (lambda (value)
		   (let ((entry (accumulate-meltentry
				 (qc value) cached threshold)))
		     (store! cache (cons prockey args) entry)
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
		 (store! cache (cons prockey args) entry)
		 (meltentry-value entry)))
	     (lambda (value)
	       (let ((entry (new-meltentry (qc value) cached threshold)))
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
		     (store! cache (cons prockey args) entry)
		     (meltentry-value entry)))
		 (lambda (value)
		   (let ((entry (meltcache-accumulate-entry
				 (qc value) (qc cached) threshold)))
		     (store! cache (cons prockey args) entry)
		     (meltentry-value entry))))
	(meltentry-value cached))))

;;; Configuring meltcaches

(define meltcache-threshold-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) meltcache-threshold)
	  ((equal? meltcache-threshold val))
	  ((pair? val)
	   (if (and (or (symbol? (car val)) (applicable? (car val)))
		    (fixnum? (cdr val)))
	       (store! meltcache-threshold-table (car val) (cdr val))
	       (error 'typeerror 'meltcache-threshold-config
		      "Not a valid meltcache config" val)))
	  ((fixnum? val)
	   (set! meltcache-threshold val))
	  ((or (symbol? val) (applicable? val))
	   (get meltcache-threshold-table val))
	  (else (error 'typeerror 'meltcache-threshold-config
		       "Not a valid meltcache config" val)))))
(config-def! 'meltcache meltcache-threshold-config)


