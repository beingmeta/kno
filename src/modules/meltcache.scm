;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'meltcache)

(use-module '{reflection ezrecords logger})

;;; Meltcaches are caches whose values decay selectively, so that
;;;  fast changing values decay more quickly and slower changing values
;;;  decay more slowly.

;;; Basic design:
;;;  A meltcache is an index or hashtable 
;;;     which stores *meltenries* as values and 
;;;     whose keys are function call signatures
;;;  A meltentry has a value, a creation timestamp, an expiration timestamp
;;;   and an indication of whether the value is an error value
;;;  A function call signature is a cons of a procedure and arguments
;;;  Whenever a result is saved, a *lifetime* is determined.  The
;;;   expiration time is _lifetime_ seconds from when the result was first
;;;   saved.

(define-init %loglevel 4)
(define-init trace-values #f)

(module-export!
 '{meltcache/get
   meltcache/update meltcache/force meltcache/info
   meltcache/getentry meltcache/probe meltcache/store
   melted? meltentry? meltentry/ttl meltentry/age meltentry/duration
   cons-meltentry meltentry meltentry-value meltvalue melted-value?
   meltupdate})

(define (now) (gmtimestamp 'seconds))
(define (now+ delta) (timestamp+ (gmtimestamp 'seconds) delta))
(define (now- past) (difftime (gmtimestamp 'seconds) past))

(define (roundup x) (inexact->exact (ceiling x)))
(define (randomish delta)
  "Returns a random number in the delta/2 neighborhood of delta"
  (if (< delta 2) delta
      (+ (quotient delta 2) (random delta))))

;;;; Implementation

;;; A meltcache entry knows when it will expire and when it was created.
;;;  Meltcache entries are immutable
(defrecord meltentry value creation expiration (err #f))
#|(module-export! '{meltentry-creation meltentry-expiration meltentry-err})|#

;;; Utility functions
(define (melted? entry)
  (time-earlier? (meltentry-expiration entry)))
(define (meltentry/ttl entry)
  (difftime (meltentry-expiration entry) (gmtimestamp 'seconds)))
(define (meltentry/age entry)
  (difftime (gmtimestamp 'seconds) (meltentry-creation entry)))
(define (meltentry/duration entry)
  (difftime (meltentry-expiration entry) (meltentry-creation entry)))
(define (meltvalue v) (if (meltentry? v) (meltentry-value v) v))
(define (melted-value? v)
  (if (meltentry? v)
      (time-earlier? (meltentry-expiration v))
      #f))

;; Default threshold in seconds: 15 minutes
(define-init default-meltpoint (* 15 60))
(define-init meltpoint-table (make-hashtable))

;;;; Updating melt entries

;;; This is the core of the algorithm, increasing the expiration
;;;  time for unchanged values and decreasing it when values change.
;;; It also includes special cases for error results which assume
;;;  that errors may be transient.

(define (meltupdate entry fcn args (meltpoint default-meltpoint))
  (let* ((fcnid (procedure-id fcn))
	 (newv (onerror (apply fcn args)
			(lambda (ex) ex)
			(lambda (v) v)))
	 (meltpoint (try (tryif (fail? newv)
				(try (get meltpoint-table (cons fcn 'fail))
				     (get meltpoint-table (cons fcnid 'fail))))
			 (tryif (error? newv)
				(try (get meltpoint-table (cons fcn 'error))
				     (get meltpoint-table (cons fcnid 'error))))
			 (get meltpoint-table fcn)
			 (get meltpoint-table fcnid)
			 meltpoint))
	 (next (if (meltentry? newv) (meltentry-expiration newv)
		   (now+ (meltentry-delta entry newv meltpoint)))))
    (if (error? newv)
	(lognotice "Error (retry at " (get next 'iso) ") "
		   "while updating meltentry applying "
		   fcn " to " args ": " newv)
	(logdebug "Computed new value (expires at " (get next 'iso) ") "
		  "for meltentry applying " fcn " to " args
		  (if trace-values (printout ": " newv))))
    (meltentry entry newv meltpoint #f)))

(defambda (meltentry entry newv (meltpoint default-meltpoint) (dolog #t))
  (let ((next (if (meltentry? newv) (meltentry-expiration newv)
		  (now+ (meltentry-delta entry newv meltpoint)))))
    (when dolog
      (if (error? newv)
	  (lognotice "Error (retry at " (get next 'iso) ") "
		     "while updating meltentry: " newv)
	  (logdebug "Computed new value (expires at " (get next 'iso) ") "
		    "for meltentry"
		    (if trace-values (printout ": " newv)))))
    ;; If the value is a meltentry, we just use that
    (if (and (exists? newv) (meltentry? newv)) newv
	(cons-meltentry (if (error? newv) {} newv)
			(cond ((not entry) (now))
			      ((identical? (meltentry-value entry) newv)
			       (meltentry-creation entry))
			      ((error? newv) (meltentry-creation entry))
			      (else (now)))
			next
			(if (error? newv) newv #f)))))

(define (meltentry-delta entry newv meltpoint)
  (cond ((applicable? meltpoint) (meltpoint entry newv))
	((not entry) meltpoint)
	;; If the value is unchanged, wait a little longer next time
	((identical? newv (meltentry-value entry))
	 (max meltpoint
	      (+ (randomish meltpoint)
		 (/~ (+ (now- (meltentry-creation entry))
			(meltentry/duration entry)) 2))))
	;; If the value is an error, wait the default interval.
	;; If the original value recurs after an error, the error is ignored
	;;  because the creation timestamp is retained across errors.
	((error? newv) (randomish meltpoint))
	;; If the value is changed, wait half as long, down to meltpoint
	(else (max meltpoint
		   (roundup (/~ (difftime (meltentry-expiration entry)
					  (meltentry-creation entry))
				2))))))

;;; Get functions

(define (meltcache/update cache fcn args (meltpoint default-meltpoint))
  (let* ((procname (procedure-id fcn))
	 (meltkey (cons (if (index? cache) procname fcn) args))
	 (cached (and cache (try (get cache meltkey) #f))))
    (if (or (not cached) (melted? cached))
	(let ((tostore (meltupdate cached fcn args meltpoint)))
	  (when cache
	    (onerror (store! cache meltkey tostore)
		     (lambda (ex)
		       (logcrit "Error saving meltcache result for "
				meltkey " in " cache))))
	  tostore)
	cached)))

(define (meltcache/force cache fcn . args)
  (let* ((procname (procedure-id fcn))
	 (meltkey (cons (if (index? cache) procname fcn) args))
	 (cached (and cache (try (get cache meltkey) #f)))
	 (tostore (meltupdate cached fcn args)))
    (when cache
      (onerror (store! cache meltkey tostore)
	       (lambda (ex)
		 (logcrit "Error saving meltcache result for "
			  meltkey " in " cache))))
    (meltentry-value tostore)))

(defambda (meltcache/store cache newv fcn args (meltpoint default-meltpoint))
  (let* ((fcnid (or (procedure-id fcn) fcn))
	 (meltpoint (try (tryif (fail? newv)
				(try (get meltpoint-table (cons fcn 'fail))
				     (get meltpoint-table (cons fcnid 'fail))))
			 (tryif (error? newv)
				(try (get meltpoint-table (cons fcn 'error))
				     (get meltpoint-table (cons fcnid 'error))))
			 (get meltpoint-table fcn)
			 (get meltpoint-table fcnid)
			 meltpoint))
	 (meltkey (if (index? cache)
		      (cons (procedure-id fcn) args)
		      (cons fcn args)))
	 (cached (and cache (try (get cache meltkey) #f)))
	 (tostore (meltentry cached newv meltpoint)))
    (when cache
      (onerror (store! cache meltkey tostore)
	       (lambda (ex)
		 (logcrit "Error saving meltcache result for "
			  meltkey " in " cache))))
    (meltentry-value tostore)))

(define (meltcache/getentry cache fcn . args)
  (meltcache/update cache fcn args))

(define (meltcache/get cache fcn . args)
  (meltentry-value (meltcache/update cache fcn args)))

(define (meltcache/probe cache fcn . args)
  (let* ((procname (procedure-id fcn))
	 (prockey (if (index? cache) procname fcn)))
    (meltentry-value (get cache (cons prockey args)))))

(define (meltcache/info cache fcn . args)
  (let* ((procname (procedure-id fcn))
	 (prockey (if (index? cache) procname fcn)))
    (get cache (cons prockey args))))

;;; Configuring meltcaches

(define meltpoint-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) default-meltpoint)
	  ((equal? default-meltpoint val))
	  ((and (pair? val)
		(or (symbol? (car val))
		    (applicable? (car val))
		    (and (pair? (car val))
			 (overlaps? (cdr (car val)) '{fail error})
			 (or (symbol? (car (car val)))
			     (applicable? (car (car val))))))
		(fixnum? (cdr val)))
	   (lognotice "Setting meltcache threshold for " (car val)
		      " to " (cdr val) " seconds")
	   (store! meltpoint-table (car val) (cdr val)))
	  ((and (pair? val)
		(or (symbol? (car val))
		    (applicable? (car val))
		    (and (pair? (car val))
			 (overlaps? (cdr (car val)) '{fail error})
			 (or (symbol? (car (car val)))
			     (applicable? (car (car val))))))
		(applicable? (cdr val)))
	   (lognotice "Setting meltcache threshold function for " (car val)
		      " to " (cdr  val))
	   (store! meltpoint-table (car val) (cdr val)))
	  ((pair? val)
	   (error 'typeerror 'meltpoint-config
		  "Not a valid meltcache config" val))
	  ((fixnum? val)
	   (lognotice "Setting default meltcache threshold to " val " seconds")
	   (set! default-meltpoint val))
	  ((or (symbol? val) (applicable? val))
	   (get meltpoint-table val))
	  (else (error 'typeerror 'meltpoint-config
		       "Not a valid meltcache config" val)))))
(config-def! 'meltpoint meltpoint-config)

