;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'cachequeue)

;;; Cache queues support the queued computation and caching of complex
;;; functions.
;;;  cq/request requests an up to date value for a given call, its
;;;    consumer argument (if not false) is called on the result when it is ready.
;;;    if the consumer argument is false, this queues the request (if neccesary)
;;;    and returns any cached value
;;;  cq/require gets a value from the cachequeue's cache or, if neccessary,
;;;   generates it and stores it in the cache.

(use-module '{ezrecords meltcache fifo logger reflection})

(define-init %loglevel %warning%)

(module-export!
 '{cachequeue
   cq/get cq/require cq/status cq/probe cq/info cq/meltentry
   cachequeue? cqstep cqdaemon
   cq/prefetch})

(define meltentry-creation (within-module 'meltcache meltentry-creation))
(define meltentry-expiration (within-module 'meltcache meltentry-expiration))

(define (doconsume method result)
  (if (null? method) result
      (apply (car method) result (cdr method))))

(define-init cachequeue-default-nthreads 4)
(define-init cachequeue-default-meltpoint #f)

;;; Making cache qeueues

(defrecord (cachequeue #[PREFIX "CQ" OPAQUE #t])
  cache     ; This caches computed values
  fifo      ; This is the FIFO of pending compute requests

  ;;  State for active tasks
  lock      ; A lock/condvar for controlling modifications to the state
  pending   ; This is a table mapping pending tasks to state information
	    ;  the data is either a timestamp (time queued), a pair of
            ;  timestamps (time queued/time started), or an error (when ending
            ;  with an error)

  ;; Various configuration information
  meltpoint ; If not #f, the cache is a meltcache with this meltpoint
  props     ; These are miscellaneous properties of the object
  )

#|
(module-export!
 '{cq-cache cq-method cq-fifo cq-cache
	    cq-compute cq-consumers
	    cq-meltpoint cq-props})
|#

;; This maps cqs to the threads listening to them
;; Warning! This could introduce hard-to-gc circularities
;; (We're not using this right now)
(define cqthreads (make-hashtable))

;;; Constructing cachequeues

(define (cachequeue (cache (make-hashtable))
		    (nthreads cachequeue-default-nthreads)
		    (meltpoint cachequeue-default-meltpoint))
  "Create a cachequeue which allows for scheduled cached computations. \
   CACHE is the results cache, NTHREADS is the number of threads to start \
   for processing requests, and MELTPOINT indicates whether the cache \
   is a meltcache."
  (when (string? cache)
    (unless (or (position #\@ cache) (file-exists? cache))
      (make-hash-index cache 1000))
    (set! cache (open-index cache)))
  (let ((cq (cons-cachequeue
	     cache (make-fifo) (make-condvar)
	     (make-hashtable) meltpoint (frame-create #f))))
    (when nthreads
      (dotimes (i nthreads)
	(add! cqthreads cq (threadcall cqdaemon cq i #f))))
    cq))

;;; Standard API

(define (cq/get cq fn . args)
  (cachequeuerequest
   cq (if (and (pair? fn) (null? args)) fn (cons fn args))))

(define (cq/require cq fn . args)
  (cachequeuerequire
   cq (if (and (pair? fn) (null? args)) fn (cons fn args))))

(define (cq/status cq fn . args)
  (cqstatus cq (if (and (pair? fn) (null? args)) fn (cons fn args))))

(define (cq/probe cq fn . args)
  (let* ((call (if (and (pair? fn) (null? args)) fn (cons fn args)))
	 (cache (cq-cache cq))
	 (cachekey (if (index? cache)
		       (cons (procedure-id (car call)) (cdr call))
		       call))
	 (cachevalue (get cache cachekey)))
    (meltvalue cachevalue)))

(define (cq/meltentry cq fn . args)
  (cachequeuerequest
   cq (if (and (pair? fn) (null? args)) fn (cons fn args))
   #f))

(define (cq/info cq fn . args)
  (let* ((call (if (and (pair? fn) (null? args)) fn (cons fn args)))
	 (cache (cq-cache cq))
	 (cachekey (if (index? cache)
		       (cons (procedure-id (car call)) (cdr call))
		       call)))
    (get cache cachekey)))

(define (cq/prefetch cq fn . args)
  (when (index? (cq-cache cq))
    (prefetch-keys!
     (cq-cache cq)
     (for-choices (f fn)
       (apply list (procedure-id f) args)))))

;;; Making requests

(define (cachequeuepush cq call (statehandler #f))
  (%debug "Pushing request for " call)
  (with-lock (cq-lock cq)
    (let* ((cache (cq-cache cq))
	   (pending (cq-pending cq))
	   (cachekey (if (index? cache)
			 (cons (procedure-id (car call)) (cdr call))
			 call))
	   (cachevalue (get cache cachekey)))
      (if (if (exists? cachevalue)
	      (not (melted-value? cachevalue))
	      (test cache cachekey))
	  cachevalue
	  (let ((state (get pending call)))
	    (when (fail? state)
	      (set! state (timestamp))
	      (add! pending call state)
	      (fifo-push (cq-fifo cq) call)
	      (when statehandler (statehandler state)))
	    cachevalue)))))

(define (cachequeuepreempt cq call)
  (%debug "Preemptive request for " call)
  (with-lock (cq-lock cq)
    (let* ((cache (cq-cache cq))
	   (pending (cq-pending cq))
	   (cachekey (if (index? cache)
			 (cons (procedure-id (car call)) (cdr call))
			 call))
	   (cachevalue (get cache cachekey)))
      (if (if (exists? cachevalue)
	      (not (melted-value? cachevalue))
	      ;; Since we can cache an empty choice, we use test to determine
	      ;;  whether the empty value is cached
	      (test cache cachekey))
	  (begin (%debug "Using cache value for preemptive request " call)
		 cachevalue)
	  (let ((state (get pending call)))
	    (if (and (exists? state) (pair? state))
		(begin
		  (%debug "Waiting for active computation to resolve " call)
		  (while (pair? (get pending call))
		    (condvar-wait (cq-lock cq)))
		  (%debug "Call is no longer pending " call)
		  (try (get pending call) (get cache cachekey)))
		(begin
		  (%debug "Directly executing " call)
		  (when (exists? state)
		    (store! pending call (cons (get pending call) (timestamp)))
		    (fifo-jump (cq-fifo cq) call))
		  (synchro-unlock (cq-lock cq))
		  (%debug "Starting execution of " call)
		  (let ((value (erreify (apply (car call) (cdr call)))))
		    (%debug "Finished executing " call)
		    (synchro-lock (cq-lock cq))
		    (%debug "Caching results of " call)
		    (if (error? value)
			(store! pending call value)
			(begin
			  (when (cq-meltpoint cq)
			    (set! value
				  (meltentry (try cachevalue #f)
					     value (cq-meltpoint cq))))
			  (store! cache cachekey value)
			  (drop! pending call)))
		    value))))))))

(define (cachequeuerequest cq call (melt #t))
  (let* ((cache (cq-cache cq))
	 (pending (cq-pending cq))
	 (cachekey (if (index? cache)
		       (cons (procedure-id (car call)) (cdr call))
		       call))
	 (cachevalue (get cache cachekey)))
    (if (if (exists? cachevalue)
	    (not (melted-value? cachevalue))
	    (test cache cachekey))
	(begin (%debug "Using cached result for " call)
	       (if melt (meltvalue cachevalue) cachevalue))
	(if melt (meltvalue (cachequeuepush cq call))
	    (cachequeuepush cq call)))))

(define (cachequeuerequire cq call)
  (let* ((cache (cq-cache cq))
	 (pending (cq-pending cq))
	 (cachekey (if (index? cache)
		       (cons (procedure-id (car call)) (cdr call))
		       call))
	 (cachevalue (get cache cachekey)))
    (if (if (exists? cachevalue)
	    (not (melted-value? cachevalue))
	    (test cache cachekey))
	(meltvalue cachevalue)
	(meltvalue (cachequeuepreempt cq call)))))

(define (cqstatus cq call (args))
  "Returns the status of a queued computation"
  (set! args (cdr call))
  (let ((cachekey (if (index? (cq-cache cq))
		      (cons (procedure-id (car call)) (cdr call))
		      call)))
    (if (test (cq-cache cq) cachekey)
	(let ((cachev (get (cq-cache cq) cachekey)))
	  (if (and (exists? cachev) (meltentry? cachev))
	      (if (melted? cachev)
		  (list 'melted
			(meltentry-creation cachev)
			(meltentry-expiration cachev))
		  (list 'cached 
			(meltentry-creation cachev)
			(meltentry-expiration cachev)))
	      '(cached)))
	(let ((statev (get (cq-pending cq) call)))
	  (cond ((fail? statev) #f)
		((error? statev) (cons 'error statev))
		((pair? statev)
		 (list 'running (car statev) (cdr statev)))
		((timestamp? statev)
		 (list 'queued statev))))))
  (if (test (cq-cache cq) args) #t
      (unwind-protect
	  (begin (synchro-lock (cq-lock cq))
		 (try (get (cq-cache cq) args)
		      #f))
	(synchro-unlock (cq-lock cq)))))

(define (cqinfo cq call)
  "Returns the status of a queued computation"
  (let ((cachekey (if (index? (cq-cache cq))
		      (cons (procedure-id (car call)) (cdr call))
		      call)))
    (get (cq-cache cq) cachekey)))

;;; Doing tasks on the queue

(define (cqstep cq call)
  (%debug "CACHEQUEUE step applying " (car call) " to " (cdr call))
  (store! (cq-pending cq) call
	  (cons (get (cq-pending cq) call)
		(timestamp)))
  (let ((cache (cq-cache cq))
	(value (erreify (apply (car call) (cdr call)))))
    (if (error? value)
	(store! (cq-pending cq) call value)
	(begin (if (cq-meltpoint cq)
		   (meltcache/store cache value (car call) (cdr call)
				    (cq-meltpoint cq))
		   (store! cache
			   (if (index? cache)
			       (cons (procedure-id (car call)) (cdr call))
			       call)
			   value))
	       (drop! (cq-pending cq) call)))
    (%debug "CACHEQUEUE step done applying " (car call) " to " (cdr call))
    value))

(define (cqdaemon cq i (max 1))
  (let ((entry (fifo-pop (cq-fifo cq)))
	(count 0))
    (while (and (exists? entry)
		(pair? entry)
		(or (not max) (< count max)))
      (cqstep cq entry)
      (set! count (1+ count))
      (when (or (not max) (< count max))
	(set! entry (fifo-pop (cq-fifo cq)))))))

