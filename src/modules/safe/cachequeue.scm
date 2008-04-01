(in-module 'cachequeue)

;;; Cache queues support the queued computation and caching of complex
;;; functions.
;;;  cq/request requests an up to date value for a given call, its
;;;    consumer argument (if not false) is called on the result when it is ready.
;;;    if the consumer argument is false, this queues the request (if neccesary)
;;;    and returns any cached value
;;;  cq/require gets a value from the cachequeue's cache or, if neccessary,
;;;   generates it and stores it in the cache.


(define version "$Id$")

(use-module '{ezrecords meltcache fifo logger reflection})

(define %loglevel %warning!)
;(define %loglevel %debug!)

(module-export!
 '{cachequeue
   cq/get cq/require cq/status cq/probe cq/info
   cqstep cqdaemon})

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
 '{cq-cache cq-method cq-fifo cq-state
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
  (cachequeuestatus
   cq (if (and (pair? fn) (null? args)) fn (cons fn args))))

(define (cq/probe cq fn . args)
  (let* ((cache (cq-cache cq))
	 (cachekey (if (index? cache)
		       (cons (procedure-name (car call)) (cdr call))
		       call))
	 (cachevalue (get cache cachekey)))
    (meltvalue cachevalue)))

(define (cq/info cq fn . args)
  (let* ((cache (cq-cache cq))
	 (cachekey (if (index? cache)
		       (cons (procedure-name (car call)) (cdr call))
		       call)))
    (get cache cachekey)))

;;; Making requests

(define (cachequeuepush cq call (statehandler #f))
  (with-lock (cq-lock cq)
    (let* ((cache (cq-cache cq))
	   (pending (cq-pending cq))
	   (cachekey (if (index? cache)
			 (cons (procedure-name (car call)) (cdr call))
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
  (with-lock (cq-lock cq)
    (let* ((cache (cq-cache cq))
	   (pending (cq-pending cq))
	   (cachekey (if (index? cache)
			 (cons (procedure-name (car call)) (cdr call))
			 call))
	   (cachevalue (get cache cachekey)))
      (if (if (exists? cachevalue)
	      (not (melted-value? cachevalue))
	      ;; Since we can cache an empty choice, we use test to determine
	      ;;  whether the empty value is cached
	      (test cache cachekey))
	  cachevalue
	  (let ((state (get pending call)))
	    (if (pair? state)
		(begin
		  (while (pair? (get pending call))
		    (condvar-wait (cq-lock cq)))
		  (try (get pending call) (get cache cachekey)))
		(begin
		  (store! pending call (cons (get pending call) (timestamp)))
		  (fifo-jump (cq-fifo cq) call)
		  (synchro-unlock (cq-lock cq))
		  (let ((value (erreify (apply (car call) (cdr call)))))
		    (synchro-lock (cq-lock cq))
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

(define (cachequeuerequest cq call)
  (let* ((cache (cq-cache cq))
	 (pending (cq-pending cq))
	 (cachekey (if (index? cache)
		       (cons (procedure-name (car call)) (cdr call))
		       call))
	 (cachevalue (get cache cachekey)))
    (if (if (exists? cachevalue)
	    (not (melted-value? cachevalue))
	    (test cache cachekey))
	(meltvalue cachevalue)
	(meltvalue (cachequeuepush cq call)))))

(define (cachequeuerequire cq call)
  (let* ((cache (cq-cache cq))
	 (pending (cq-pending cq))
	 (cachekey (if (index? cache)
		       (cons (procedure-name (car call)) (cdr call))
		       call))
	 (cachevalue (get cache cachekey)))
    (if (if (exists? cachevalue)
	    (not (melted-value? cachevalue))
	    (test cache cachekey))
	(meltvalue cachevalue)
	(meltvalue (cachequepreempt cq call)))))

(define (cqstatus cq call)
  "Returns the status of a queued computation"
  (let ((cachekey (if (index? (cq-cache cq))
		      (cons (procedure-name (car call))
			    (cdr call))
		      call)))
    (if (test (cq-cache cq) cachekey)
	(let ((cachev (get (cq-cache cq) cachekey)))
	  (if (and (exists? v) (meltentry? v))
	      (if (melted? v)
		  (list 'melted
			(meltentry-creation v)
			(meltentry-expiration v))
		  (list 'cached 
			(meltentry-creation v)
			(meltentry-expiration v)))
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
	  (begin (synchro-lock! (cq-compute cq))
		 (try (get (cq-state cq) args)
		      #f))
	(synchro-unlock! (cq-compute cq)))))

(define (cqinfo cq call)
  "Returns the status of a queued computation"
  (let ((cachekey (if (index? (cq-cache cq))
		      (cons (procedure-name (car call))
			    (cdr call))
		      call)))
    (get (cq-cache cq) cachekey)))

;;; Doing tasks on the queue

(define (cqstep cq call)
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
			       (cons (procedure-name (car call)) (cdr call))
			       call)
			   value))
	       (drop! (cq-pending cq) call)))
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

