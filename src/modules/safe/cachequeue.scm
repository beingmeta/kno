(in-module 'cachequeue)

;;; Cache queues support the queued computation and caching of complex
;;; functions.

(define version "$Id$")

(use-module '{ezrecords meltcache fifo logger reflection})

(define %loglevel %warning!)
;(define %loglevel %debug!)

(module-export!
 '{cachequeue
   cq/call cq/consume cq/request cq/require
   cq/status cq/status* cq/probe cq/probe*
   ;; Recent legacy
   cqompute cqonsume})

(define (doconsume method result)
  (if (null? method) result
      (apply (car method) result (cdr method))))

;;; Making cache qeueues

(defrecord (cachequeue #[PREFIX "CQ" OPAQUE #t])
  cache     ; This caches computed values
  method    ; This is the method (procedure) used to compute cache valuesp
  fifo      ; This is the FIFO of compute requests
  state     ; This is a table of state information for pending requests
  compute   ; This is the procedure to actually process a queued request.
            ;   It wraps the method (above) in various bookkeeping operations.
  consumers ; This is the callbacks waiting on this value
  meltpoint ; This is the duration of entries in the the cache, or #f for forever
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

(define (cachequeue method (cache (make-hashtable)) (nthreads 4) (meltpoint #f))
  "Create a cachequeue which allows for scheduled cached computations. \
   CACHE is a table used to store results, METHOD is used to compte them \
   and THREADS is the number of threads to start for processing them."
  (let ((statetable (make-hashtable))
	(consumers (make-hashtable)))
    (let ((cqmethod
	   (slambda (cq args consumer)
	     (if (test cache args)
		 (let ((v (get cache args)))
		   (when (meltentry? v)
		     (when (melted? v)
		       (%debug "recomputing "
			       (or (procedure-name (cq-method cq)) (cq-method cq)) 
			       " for " args)
		       (cqompute-inner cq args))
		     (set! v (meltentry-value v)))
		   (if (not consumer) v (doconsume consumer v)))
		 (if (test statetable args)
		     (let ((state (get statetable args)))
		       (if (error? (first state))
			   (doconsume consumer (first state))
			   (if consumer (add! consumers args consumer)))
		       state)
		     (begin (if consumer (add! consumers args consumer))
			    (cqompute-inner cq args)))))))
      (let ((cq (cons-cachequeue cache method (make-fifo)
				 statetable cqmethod consumers
				 meltpoint (frame-create #f))))
	(dotimes (i nthreads)
	  (add! cqthreads cq (threadcall cqdaemon cq i #f)))
	cq))))

(defambda (cq/reserve cq args token)
  (if (test (cq-cache cq) args)
      (let ((v (get (cq-cache cq) args)))
	(if (and (meltentry? v) (melted? v))
	    (try (get (cq-state cq) args)
		 (begin (store! (cq-state cq) args token)
			token))
	    #f))
      (try (get (cq-state cq) args)
	   (begin (store! (cq-state cq) args token)
		  token))))

;; This is used by the cachequeue compute method
(define (cqompute-inner cq args)
  "Internal function queues a call"
  (let* ((token (vector '|queued...| (timestamp)))
	 (active (cq/reserve cq args token)))
    (when (eq? token active)
      (%debug "queuing "
	      (or (procedure-name (cq-method cq)) (cq-method cq)) 
	      " for " args)
      (fifo-push (cq-fifo cq) args))
    (if (not active)
	(get (cq-cache cq) args)
	active)))

(define (cq/cached? cq args)
  (let ((v (get (cq-cache cq) args)))
    (and (exists? v)
	 (and (cq-meltpoint cq)
	      (time-later? (timestamp+ (third v) (cq-meltpoint cq)))))))

;;; Using cachequeues

(define (cq/call cq . args)
  "Invokes a cachequeue on some arguments, returning cached values
   or queueing the request."
  (let ((v (get (cq-cache cq) args)))
    (if (fail? v)
	(begin ((cq-compute cq) cq args #f)
	       (get (cq-cache cq) args))
	(if (meltentry? v)
	    (begin (if (melted? v) ((cq-compute cq) cq args #f))
		   (meltentry-value v))
	    v))))
(define cqompute cq/call)

(define (cq/consume cq args . consumer)
  "Invokes a cachequeue on some arguments, and applies a consumer \
   to the result (as a callback).  This works immediately if the  \
   value is cached."
  (let ((v (get (cq-cache cq) args)))
    (if (fail? v)
	(if (null? consumer)
	    ((cq-compute cq) cq args #f)
	    ((cq-compute cq) cq args consumer))
	(if (meltentry? v)
	    (if (melted? v)
		((cq-compute cq) cq args consumer)
		(doconsume consumer (meltentry-value v)))
	    (doconsume consumer v))
	(if (and (meltentry? v) (melted? v))
	    ((cq-compute cq) cq args consumer)
	    (doconsume consumer (meltentry-value v))))))
(define cqonsume cq/consume)

(define (cq/request cq . args)
  "This queues a request for cache queue but doesn't wait around \
   or consume any results."
  (unless (test (cq-cache cq) args)
    (cqompute-inner cq args)))

(define (cqwait result cv)
  (condvar-signal cv))

(define (cq/require cq . args)
  "Jumps the cachequeue to process particular arguments"
  (if (test (cq-cache cq) args)
      (apply cq/call cq args)
      (let ((statetable (cq-state cq)))
	(let* ((token (vector '|queued...| (timestamp)))
	       (active (cq/reserve cq args token)))
	  (if (not active) (get (cq-cache cq) args)
	      (if (eq? token active)
		  (begin
		    (cqdaemon-step cq args)
		    (meltvalue (get (cq-cache cq) args)))
		  (let ((consumers (cq-consumers cq))
			(cv (make-condvar)))
		    (add! consumers args (list cqwait cv))
		    (condvar-wait cv)
		    (meltvalue (get (cq-cache cq) args)))))))))

;;; The daemon which gets requests and processes them

(define (cqdaemon-step cq entry)
  (let* ((statetable (cq-state cq))
	 (consumers (cq-consumers cq))
	 (method (cq-method cq))
	 (cache (cq-cache cq))
	 (token (get statetable entry)))
    (when (eq? (first token) '|queued...|)
      ;; Update the state table
      (store! statetable entry
	      (vector '|computing...| (timestamp) (second token)))
      (%debug "Applying " (or (procedure-name method) method)
	      " to " entry)
      ;; Apply the result
      (onerror (let ((result (apply method entry)))
		 (%debug "Applied " (or (procedure-name method) method)
			 " to " entry)
		 (store! cache entry
			 (if (cq-meltpoint cq)
			     (if (meltentry? result) result
				 (cons-meltentry result (timestamp)
						 (timestamp+ (cq-meltpoint cq))))
			     result))
		 (do-choices (consumer (get consumers entry))
		   (onerror (doconsume consumer result)
			    (lambda (ex)
			      (logger %error! "Error consuming " result
				      " for " entry " in " cq ": "
				      ex))))
		 (drop! statetable entry)
		 (drop! consumers entry))
	       (lambda (ex)
		 (store! statetable entry (cons ex (timestamp)))
		 (do-choices (consumer )
		   (doconsume consumer ex))
		 (logger %error!
		   "Error while attempting to compute " (cq-method cq)
		   " for " entry ": " ex)
		 (get statetable entry))))))

(define (cqdaemon cq i (max 1))
  (let ((statetable (cq-state cq))
	(consumers (cq-consumers cq))
	(method (cq-method cq))
	(cache (cq-cache cq))
	(entry (fifo-pop (cq-fifo cq)))
	(count 0))
    (while (and (pair? entry) (or (not max) (< count max)))
      (cqdaemon-step cq entry)
      (set! count (1+ count))
      (when (or (not max) (< count max))
	(set! entry (fifo-pop (cq-fifo cq)))))))

;;; Probing cachequeues

(define (cq/probe cq args)
  "Returns true of ARGS are already cached in CQ."
  (test (cq-cache cq) args))
(define (cq/probe* cq . args)
  "Returns true of ARGS are already cached in CQ."
  (test (cq-cache cq) args))

(define (cq/status cq args)
  "Returns the status of a queued computation"
  (if (test (cq-cache cq) args) #t
      (unwind-protect
	  (begin (synchro-lock! (cq-compute cq))
		 (try (get (cq-state cq) args)
		      #f))
	(synchro-unlock! (cq-compute cq)))))
(define (cq/status* cq . args)
  "Returns the status of a queued computation"
  (if (test (cq-cache cq) args) #t
      (unwind-protect
	  (begin (synchro-lock! (cq-compute cq))
		 (try (get (cq-state cq) args)
		      #f))
	(synchro-unlock! (cq-compute cq)))))



