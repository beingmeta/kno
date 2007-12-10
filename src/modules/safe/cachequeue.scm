(in-module 'cachequeue)

;;; Cache queues support the queued computation and caching of complex
;;; functions.

(define version "$Id$")

(use-module '{ezrecords fifo logger reflection})

(define %loglevel 9)

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

(defrecord (cachequeue #[PREFIX "CQ"])
  cache     ; This caches computed values
  method    ; This is the method (procedure) used to compute cache valuesp
  fifo      ; This is the FIFO of compute requests
  state     ; This is a table of state information for pending requests
  compute   ; This is the procedure to actually process a queued request.
            ;   It wraps the method (above) in various bookkeeping operations.
  consumers ; This is the callbacks waiting on this value
  props     ; These are miscellaneous properties of the object
  )

;; This maps cqs to the threads listening to them
;; Warning! This could introduce hard-to-gc circularities
;; (We're not using this right now)
(define cqthreads (make-hashtable))

(define (cachequeue method (cache (make-hashtable)) (nthreads 4))
  "Create a cachequeue which allows for scheduled cached computations. \
   CACHE is a table used to store results, METHOD is used to compte them \
   and THREADS is the number of threads to start for processing them."
  (let ((statetable (make-hashtable))
	(consumers (make-hashtable)))
    (let ((cqmethod
	   (slambda (cq args consumer)
	     (if (test cache args)
		 (if (not consumer) (get cache args)
		     (doconsume consumer (get cache args)))
		 (if (test statetable args)
		     (let ((state (get statetable args)))
		       (if (error? (car state))
			   (doconsume consumer (car state))
			   (if consumer (add! consumers args consumer)))
		       state)
		     (begin (if consumer (add! consumers args consumer))
			    (cqompute-inner cq args)))))))
      (let ((cq (cons-cachequeue cache method (make-fifo)
				 statetable cqmethod consumers
				 (frame-create #f))))
	(dotimes (i nthreads)
	  (add! cqthreads cq (threadcall cqdaemon cq i #f)))
	cq))))

(defambda (cq/reserve cq args token)
  (try (tryif (test (cq-cache cq) args) #f)
       (get (cq-state cq) args)
       (begin (store! (cq-state cq) args token)
	      token)))

;; This is used by the cachequeue compute method
(define (cqompute-inner cq args)
  "Internal function queues a call"
  (let* ((token (list '|queued...| (timestamp)))
	 (active (cq/reserve cq args token)))
    (if (eq? token active)
	(fifo-push (cq-fifo cq) args))
    (if (not active)
	(get (cq-cache cq) args)
	active)))

;;; Using cachequeues

(define (cq/call cq . args)
  "Invokes a cachequeue on some arguments, returning cached values
   or queueing the request."
  (car
    (try (get (cq-cache cq) args)
	 ((cq-compute cq) cq args #f))))
(define cqompute cq/call)

(define (cq/consume cq args . consumer)
  "Invokes a cachequeue on some arguments, and applies a consumer \
   to the result (as a callback).  This works immediately if the  \
   value is cached."
  (if (test (cq-cache cq) args)
      (doconsume consumer (car (get (cq-cache cq) args)))
      (apply (cq-compute cq) cq args consumer)))
(define cqconsume cq/consume)

(define (cq/request cq . args)
  "This queues a request for cache queue but doesn't wait around \
   or consume an y results."
  (unless (test (cq-cache cq) args)
    (cqompute-inner cq args)))

(define (cqwait result cv)
  (condvar-signal cv))

(define (cq/require cq . args)
  "Jumps the cachequeue to process particular arguments"
  (if (test (cq-cache cq) args)
      (first (get (cq-cache cq) args))
      (let ((statetable (cq-state cq)))
	(let* ((token (list '|queued...| (timestamp)))
	       (active (cq/reserve cq args token)))
	  (if (not active) (get (cq-cache cq) args)
	      (if (eq? token active)
		  (begin
		    (cqdaemon-step cq args)
		    (first (get (cq-cache cq) args)))
		  (let ((consumers (cq-consumers cq))
			(cv (make-condvar)))
		    (add! consumers args (list cqwait cv))
		    (condvar-wait cv)
		    (first (get (cq-cache cq) args)))))))))

;;; The daemon which gets requests and processes them

(define (cqdaemon-step cq entry)
  (let* ((statetable (cq-state cq))
	 (consumers (cq-consumers cq))
	 (method (cq-method cq))
	 (cache (cq-cache cq))
	 (token (get statetable entry)))
    (when (eq? (car token) '|queued...|)
      ;; Update the state table
      (store! statetable entry
	      (list '|computing...| (timestamp) (second token)))
      (logger %debug! "Applying " (or (procedure-name method) method)
	      " to " entry)
      ;; Apply the result
      (onerror (let ((result (apply method entry)))
		 (logger %debug! "Applied " (or (procedure-name method) method)
			 " to " entry)
		 (store! cache entry
			 (list result (second token)
			       (timestamp) (config 'sessionid)))
		 (do-choices (consumer (get consumers entry))
		   (onerror (doconsume consumer result)
			    (lambda (ex)
			      (warn "Error consuming " result
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



