(in-module 'cachequeue)

;;; Cache queues support the queued computation and caching of complex
;;; functions.

(define version "$Id: fifo.scm 1761 2007-08-31 11:20:52Z haase $")

(use-module '{ezrecords fifo logger})

(module-export! '{cachequeue cqompute cqonsume cachequeue/status})
#|
(module-export!
 '{cachequeue-cache
   cachequeue-method cachequeue-fifo
   cachequeue-state cachequeue-consumers
   cqdaemon})
|#

(define (doconsume method result)
  (if (pair? method)
      (apply (car method) result (cdr method))
      (method result)))

;;; Making cache qeueues

(defrecord cachequeue cache method fifo state compute consumers props)

(define (cachequeue cache method (nthreads 4))
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
	  (threadcall cqdaemon cq i #f))
	cq))))

;;; Using cachequeues

(define (cqompute cq . args)
  (car
    (try (get (cachequeue-cache cq) args)
	 ((cachequeue-compute cq) cq args #f))))

(define (cqonsume cq args consumer . consumer-args)
  (if (test (cachequeue-cache cq) args)
      (if (null? consumer-args)
	  (consumer (car (get (cachequeue-cache cq) args)))
	  (apply consumer (car (get (cachequeue-cache cq) args))
		 consumer-args))
      ((cachequeue-compute cq) cq args
       (if (null? consumer-args) consumer (cons consumer consumer-args)))))

(define (cachequeue/status cq . args)
  (if (test (cachequeue-cache cq) args) #t
      (unwind-protect
	  (begin (synchro-lock! (cachequeue-compute cq))
		 (try (get (cachequeue-state cq) args)
		      #f))
	(synchro-unlock! (cachequeue-compute cq)))))

(define (cqompute-inner cq args)
  (let* ((statetable (cachequeue-state cq))
	 (token (list '|queued...| (timestamp))))
    (store! statetable args token)
    (fifo-push (cachequeue-fifo cq) args)
    token))

;;; The daemon which gets requests and processes them

(define (cqdaemon cq i (max 1))
  (let ((statetable (cachequeue-state cq))
	(consumers (cachequeue-consumers cq))
	(method (cachequeue-method cq))
	(cache (cachequeue-cache cq))
	(entry (fifo-pop (cachequeue-fifo cq)))
	(count 0))
    (while (and (pair? entry) (or (not max) (< count max)))
      (let ((token (get statetable entry)))
	(when (eq? (car token) '|queued...|)
	  (store! statetable entry (list '|computing...| (timestamp) (second token)))
	  (let ((result (apply method entry)))
	    (onerror (begin
		       (store! cache entry
			       (list result (second token)
				     (timestamp) (config 'sessionid)))
		       (do-choices (consumer (get consumers entry))
			 (if (pair? consumer)
			     (apply (car consumer) result (cdr consumer))
			     (consumer result)))
		       (drop! statetable entry)
		       (drop! consumers entry))
		     (lambda (ex)
		       (store! statetable entry (cons ex (timestamp)))
		       (do-choices (consumer (get consumers entry))
			 (if (pair? consumer)
			     (apply (car consumer) ex (cdr consumer))
			     (consumer ex)))
		       (logger %error!
			 "Error while attempting to compute " (cachequeue-method cq)
			 " for " entry ": " ex)
		       (get statetable entry))))))
      (set! count (1+ count))
      (when (or (not max) (< count max))
	(set! entry (fifo-pop (cachequeue-fifo cq)))))))



