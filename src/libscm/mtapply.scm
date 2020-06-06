;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'mtapply)

;;; This provides macros for easy use of multiple threads (and cores)
;;; in applications.  It also provides a way to easily implement the
;;; prefetch/execute cycles which can improve performance on many
;;; database-intensive operations. 

(use-module '{reflection stringfmts varconfig logger})

(define (just-clearcaches (ignored))
  (clearcaches)
  #t)

(defambda (mt/apply spec . args)
  (let ((opts spec) (fcn #f) (args args))
    (cond ((applicable? opts)
	   (set! fcn opts) (set! opts #f))
	  ((and (testopt spec 'apply)
		(applicable? (getopt spec 'apply)))
	   (set! fcn (getopt spec 'apply))
	   (set! opts spec))
	  ((empty-list? args)
	   (irritant spec |No apply method|))
	  ((applicable? (car args))
	   (set! fcn (car args))
	   (set! opts spec)
	   (set! args (cdr args)))
	  (else (irritant spec |No apply method|)))
    (let ((nthreads (getopt opts 'nthreads (config 'nthreads #t)))
	  (fetchfn (getopt opts 'fetchfn #f))
	  (fetchsize (getopt opts 'fetchsize #f))
	  (finishfn (getopt opts 'finishfn just-clearcaches))
	  (choicevec (choice->vector (car args)))
	  (xargs (cdr args))
	  (limit (choice-size (car args)))
	  (fetch-limit 0)
	  (runvar (make-condvar))
	  (running 0)
	  (offset 0))
      (cond ((= (length choicevec) 0) (fail))
	    ((= (length choicevec) 1) (apply fn args))
	    (fetchsize (let ((sync-one-choice
			      (slambda ()
				(if (< offset fetch-limit)
				    (prog1 (elt choicevec offset)
				      (set! offset (1+ offset)))
				    (let* ((block (slice choicevec fetch-limit))
					   (queue (elts block)))
				      (when fetchfn (fetchfn (qc queue)))
				      (condvar/lock! runvar)
				      (set! running (-1+ running))
				      (condvar/unlock! runvar)
				      (until (zero? running) (condvar/wait runvar))
				      (finishfn (qc queue))
				      (prog1 (elt choicevec offset)
					(set! offset (1+ offset))))))))
			 (let ((get-one-choice
				(lambda ()
				  (condvar/lock! runvar)
				  (set! running (-1+ running))
				  (condvar/unlock! runvar)
				  (sync-one-choice )))))
			 ))))))



