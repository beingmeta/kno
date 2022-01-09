;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'kno/threads)

;;; This provides macros for easy use of multiple threads (and cores)
;;; in applications.  It also provides a way to easily implement the
;;; prefetch/execute cycles which can improve performance on many
;;; database-intensive operations. 

(use-module '{reflection text/stringfmts varconfig logger fifo/call})

(module-export! '{threadcount mt/call})

;;; Thread count utilities

(define default-threadcount 1.0)
(varconfig! nthreads default-threadcount)

(define (threadcount (arg default-threadcount)
		     (maxval (config 'maxthreads #f))
		     (ncpus (get (rusage) 'ncpus)))
  (cond ((flonum? maxval) (set! maxval (threadcount maxval)))
	((eq? maxval #t) (set! maxval ncpus)))
  (cond ((not arg) #f)
	((eq? arg #t) (if maxval (max ncpus maxval) ncpus))
	((and (fixnum? arg) (> arg 0) maxval) (min arg maxval))
	((and (fixnum? arg) (> arg 0)) arg)
	((and (flonum? arg) (> arg 0)) 
	 (max (->exact (ceiling (* arg ncpus))) (or maxval 0)))
	((number? arg) (bad-threadcount arg))
	((symbol? arg) 
	 (if (config arg)
	     (threadcount (config arg) maxval)
	     (threadcount default-threadcount)))
	(else (bad-threadcount arg))))

(define (bad-threadcount arg)
  (logwarn |BadThreadcount|
    "The argument " arg " can't be used as a threadcount. "
    "Using the default threadcount " default-threadcount " instead.")
  (threadcount default-threadcount))

(define (check-threadcount arg)
  (if (and (singleton? arg) (number? arg)
	   (zero? (imag-part arg)) (> arg 0))
      arg
      (if (not arg) arg
	  (irritant arg |InvalidThreadcount| 
	    "This value cannot be used as a default threadcount."))))

(defambda (mt/call opts fcn . args)
  (let* ((results {})
	 (add-result (lambda (arg) (set+! results arg)))
	 (fifo.threads (apply fifo/call [results add-result] fcn args)))
    (thread/wait (cdr fifo.threads))
    results))


