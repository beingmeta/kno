;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, LLC

(in-module 'kno/threads)

;;; This provides macros for easy use of multiple threads (and cores)
;;; in applications.  It also provides a way to easily implement the
;;; prefetch/execute cycles which can improve performance on many
;;; database-intensive operations. 

(use-module '{kno/reflect text/stringfmts varconfig logger})

(module-export! '{threadcount})

;;; Thread count utilities

(define default-threadcount 1.0)
(varconfig! nthreads default-threadcount)

(define (threadcount (arg default-threadcount) (maxval)
		     (ncpus (get (rusage) 'ncpus)))
  (default! maxval ncpus)
  (when (not (fixnum? maxval))
    (set! maxval (threadcount maxval)))
  (cond ((not arg) #f)
	((eq? arg #t) (max ncpus maxval))
	((and (fixnum? arg) (> arg 0)) (min arg (or maxval 0)))
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
