;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'mttools)

;;; This provides macros for easy use of multiple threads (and cores)
;;; in applications.  It also provides a way to easily implement the
;;; prefetch/execute cycles which can improve performance on many
;;; database-intensive operations. 

(use-module 'reflection)
(use-module 'stringfmts)

(module-export! '{mt-apply
		  do-choices-mt
		  do-vector-mt
		  interval-string
		  short-interval-string
		  mt/fetchoids
		  mt/lockoids
		  mt/save/fetch
		  mt/save/swap/fetch
		  mt/save/lock/fetch
		  mt/save/fetchkeys
		  mt/detailed-progress
		  mt/sparse-progress
		  mt/default-progress
		  mt/noprogress mt/no-progress
		  mt/custom-progress})

;;; Utility functions

(define (legacy-blockproc proc)
  (and proc
       (if (= (procedure-arity proc) 1)
	   (lambda (args done) (unless done (proc (qc args))))
	   proc)))

(define (nrange start end)
  (if (>= start end) (fail)
      (choice start (nrange (1+ start) end))))
(define (make-counter max)
  (let ((counter 0))
    (slambda ()
      (if (< counter max)
	  (let ((c counter))
	    (set! counter (1+ c))
	    c)
	  (fail)))))

;;;; Primary functions

(define (threadfcn id proc vec counter)
  (let ((entry (counter)))
    (while (exists? entry)
      (proc (elt vec entry))
      (set! entry (counter)))))

(define (mt-apply n-threads proc choices)
  (if (<= n-threads 1)
      (let* ((vec (choice->vector choices))
	     (counter (make-counter (length vec))))
	(threadfcn 0 proc vec counter))
      (let* ((vec (choice->vector choices))
	     (counter (make-counter (length vec)))
	     (ids (nrange 0 n-threads)))
	(threadjoin
	 (threadcall threadfcn ids proc vec counter)))))

;;; The main macros

(define do-choices-mt
  (macro expr
    (let* ((control-spec (get-arg expr 1))
	   (arg (get-arg control-spec 0))
	   (choice-generator (get-arg control-spec 1))
	   (n-threads (get-arg control-spec 2 10)))
      (if (<= (length control-spec) 3)
	  `(,mt-apply ,n-threads
		      (lambda (,arg) ,@(cdr (cdr expr)))
		      (qc ,choice-generator))
	  (let ((blockproc (get-arg control-spec 3 #f))
		(blocksize (get-arg control-spec 4 #f))
		(progressfn (get-arg control-spec 5 (get-default-progressfn))))
	    `(let ((_choice ,choice-generator)
		   (_blocksize ,blocksize)
		   (_blockproc (,legacy-blockproc ,blockproc))
		   (_progressfn (,getprogressfn ,progressfn))
		   (_bodyproc (lambda (,arg) ,@(cdr (cdr expr))))
		   (_nthreads ,n-threads)
		   (_start (elapsed-time))
		   (_startu (rusage))
		   (_prep_time 0)
		   (_post_time 0))
	       (do-subsets (_block _choice _blocksize _blockno)
		 (let ((_blockstart (elapsed-time))
		       (_phaseu #f)
		       (_prep_done #f)
		       (_core_done #f)
		       (_post_done #f))
		   (when _progressfn
		     (_progressfn
		      (* _blockno _blocksize) (choice-size _block)
		      (choice-size _choice) _nthreads _startu _phaseu
		      (elapsed-time _start) _prep_time _post_time #f #f #f))
		   (set! _phaseu (rusage))
		   (cond ((not _blockproc))
			 ((,procedure? _blockproc) (_blockproc (qc _block) #f))
			 ((and (vector? _blockproc) (> (length _blockproc) 0)
			       (,procedure? (elt _blockproc 0)))
			  ((elt _blockproc 0) (qc _block))))
		   (set! _prep_done (elapsed-time))
		   (when (and _progressfn _blockproc)
		     (_progressfn
		      (* _blockno _blocksize) (choice-size _block)
		      (choice-size _choice) _nthreads _startu _phaseu
		      (elapsed-time _start) _prep_time _post_time 
		      (- _prep_done _blockstart) #f #f))
		   (when _blockproc (set! _phaseu (rusage)))
		   (,mt-apply _nthreads _bodyproc (qc _block))
		   (set! _core_done (elapsed-time))
		   (when _progressfn
		     (_progressfn
		      (* _blockno _blocksize) (choice-size _block)
		      (choice-size _choice) _nthreads _startu _phaseu
		      (elapsed-time _start) _prep_time _post_time 
		      (- _prep_done _blockstart)
		      (- _core_done _prep_done) #f))
		   (when _blockproc (set! _phaseu (rusage)))
		   (cond ((not _blockproc))
			 ((,procedure? _blockproc) (_blockproc (qc _block) #t))
			 ((and (vector? _blockproc) (> (length _blockproc) 1)
			       (,procedure? (elt _blockproc 1)))
			  ((elt _blockproc 1) (qc _block))))
		   (set! _post_done (elapsed-time))
		   (when (and _blockproc _progressfn)
		     (_progressfn
		      (* _blockno _blocksize) (choice-size _block)
		      (choice-size _choice) _nthreads _startu _phaseu
		      (elapsed-time _start) _prep_time _post_time 
		      (- _prep_done _blockstart) (- _core_done _prep_done)
		      (- _post_done _core_done)))
		   (set! _prep_time (+ _prep_time (- _prep_done _blockstart)))
		   (set! _post_time (+ _post_time (- _post_done _core_done)))))
	       (cond ((not _blockproc))
		     ((,procedure? _blockproc) (_blockproc (qc) #t))
		     ((and (vector? _blockproc) (> (length _blockproc) 1)
			   (,procedure? (elt _blockproc 1)))
		      ((elt _blockproc 1) (qc) #t)))
	       (when _progressfn
		 (_progressfn
		  (choice-size _choice) 0 (choice-size _choice) _nthreads _startu #f
		  (elapsed-time _start) _prep_time _post_time 
		  #f #f #f))))))))

(define do-vector-mt
  (macro expr
    (let* ((control-spec (get-arg expr 1))
	   (arg (get-arg control-spec 0))
	   (vec-generator (get-arg control-spec 1))
	   (n-threads (get-arg control-spec 2 10)))
      (if (<= (length control-spec) 3)
	  `(,mt-apply ,n-threads
		      (lambda (,arg) ,@(cdr (cdr expr)))
		      (qc (elts (,vec-generator))))
	  (let ((blockproc (get-arg control-spec 3 #f))
		(blocksize (get-arg control-spec 4 #f))
		(progressfn (get-arg control-spec 5 (get-default-progressfn))))
	    (when (number? progressfn)
	      (set! progressfn (get-default-progressfn progressfn)))
	    `(let ((_vec ,vec-generator)
		   (_blocksize ,blocksize)
		   (_blockproc (,legacy-blockproc ,blockproc))
		   (_progressfn (,getprogressfn ,progressfn))
		   (_bodyproc (lambda (,arg) ,@(cdr (cdr expr))))
		   (_nthreads ,n-threads)
		   (_start (elapsed-time))
		   (_startu (rusage))
		   (_prep_time 0)
		   (_post_time 0))
	       (dotimes (_blockno (1+ (quotient (length _vec) _blocksize)))
		 (let ((_block (elts _vec (* _blockno _blocksize)
				     (min (* (1+ _blockno) _blocksize) (length _vec))))
		       (_blockstart (elapsed-time))
		       (_phaseu #f)
		       (_prep_done #f)
		       (_core_done #f)
		       (_post_done #f))
		   (when _progressfn
		     (_progressfn
		      (* _blockno _blocksize) (choice-size _block)
		      (length _vec) _nthreads _startu _phaseu
		      (elapsed-time _start) _prep_time _post_time #f #f #f))
		   (set! _phaseu (rusage))
		   (cond ((not _blockproc))
			 ((,procedure? _blockproc) (_blockproc (qc _block) #f))
			 ((and (vector? _blockproc) (> (length _blockproc) 0)
			       (,procedure? (elt _blockproc 0)))
			  ((elt _blockproc 0) (qc _block))))
		   (set! _prep_done (elapsed-time))
		   (when (and _blockproc _progressfn)
		     (_progressfn
		      (* _blockno _blocksize) (choice-size _block)
		      (length _vec) _nthreads _startu _phaseu
		      (elapsed-time _start) _prep_time _post_time 
		      (- _prep_done _blockstart) #f #f))
		   (when _blockproc (set! _phaseu (rusage)))
		   (,mt-apply _nthreads _bodyproc (qc _block))
		   (set! _core_done (elapsed-time))
		   (when _progressfn
		     (_progressfn
		      (* _blockno _blocksize) (choice-size _block)
		      (length _vec) _nthreads _startu _phaseu
		      (elapsed-time _start) _prep_time _post_time 
		      (- _prep_done _blockstart)
		      (- _core_done _prep_done) #f))
		   (when _blockproc (set! _phaseu (rusage)))
		   (cond ((not _blockproc))
			 ((,procedure? _blockproc) (_blockproc (qc _block) #t))
			 ((and (vector? _blockproc) (> (length _blockproc) 1)
			       (,procedure? (elt _blockproc 1)))
			  ((elt _blockproc 1) (qc _block))))
		   (set! _post_done (elapsed-time))
		   (when (and _blockproc _progressfn)
		     (_progressfn
		      (* _blockno _blocksize) (choice-size _block)
		      (length _vec) _nthreads  _startu _phaseu
		      (elapsed-time _start) _prep_time _post_time 
		      (- _prep_done _blockstart) (- _core_done _prep_done)
		      (- _post_done _core_done)))
		   (set! _prep_time (+ _prep_time (- _prep_done _blockstart)))
		   (set! _post_time (+ _post_time (- _post_done _core_done)))))
	       (cond ((not _blockproc))
		     ((,procedure? _blockproc) (_blockproc (qc) #t))
		     ((and (vector? _blockproc) (> (length _blockproc) 1)
			   (,procedure? (elt _blockproc 1)))
		      ((elt _blockproc 1) (qc) #t)))
	       (when _progressfn
		 (_progressfn
		  (length _vec) 0 (length _vec) _nthreads
		   _startu #f
		  (elapsed-time _start) _prep_time _post_time 
		  #f #f #f))))))))

;;; Progress reports

;;; The default method for reporting progress emits status reports
;;;  after each time consuming chunk, together with an initial and
;;;  a final report

(define (report-preamble block limit nthreads)
  (if (< block limit)
      (notify (if (config 'appid) (printout (config 'appid) ": "))
	      "Processing " (printnum limit) " items in "
	      (1+ (quotient limit block)) " chunks of "
	      (printnum block) " items using " nthreads
	      (if (= nthreads 1) " thread" " threads"))
      (notify (if (config 'appid) (printout (config 'appid) ": "))
	      "Processing " limit " items in one chunk using "
	      nthreads (if (= nthreads 1) " thread" " threads"))))

(define (showcpusage startu phaseu)
  (when (and startu phaseu)
    (printout ", cpusage=" (show% (cpusage phaseu) 100)
	      "/" (show% (cpusage startu) 100) " (phase/total)")))

(define (default-progress-report
	  count thisblock limit nthreads startu phaseu
	  time preptime posttime blockprep blocktime blockpost)
  (cond ((= count limit)
	 (notify (if (config 'appid) (printout (config 'appid) ": "))
		 "Processed all " (printnum limit) " elements "
		 "in " (short-interval-string time) " "
		 "with " (get% preptime time) "% ("
		 (short-interval-string preptime) ") in pre-processing, "
		 (let ((exectime (- time preptime posttime)))
		   (printout (get% exectime time) "% ("
			     (short-interval-string exectime)
			     ") in execution, "))
		 "and "
		 (get% posttime time) "% ("
		 (short-interval-string posttime) ") in post-processing."))
	((not (or blocktime blockprep blockpost))
	 (if (= count 0)
	     (report-preamble thisblock limit nthreads)
	     (let ((togo (* time (/ (- limit count) count))))
	       (notify (if (config 'appid) (printout (config 'appid) ": "))
		       "Processed " (get% count limit) "%: "
		       (printnum count) " of " (printnum limit) " items in "
		       (short-interval-string time)
		       (when (> count 0)
			 (printout ", ~"
			   (short-interval-string togo) " to go "
			   "(~" (short-interval-string (+ togo time))
			   " total)"))))))
	(blockpost
	 (let ((total (+ blockprep blocktime blockpost)))
	   (notify (if (config 'appid) (printout (config 'appid) ": "))
		   "Processed " (printnum thisblock) " items in "
		   (short-interval-string total)
		   "= " (short-interval-string blockprep)
		   " (" (get% blockprep total) "%) "
		   "+ " (short-interval-string blocktime)
		   " (" (get% blocktime total) "%) "
		   "+ " (short-interval-string blockpost)
		   " (" (get% blockpost total) "%)"
		   (if (and startu phaseu) (showcpusage startu phaseu)))))
	(blocktime
	 (notify (if (config 'appid) (printout (config 'appid) ": "))
		 "Finished core processing of " (printnum thisblock)
		 " items in " (short-interval-string blocktime)
		 " using " nthreads (if (= nthreads 1) " thread" " threads")
		 (if (and startu phaseu) (showcpusage startu phaseu))))
	(blockprep)
	(else)))
(define mt/default-progress default-progress-report)

(define (mt/sparse-progress
	 count thisblock limit nthreads startu phaseu
	 time preptime posttime blockprep blocktime blockpost)
  (cond ((= count limit)
	 (notify (if (config 'appid) (printout (config 'appid) ": "))
		 "Processed all " (printnum limit) " elements "
		 "in " (short-interval-string time) " "
		 "with " (get% preptime time) "% ("
		 (short-interval-string preptime) ") in pre-processing, "
		 (let ((exectime (- time preptime posttime)))
		   (printout (get% exectime time) "% ("
			     (short-interval-string exectime)
			     ") in execution, "))
		 "and "
		 (get% posttime time) "% ("
		 (short-interval-string posttime) ") in post-processing."))
	((not (or blocktime blockprep blockpost))
	 (if (= count 0)
	     (report-preamble thisblock limit nthreads)
	     (notify (if (config 'appid) (printout (config 'appid) ": "))
		     "Processed " (get% count limit) "%: "
		     (printnum count) " of " (printnum limit) " items in "
		     (short-interval-string time)
		     (when (> count 0)
		       (printout ", ~"
			 (short-interval-string (* time (/ (- limit count) count)))
			 " to go "
			 "(~" (short-interval-string (+ (* time (/ (- limit count) count)) time))
			 " total)")))))
	(blockpost)
	(blocktime)
	(blockprep)
	(else)))

(define (mt/detailed-progress
	 count thisblock limit nthreads startu phaseu
	 time preptime posttime blockprep blocktime blockpost)
  (cond ((= count limit)
	 (notify (if (config 'appid) (printout (config 'appid) ": "))
		 "Processed all " (printnum limit) " elements "
		 "in " (short-interval-string time) " "
		 "with " (get% preptime time) "% ("
		 (short-interval-string preptime) ") in pre-processing, "
		 (let ((exectime (- time preptime posttime)))
		   (printout (get% exectime time) "% ("
			     (short-interval-string exectime)
			     ") in execution, "))
		 "and "
		 (get% posttime time) "% ("
		 (short-interval-string posttime) ") in post-processing."))
	((not (or blocktime blockprep blockpost))
	 (if (= count 0)
	     (report-preamble thisblock limit nthreads)
	     (notify (if (config 'appid) (printout (config 'appid) ": "))
		     "Processed " (get% count limit) "%: "
		     (printnum count) " of " (printnum limit) " items in "
		     (short-interval-string time)
		     (when (> count 0)
		       (printout ", ~"
			 (short-interval-string (* time (/ (- limit count) count)))
			 " to go "
			 "(~" (short-interval-string (+ (* time (/ (- limit count) count)) time))
			 " total)")))))
	(blockpost
	 (notify (if (config 'appid) (printout (config 'appid) ": "))
		 "Finished post processing for " (printnum thisblock)
		 " items in " (short-interval-string blockpost))
	 (let ((total (+ blockprep blocktime blockpost)))
	   (notify (if (config 'appid) (printout (config 'appid) ": "))
		   "Processed " (printnum thisblock) " items in "
		   (short-interval-string total)
		   "= " (short-interval-string blockprep)
		   " (" (get% blockprep total) "%) "
		   "+ " (short-interval-string blocktime)
		   " (" (get% blocktime total) "%) "
		   "+ " (short-interval-string blockpost)
		   " (" (get% blockpost total) "%)"
		   (if (and startu phaseu) (showcpusage startu phaseu)))))
	(blocktime
	 (notify (if (config 'appid) (printout (config 'appid) ": "))
		 "Finished execution for " (printnum thisblock) " items in "
		 (short-interval-string blocktime)
		 (if (and startu phaseu) (showcpusage startu phaseu))))
	(blockprep
	 (notify (if (config 'appid) (printout (config 'appid) ": "))
		 "Finished preparation for " (printnum thisblock) " items in "
		 (short-interval-string blockprep)
		 (if (and startu phaseu) (showcpusage startu phaseu))))
	(else)))

(define (mt/noprogress
	 count thisblock limit nthreads startu phaseu
	 time preptime posttime blockprep blocktime blockpost))
(define (mt/no-progress
	 count thisblock limit nthreads startu phaseu
	 time preptime posttime blockprep blocktime blockpost))

(define (mt/custom-progress task)
  (lambda (count thisblock limit nthreads startu phaseu
		 time preptime posttime blockprep blocktime blockpost)
    (cond ((= count limit)
	   (notify task ": finished all " (printnum limit) " elements "
		   "in " (short-interval-string time) " "
		   "with " (get% preptime time) "% ("
		   (short-interval-string preptime) ") in pre-processing, "
		   (let ((exectime (- time preptime posttime)))
		     (printout (get% exectime time) "% ("
			       (short-interval-string exectime)
			       ") in execution, "))
		   "and "
		   (get% posttime time) "% ("
		   (short-interval-string posttime) ") in post-processing."))
	  ((not (or blocktime blockprep blockpost))
	   (unless (= count 0)
	     (let ((togo (* time (/ (- limit count) count))))
	       (notify task ": finished " (get% count limit) "% ("
		       (printnum count) "/" (printnum limit) ") in "
		       (short-interval-string time)
		       (when (> count 0)
			 (printout ", ~"
				   (short-interval-string togo) " to go "
				   "(~" (short-interval-string (+ togo time))
				   " total)"))))))
	  (else))))

;;; Verbosity configuration

(define verbosity #f)

(define (get-default-progressfn (v verbosity))
  (cond ((not (number? v)) mt/sparse-progress)
	((= v 0) mt/no-progress)
	((= v 1) mt/sparse-progress)
	((= v 2) mt/default-progress)
	(else mt/detailed-progress)))

(define (getprogressfn v)
  (if (number? v)
      (get-default-progressfn v)
      v))

(define (config-verbosity var (value))
  (if (bound? value)
      (if (number? value)
	  (begin (set! verbosity value) #t)
	  (if (not value) (set! verbosity 0)
	      (if (eq? value #t) (set! verbosity 3)
		  (set! verbosity 2))))
      verbosity))
(config-def! 'mtverbose config-verbosity)

;;; Utility prefetchers

(define (mt/save/swap/fetch oids done)
  (when done (commit) (swapout))
  (unless done (prefetch-oids! oids)))
(define (mt/save/fetch oids done)
  (when done (commit) (clearcaches))
  (unless done (prefetch-oids! oids)))
(define (mt/save/lock/fetch oids done)
  (when done (commit) (clearcaches))
  (unless done (prefetch-oids! oids)))
(define (mt/save/fetchkeys index)
  (lambda (keys done)
    (when done (commit) (clearcaches))
    (unless done (prefetch-keys! index keys))))

(define (mt/fetchoids f done)
  (when done (clearcaches))
  (unless done (prefetch-oids! f)))
(define (mt/lockoids f done)
  (when done (commit) (clearcaches))
  (unless done
    (lock-oids! f) (prefetch-oids! f)))



