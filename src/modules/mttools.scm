(in-module 'mttools)
(use-module 'reflection)

(define (legacy-blockproc proc)
  (if (= (fcn-arity proc) 1)
      (lambda (args done) (unless done (proc (qc args))))
      proc))

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

(define (get% num den (prec 2))
  (inexact->string (/ (* num 100.0) den) prec))

(define (interval-string secs (precise #t))
  (let* ((days (inexact->exact (floor (/ secs (* 3600 24)))))
	 (hours (inexact->exact
		 (floor (/ (- secs (* days 3600 24))
			   3600))))
	 (minutes (inexact->exact
		   (floor (/ (- secs (* days 3600 24) (* hours 3600))
			     60))))
	 (secs (- secs (* days 3600 24) (* hours 3600) (* minutes 60))))
    (stringout
	(cond ((= days 1) "one day, ")
	      ((> days 0) (printout days " days, ")))
      (cond ((= hours 1) "one hour, ")
	    ((> hours 0) (printout hours " hours, ")))
      (cond ((= minutes 1) "one minute, ")
	    ((> minutes 0) (printout minutes " minutes, ")))
      (cond ((= secs 1) "one second")
	    ((< secs 1) (printout secs " seconds"))
	    (else (printout (inexact->string secs 2) " seconds"))))))

(define (short-interval-string secs (precise #t))
  (if (< secs 180)
      (stringout (cond ((< secs 0) secs)
		       ((< secs 10) (inexact->string secs 3))
		       (else (inexact->string secs 2)))
	" secs")
      (let* ((days (inexact->exact (floor (/ secs (* 3600 24)))))
	     (hours (inexact->exact
		     (floor (/ (- secs (* days 3600 24))
			       3600))))
	     (minutes (inexact->exact
		       (floor (/ (- secs (* days 3600 24) (* hours 3600))
				 60))))
	     (seconds (- secs (* days 3600 24) (* hours 3600) (* minutes 60))))
	(stringout
	    (cond ((= days 1) "one day, ")
		  ((> days 0) (printout days " days, ")))
	  (when (> hours 0) (printout hours ":"))
	  (printout 
	      (if (and (> hours 0) (< minutes 10)) "0")
	    minutes ":")
	  (printout (if (< seconds 10) "0")
	    (cond ((> secs 600) (inexact->exact (round seconds)))
		  ((>= secs 10) (inexact->string seconds 2))
		  (else seconds)))))))

;;; The default method for reporting progress emits status reports
;;;  after each time consuming chunk, together with an initial and
;;;  a final report

(define (report-preamble block limit nthreads)
  (if (< block limit)
      (status "Processing " limit " items in "
	      (1+ (quotient limit block)) " chunks of "
	      block " items using " nthreads
	      (if (= nthreads 1) " thread" " threads"))
      (status "Processing " limit " items in one chunk using "
	      nthreads (if (= nthreads 1) " thread" " threads"))))

(define (default-progress-report count thisblock limit nthreads
	  time preptime posttime blockprep blocktime blockpost)
  (cond ((= count limit)
	 (status "Processed all " limit " elements "
		 " in " (short-interval-string time)
		 " with " (get% preptime time) "% ("
		 (short-interval-string preptime) ") in pre-processing and "
		 (get% preptime time) "% ("
		 (short-interval-string posttime) ") in post-processing."))
	((not (or blocktime blockprep blockpost))
	 (if (= count 0)
	     (report-preamble thisblock limit nthreads)
	     (let ((togo (* time (/ (- limit count) count))))
	       (status "Processed " (get% count limit) "%: "
		       count " of " limit " items in "
		       (short-interval-string time)
		       (when (> count 0)
			 (printout ", ~ "
			   (short-interval-string togo) " to go "
			   "(~" (short-interval-string (+ togo time))
			   " total)"))))))
	(blockpost
	 (let ((total (+ blockprep blocktime blockpost)))
	   (status "Processed " thisblock " items in " (short-interval-string total)
		   "; prep took " (short-interval-string blockprep) " (" (get% blockprep total) "%)"
		   ", exec took " (short-interval-string blocktime) " (" (get% blocktime total) "%)"
		   ", post took " (short-interval-string blockpost) " (" (get% blockpost total) "%)")))
	(blocktime
	 (status (config 'appid) ": "
		 "Finished core processing of " thisblock " items in " (short-interval-string blocktime)
		 " using " nthreads (if (= nthreads 1) " thread" " threads")))
	(blockprep)
	(else)))
(define mt/default-progress default-progress-report)

(define (mt/sparse-progress count thisblock limit nthreads
	  time preptime posttime blockprep blocktime blockpost)
  (cond ((= count limit)
	 (status "Processed all " limit " elements "
		 " in " (short-interval-string time)
		 " with " (get% preptime time) "% ("
		 (short-interval-string preptime) ") in pre-processing and "
		 (get% preptime time) "% ("
		 (short-interval-string posttime) ") in post-processing."))
	((not (or blocktime blockprep blockpost))
	 (if (= count 0)
	     (report-preamble thisblock limit nthreads)
	     (status "Processed " (get% count limit) "%: "
		     count " of " limit " items in "
		     (short-interval-string time)
		     (when (> count 0)
		       (printout ", ~ "
			 (short-interval-string (* time (/ (- limit count) count)))
			 " to go "
			 "(~" (short-interval-string (+ (* time (/ (- limit count) count)) time))
			 " total)")))))
	(blockpost)
	(blocktime)
	(blockprep)
	(else)))

(define (mt/detailed-progress
	 count thisblock limit nthreads
	 time preptime posttime blockprep blocktime blockpost)
  (cond ((= count limit)
	 (status "Processed all " limit " elements "
		 " in " (short-interval-string time)
		 " with " (get% preptime time) "% ("
		 (short-interval-string preptime) ") in pre-processing and "
		 (get% preptime time) "% ("
		 (short-interval-string posttime) ") in post-processing."))
	((not (or blocktime blockprep blockpost))
	 (if (= count 0)
	     (report-preamble thisblock limit nthreads)
	     (status "Processed " (get% count limit) "%: "
		     count " of " limit " items in "
		     (short-interval-string time)
		     (when (> count 0)
		       (printout ", ~ "
			 (short-interval-string (* time (/ (- limit count) count)))
			 " to go "
			 "(~" (short-interval-string (+ (* time (/ (- limit count) count)) time))
			 " total)")))))
	(blockpost
	 (status "Finished post processing for " thisblock " items in "
		 (short-interval-string blockpost))
	 (let ((total (+ blockprep blocktime blockpost)))
	   (status "Processed " thisblock " items in " (short-interval-string total)
		   "; prep took " (short-interval-string blockprep) " (" (get% blockprep total) "%)"
		   ", exec took " (short-interval-string blocktime) " (" (get% blocktime total) "%)"
		   ", post took " (short-interval-string blockpost) " (" (get% blockpost total) "%)")))
	(blocktime
	 (status "Finished execution for " thisblock " items in "
		 (short-interval-string blocktime)))
	(blockprep
	 (status "Finished preparation for " thisblock " items in "
		 (short-interval-string blockprep)))
	(else)))

(define (mt/noprogress count thisblock limit nthreads
	  time preptime posttime blockprep blocktime blockpost))

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
		(progressfn (get-arg control-spec 5 default-progress-report)))
	    `(let ((_choice ,choice-generator)
		   (_blocksize ,blocksize)
		   (_blockproc (,legacy-blockproc ,blockproc))
		   (_progressfn ,progressfn)
		   (_bodyproc (lambda (,arg) ,@(cdr (cdr expr))))
		   (_nthreads ,n-threads)
		   (_start (elapsed-time))
		   (_prep_time 0)
		   (_post_time 0))
	       (do-subsets (_block _choice _blocksize _blockno)
		 (let ((_blockstart (elapsed-time))
		       (_prep_done #f)
		       (_core_done #f)
		       (_post_done #f))
		   (when _progressfn
		     (_progressfn
		      (* _blockno _blocksize) (choice-size _block)
		      (choice-size _choice) _nthreads
		      (elapsed-time _start) _prep_time _post_time #f #f #f))
		   (cond ((not _blockproc))
			 ((,procedure? _blockproc) (_blockproc (qc _block) #f))
			 ((and (vector? _blockproc) (> (length _blockproc) 0)
			       (,procedure? (elt _blockproc 0)))
			  ((elt _blockproc 0) (qc _block))))
		   (set! _prep_done (elapsed-time))
		   (when _progressfn
		     (_progressfn
		      (* _blockno _blocksize) (choice-size _block)
		      (choice-size _choice) _nthreads
		      (elapsed-time _start) _prep_time _post_time 
		      (- _prep_done _blockstart) #f #f))
		   (,mt-apply _nthreads _bodyproc (qc _block))
		   (set! _core_done (elapsed-time))
		   (when _progressfn
		     (_progressfn
		      (* _blockno _blocksize) (choice-size _block)
		      (choice-size _choice) _nthreads
		      (elapsed-time _start) _prep_time _post_time 
		      (- _prep_done _blockstart)
		      (- _core_done _blockstart) #f))
		   (cond ((not _blockproc))
			 ((,procedure? _blockproc) (_blockproc (qc _block) #t))
			 ((and (vector? _blockproc) (> (length _blockproc) 1)
			       (,procedure? (elt _blockproc 1)))
			  ((elt _blockproc 1) (qc _block))))
		   (set! _post_done (elapsed-time))
		   (when _progressfn
		     (_progressfn
		      (* _blockno _blocksize) (choice-size _block)
		      (choice-size _choice) _nthreads
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
		  (choice-size _choice) 0 (choice-size _choice) _nthreads
		  (elapsed-time _start) _prep_time _post_time 
		  #f #f #f))))))))

(define (mt/save/fetch oids done)
  (when done (commit) (clearcaches))
  (unless done (prefetch-oids! oids)))
(define (mt/save/lock/fetch oids)
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

(module-export! '{mt-apply
		  do-choices-mt
		  interval-string
		  short-interval-string
		  mt/fetchoids
		  mt/lockoids
		  mt/save/fetch
		  mt/save/lock/fetch
		  mt/save/fetchkeys
		  mt/detailed-progress
		  mt/sparse-progress
		  mt/default-progress
		  mt/noprogress})

