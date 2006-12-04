(in-module 'mttools)
(use-module 'reflection)

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
      (stringout (if (< secs 10) secs (inexact->string secs 2))
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
		  ((> secs 10) (inexact->string seconds 2))
		  (else seconds)))))))

(define (default-progress-report count limit time blocktime)
  (when (> count 0)
    (notify "Processed " (get% count limit) "%: "
	    count " of " limit " items in "
	    (short-interval-string time)
	    (when (> count 0)
	      (printout ", ~ "
		(short-interval-string (* time (/ (- limit count) count)))
		" to go"))
	    "; " (get% blocktime time) "% (~"
	    (short-interval-string blocktime) ") in block setup")))

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
	  (let ((blockproc (get-arg control-spec 3 #t))
		(blocksize (get-arg control-spec 4 #f))
		(progressfn (get-arg control-spec 5 default-progress-report)))
	    `(let ((_choice ,choice-generator)
		   (_blocksize ,blocksize)
		   (_blockproc ,blockproc)
		   (_progressfn ,progressfn)
		   (_bodyproc (lambda (,arg) ,@(cdr (cdr expr))))
		   (_nthreads ,n-threads)
		   (_start (elapsed-time))
		   (_block_time 0))
	       (do-subsets (_block ,choice-generator _blocksize _blockno)
		 (when _progressfn
		   (_progressfn (* _blockno _blocksize)
				(choice-size _choice)
				(- (elapsed-time) _start)
				_block_time))
		 (let ((_blockstart (elapsed-time)))
		   (_blockproc (qc _block))
		   (set! _block_time
			 (+ _block_time (- (elapsed-time) _blockstart))))
		 (,mt-apply _nthreads _bodyproc (qc _block)))
	       (_blockproc (qc))
	       (when _progressfn
		 (_progressfn (choice-size _choice)
			      (choice-size _choice)
			      (- (elapsed-time) _start)
			      _block_time))))))))

(define (mt/save/fetch oids)
  (commit) (clearcaches)
  (prefetch-oids! oids))
(define (mt/save/lock/fetch oids)
  (commit) (clearcaches)
  (prefetch-oids! oids))
(define (mt/save/fetchkeys index)
  (lambda (keys)
    (commit) (clearcaches)
    (prefetch-keys! index keys)))

(module-export! '{mt-apply
		  do-choices-mt
		  interval-string
		  short-interval-string
		  mt/save/fetch
		  mt/save/lock/fetch
		  mt/save/fetchkeys})

