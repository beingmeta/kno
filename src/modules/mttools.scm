(in-module 'mttools)
(use-module 'reflection)

(define (legacy-blockproc proc)
  (if (= (procedure-arity proc) 1)
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
		  ((>= secs 10) (inexact->string seconds 2))
		  (else seconds)))))))

;;; The default method for reporting progress emits status reports
;;;  after each time consuming chunk, together with an initial and
;;;  a final report

(define (default-progress-report count thisblock limit
	  time preptime blocktime blockprep)
  (cond ((and blocktime blockprep)
	 (status "Processed " (get% count limit) "%: "
		 count " of " limit " items in "
		 (short-interval-string time)
		 (when (> count 0)
		   (printout ", ~ "
		     (short-interval-string (* time (/ (- limit count) count)))
		     " to go"))
		 "; " (get% blocktime time) "% (~"
		 (short-interval-string blocktime) ") in setup."))
	(blockprep
	 (status "After processing " count " items "
		 "in " (short-interval-string (- time blockprep)) ", "
		 "finished setup for " thisblock " more items in "
		 (short-interval-string blockprep)))
	((= count 0)
	 (notify "Processing " limit " elements "
		 "in chunks of " thisblock " items"))
	((= count limit)
	 (status "Processed all " limit " elements "
		 " in " (short-interval-string time)
		 " with " (get% preptime time) "% (~"
		 (short-interval-string preptime) ") in block preparation."))
	(else)))

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
		   (_prep_time 0))
	       (do-subsets (_block _choice _blocksize _blockno)
		 (let ((_blockstart (elapsed-time)) (_block_prep #f))
		   (when _progressfn
		     (_progressfn (* _blockno _blocksize) (choice-size _block) (choice-size _choice)
				  (elapsed-time _start) _prep_time #f #f))
		   (cond ((not _blockproc))
			 ((applicable? _blockproc) (_blockproc (qc _block) #f))
			 ((and (vector? _blockproc) (> (length _blockproc) 0)
			       (applicable? (elt _blockproc 0)))
			  ((elt _blockproc 0) (qc _block))))
		   (set! _block_prep (elapsed-time _blockstart))
		   (set! _prep_time (+ _prep_time _block_prep))
		   (when _progressfn
		     (_progressfn (* _blockno _blocksize) (choice-size _block) (choice-size _choice)
				  (elapsed-time _start) _prep_time
				  #f _block_prep))
		   (,mt-apply _nthreads _bodyproc (qc _block))
		   (cond ((not _blockproc))
			 ((applicable? _blockproc) (_blockproc (qc _block) #t))
			 ((and (vector? _blockproc) (> (length _blockproc) 1)
			       (applicable? (elt _blockproc 1)))
			  ((elt _blockproc 1) (qc _block))))
		   (when _progressfn
		     (_progressfn (+ (* _blockno _blocksize) (choice-size _block))
				  (choice-size _block) (choice-size _choice)
				  (elapsed-time _start) _prep_time
				  (elapsed-time _blockstart) _block_prep))))
	       (when _blockproc (_blockproc (qc)))
	       (when _progressfn
		 (_progressfn (choice-size _choice) 0
			      (choice-size _choice)
			      (elapsed-time _start) _prep_time 0 0))))))))

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
		  mt/save/fetchkeys})

