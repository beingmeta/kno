;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2016-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu).

(in-module 'bench)

(use-module '{optimize varconfig logger ezrecords reflection kno/mttools})

(define default-repeat 20)
(varconfig! bench:repeat default-repeat)
(varconfig! repeat default-repeat)

(define default-trials 5)
(varconfig! bench:trials default-repeat)
(varconfig! trials default-repeat)

(define bench-before #f)
(varconfig! bench:setup bench-before)

(define bench-after #f)
(varconfig! bench:cleanup bench-after)

(define bench-log #t)
(varconfig! bench:log bench-log)

(module-export! '{bench bench-stats})

(define (benchn repeat benchmark-name fn . args)
  (when (applicable? benchmark-name)
    (set! args (cons fn args))
    (set! fn benchmark-name)
    (set! benchmark-name 
      (or (procedure-name benchmark-name) benchmark-name)))
  (let ((start (timestamp)))
    (dotimes (i repeat) (apply fn args))
    (let ((time (difftime (timestamp) start)))
      (logwarn |Benchmark| benchmark-name 
	" (" repeat " cycles) took " time " seconds"))))

(define (log-bench name fcn repeat opts total before after)
  (lineout ";; [" (timestring) "] Benchmark " (if name (printout "'" name "'"))
    " took " total " seconds for " repeat " iterations."))

(define (update-usage usage before after opts)
  "This will accumulate the appropriate fields on *usage*"
  (do-choices (field '{utime stime faults switches waits})
    (when (and (test before field) (test after field))
      (store! usage field
	(+ (try (get usage field) 0)
	   (- (get after field) (get before field))))))
  #f)

(define (bench spec . args)
  (local opts (if (opts? spec) spec #f)
	 repeat (if (number? spec) spec
		    (getopt opts 'repeat default-repeat))
	 fcn (if (applicable? spec) spec
		 (or (and (pair? args) (applicable? (car args))
			  (prog1 (car args) (set! args (cdr args))))
		     (getopt opts 'function)))
	 name (if (or (string? spec) (symbol? spec)) spec
		  (getopt opts 'name (and fcn (procedure-name fcn)))))
  (cond ((not (applicable? fcn))
	 (if (not fcn) (irritant fcn |NotAFunction| bench)
	     (error |NoFunctionProvided|)))
	((not (and (integer? repeat) (> repeat 0)))
	 (irritant repeat |BadRepeat| bench)))
  (let ((startup (getopt opts 'startup bench-before))
	(cleanup (getopt opts 'cleanup bench-after))
	(usage (and (getopt opts 'rusage) #[]))
	(log (getopt opts 'log bench-log))
	(getargs (getopt opts 'getargs))
	(start (elapsed-time))
	(before #f)
	(after #f)
	(total 0))
    (dotimes (i repeat)
      (when startup (startup))
      (when usage (set! before (rusage)))
      (when getargs (set! args (getargs)))
      (set! start (elapsed-time))
      (apply fcn args)
      (set! total (+ total (elapsed-time start)))
      (when usage (set! after (rusage)))
      (when cleanup (cleanup))
      (when usage (update-usage usage before after opts)))
    (when log
      (if (string? log) 
	  (printout-to (open-output-file log #[append #t])
	    (log-bench name fcn repeat opts total before after))
	  (log-bench name fcn repeat opts total before after)))
    (when usage (store! usage 'total total))
    (if usage usage total)))

(defambda (bench-rusage fields)
  (if (singleton? fields)
      (if (not fields)
	  (elapsed-time)
	  (frame-create #f
	    'elapsed (elapsed-time)
	    fields (try (rusage fields) (get (rusage) fields))))
      (let ((usage (rusage))
	    (result (frame-create #f 'elapsed (elapsed-time))))
	(do-choices (field fields) (add! result field (get usage field)))
	result)))

(defrecord (callstats)
  thread
  trial
  arg
  iteration
  delta)

(define (default-deltafn v2 v1)
  (cond ((and (number? v1) (number? v2)) (- v2 v1))
	((and (table? v1) (table? v2))
	 (let ((delta (frame-create #f))
	       (keys (getkeys {v1 v2})))
	   (do-choices (key keys)
	     (let ((m1 (get v1 key)) (m2 (get v2 key)))
	       (store! delta
		   key (if (and (exists? m1) (exists? m2) (number? m1) (number? m2))
			   (- m2 m1)
			   (cons (qc m1) (qc m2))))))
	   delta))
	(else (cons v1 v2))))

(defambda (callstats thread trial arg repeat deltafn track before)
  (tryif before
    (modify-frame (deltafn (bench-rusage track) before)
      'thread thread 'trail trial 'repeat repeat 'arg arg)))

(defambda (bench-stats-threadfn thread fn workload (opts #f) (track) (deltafn))
  (default! track (getopt opts 'track #f))
  (default! deltafn (getopt opts 'deltafn #t))
  (when (and deltafn (not (applicable? deltafn)))
    (set! deltafn default-deltafn))
  (when (not deltafn) (set! deltafn cons))
  (let ((repeats (getopt opts 'repeat default-repeat))
	(trials (getopt opts 'trails default-trials))
	(results {})
	(v #f))
    (let ((repeat-start #f)
	  (value #f))
      (dotimes (trial trials)
	(do-choices (args workload)
	  (dotimes (repeat repeats)
	    (set! repeat-start (bench-rusage track))
	    (set! v #f)
	    (set! v (apply fn args))
	    (set+! results
	      (callstats thread trial args repeat deltafn track repeat-start))))))
    results))

(defambda (bench-stats fn workload (opts #f) (track) (deltafn) (nthreads))
  (default! track (getopt opts 'track #f))
  (default! deltafn (getopt opts 'deltafn #t))
  (default! nthreads (mt/threadcount (getopt opts 'nthreads #f)))
  (when (and deltafn (not (applicable? deltafn)))
    (set! deltafn default-deltafn))
  (when (not deltafn) (set! deltafn cons))
  (if (or (not nthreads) (= nthreads 1))
      (bench-stats-threadfn #f fn workload opts track deltafn)
      (let ((threads {}))
	(dotimes (i nthreads)
	  (set+! threads
	    (thread/call bench-stats-threadfn
		i fn (qc workload) opts (qc track) deltafn)))
	(thread/result (thread/join threads)))))

(defambda (aggregate-deltas records)
  (let ((aggregate (frame-create #f 'count (|| records))))
    (do-choices (measurement (getkeys records))
      (store! aggregate measurement
	(reduce-choice + records 0 measurement)))
    aggregate))

(defambda (get-aggregate records fcn)
  (if (vector? fcn)
      (get-compound-aggregate records fcn)
      (for-choices (val (fcn records))
	(modify-frame
	    (aggregate-deltas
	     (callstats-delta (pick records fcn val)))
	  (procedure-name fcn) val))))

(defambda (get-compound-aggregate records fnvec)
  (let ((table (make-hashtable)))
    (do-choices (record records)
      (add! table (map (lambda (fn) (fn record)) fnvec)
	record))
    (for-choices (val (getkeys table))
      (let ((aggregate
	     (aggregate-deltas
	      (callstats-delta (get table val)))))
	(dotimes (i (length fnvec))
	  (store! aggregate (procedure-name (elt fnvec i))
	    (elt val i)))
	aggregate))))

(module-export! 'get-aggregate)
