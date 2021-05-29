;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2016-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'bench)

(use-module '{optimize varconfig logger reflection})

(define default-repeat 20)
(varconfig! bench:repeat default-repeat)
(varconfig! repeat default-repeat)

(define bench-before #f)
(varconfig! bench:setup bench-before)

(define bench-after #f)
(varconfig! bench:cleanup bench-after)

(define bench-log #t)
(varconfig! bench:log bench-log)

(module-export! '{bench})

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



