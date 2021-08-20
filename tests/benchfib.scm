#!./xrun ./knox
;;; -*- Mode: Scheme; -*-

(use-module '{bench/miscfns optimize logger 
	      reflection varconfig text/stringfmts})
(config! 'optlevel 4)
(config! 'optimize:keepsource #f)

(define miscfns (get-module 'bench/miscfns))

(define (get-fibfn)
  (cond ((not (config 'FIBFN)) fibi)
	((symbol? (config 'FIBFN)) (get miscfns (config 'FIBFN)))
	((string? (config 'FIBFN)) 
	 (get miscfns (string->symbol (config 'FIBFN))))))

(define (fibtest (cycles 5000) (n 50) . more)
  (let* ((start (elapsed-time))
	 (fibfn (get-fibfn))
	 (fibname (or (procedure-name fibfn) (config 'fibfn) "fib?"))
	 (expect (config 'expect (fibi n)))
	 (runtime #f))
    (dotimes (i cycles) 
      (unless (eq? expect (fibfn n)) 
	(error |BadResult| "Function " fibname " didn't return " expected)))
    (set! runtime (elapsed-time start))
    (message cycles " x " "(" fibname " " n ") in " (secs->string runtime #f))
    (while (and (pair? more)  (pair? (cdr more)))
      (let ((n (car more)) (cycles (cadr more)))
	(set! more (cddr more))
	(set! start (elapsed-time))
	(dotimes (i cycles) 
	  (unless (eq? expect (fibfn n)) 
	    (error |BadResult| "Function " fibname " didn't return " expected)))
	(set! runtime (elapsed-time start))
	(message "(" fibname " " n ") x " cycles " in " (secs->string runtime #f))
	))))

(when (config 'optimized #t config:boolean)
  (logwarn |Optimizing| (get-source))
  (optimize! 'bench/miscfns)
  (optimize-locals!))

(define main fibtest)
