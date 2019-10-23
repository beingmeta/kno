#!./xrun ./knox
;;; -*- Mode: Scheme; -*-

(use-module '{bench/miscfns optimize logger 
	      reflection varconfig stringfmts})
(config! 'optlevel 4)
(config! 'optimize:keepsource #f)

(define (fibtest (n 50) (cycles 5000) . more)
  (let ((start (elapsed-time))
	(runtime #f))
    (dotimes (i cycles) (fibi n))
    (set! runtime (elapsed-time start))
    (message "(fibi " n ") x " cycles " in " (secs->string runtime #f))
    (while (and (pair? more)  (pair? (cdr more)))
      (let ((n (car more)) (cycles (cadr more)))
	(set! more (cddr more))
	(set! start (elapsed-time))
	(dotimes (i cycles) (fibi n))
	(set! runtime (elapsed-time start))
	(message "(fibi " n ") x " cycles " in " (secs->string runtime #f))
	))))

(when (config 'optimized #t config:boolean)
  (logwarn |Optimizing| (get-source))
  (optimize! 'bench/miscfns)
  (optimize!))

(define main fibtest)
