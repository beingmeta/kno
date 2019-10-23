#!./xrun ./knox
;;; -*- Mode: Scheme; -*-

(use-module '{bench/miscfns optimize stringfmts})
(config! 'optimize:keepsource #f)

(optimize! 'bench/miscfns)

(define (fibtest (n 50) (cycles 5000) . more)
  (let ((start (elapsed-time))
	(runtime #f))
    (dotimes (i cycles) (balancer n))
    (set! runtime (elapsed-time start))
    (message "(balancer " n ") x " cycles " in " (secs->string runtime #f))
    (while (and (pair? more)  (pair? (cdr more)))
      (let ((n (car more)) (cycles (cadr more)))
	(set! more (cddr more))
	(set! start (elapsed-time))
	(dotimes (i cycles) (balancer n))
	(set! runtime (elapsed-time start))
	(message "(balancer " n ") x " cycles " in " (secs->string runtime #f))
	))))

(optimize!)

(define main fibtest)


