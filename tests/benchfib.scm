;;; -*- Mode: Scheme; -*-

(use-module '{bench/miscfns optimize stringfmts})
(optimize! 'bench/miscfns)

(define (fibtest n cycles)
  (let ((start (elapsed-time))
	(runtime #f))
    (dotimes (i cycles) (fibi n))
    (set! runtime (elapsed-time start))
    (message "(fibi " n ") x " cycles " in " (secs->string runtime #f))))

(optimize!)

(define (main)
  (fibtest 10 5000)
  (fibtest 100 5000)
  (fibtest 200 5000)
  (fibtest 500 5000))


