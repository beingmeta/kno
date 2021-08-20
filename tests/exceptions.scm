;;; -*- Mode: scheme; text-encoding: utf-8; -*-

(load-component "common.scm")


#|
(evaltest 2 (let ((count 0))
	      (onerror
	       (dynamic-wind
		   (lambda () (set! count (1+ count)))
		   (lambda () (+ 2 'a))
		   (lambda () (set! count (1+ count))))
	       #f)
	      count))

(evaltest 2 (let ((count 0))
	      (dynamic-wind
		  (lambda () (set! count (1+ count)))
		  (lambda () (+ 2 3))
		  (lambda () (set! count (1+ count))))
	      count))
(evaltest '|Type error|
	  (onerror (+ 2 'a)
		   (lambda (ex) (error-condition ex))))
|#

(evaltest '|too few arguments|
	  (onerror (cons 8)
	      (lambda (ex) (error-condition ex))))

(test-finished "EXCEPTIONS")
