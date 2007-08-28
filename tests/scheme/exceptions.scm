;;; -*- Mode: scheme; text-encoding: latin-1; -*-

(evaltest 2 (let ((count 0))
	      (dynamic-wind
		  (lambda () (set! count (1+ count)))
		  (lambda () (+ 2 3))
		  (lambda () (set! count (1+ count))))
	      count))
(evaltest 2 (let ((count 0))
	      (onerror
	       (dynamic-wind
		   (lambda () (set! count (1+ count)))
		   (lambda () (+ 2 'a))
		   (lambda () (set! count (1+ count))))
	       #f)
	      count))
(evaltest '|Type error|
	  (onerror (+ 2 'a)
		   (lambda (ex) (exception-condition ex))))
(evaltest '|too few arguments|
	  (onerror (cons 8)
		   (lambda (ex) (exception-condition ex))))

(message "EXCEPTIONS tests successfuly completed")
