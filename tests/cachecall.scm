;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(define call-count 0)
(define count-call (slambda () (set! call-count (1+ call-count))))
(define call-count 0)
(define ccall (slambda () (set! call-count (1+ call-count))))
(define-tester (square n) (ccall) (* n n))
(evaltest 1 (let ((ccount call-count))
	      (cachecall square 4) (cachecall square 4)
	      (cachecall square 4) (cachecall square 4)
	      (- call-count ccount)))
(define-tester (identity x) x)
(applytest #f eq? (identity "foo") (identity "foo"))
(applytest #t eq? (cachecall identity "foo") (cachecall identity "foo"))

(test-finished "CACHECALL test successfuly completed")

