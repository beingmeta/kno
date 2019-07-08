;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

;;;; Testing COND apply

(define-tester (cond-tester x)
  (cond ((number? x) x)
	((string? x) => list) 
	((symbol? x) => messedup)
	(else =>)))
(applytester 9 cond-tester 9)
(applytester '(#t) cond-tester "string")
(errtest (cond-tester 'symbol))
(errtest (cond-tester '(pair)))

(define-tester (cond-tester-2 x)
  (cond ((number? x) x)
	((string? x) => list) 
	((symbol? x) => messedup)
	(else => "foo")))
(applytest 'err cond-tester-2 '(pair))

(define-tester (cond-tester-3 x)
  (cond ((number? x) x)
	((string? x) => list) 
	((symbol? x) => messedup)
	(else => . "foo")))
(applytest 'err cond-tester-3 '(pair))

(evaltest 'err (case))
(evaltest 'err (case 33))


