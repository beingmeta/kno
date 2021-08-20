;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module 'text/stringfmts)

(errtest (let ((x 3)) . body))
(errtest (let foo))
(errtest (let* foo 3))
(errtest (letrec foo 3))
(errtest (let 'foo 3))
(errtest (let* 'foo 3))
(errtest (letrec 'foo 3))

(errtest (do foo (#t 3)))
(errtest (do 'foo (#t 3)))
(errtest (do ((x)) x))
(errtest (do ((x 9)) () x))
(errtest (do (("x" 9)) (#t) x))
(errtest (do ((x 9)) (#t . exit) x))
(errtest (do ((x 9)) (#t xyzz8) x))
(errtest (do ((x 3 (1+ x))) . body))
(errtest (do ((x 3 (1+ x))) . body))

(evaltest 4 (do ((x 0 (1+ x))) ((> x 3) x)))
(evaltest 4 (do ((x 0)) ((> x 3) x) (set! x (1+ x))))

(define zz8 9)

(errtest (set!))
(errtest (set! zz8))
(errtest (set! "zz8"))
(errtest (set! zz8 (error 'justbecause)))
(evaltest 88 (begin (let ((z 3)) (set! zz8 88)) zz8))

(errtest (set+!))
(errtest (set+! zz8))
(errtest (set+! "zz8"))
(errtest (set+! zz8 (error 'justbecause)))
(evaltest {88 99} (begin (let ((z 3)) (set+! zz8 99)) zz8))

(errtest (do ((x (error 'just-because) (1+ x))) (#t 'good)))
(errtest (do ((x 3 (error 'just-because))) ((> x 3) 'good) (set! x (+ x 1))))
(errtest (do ((x 3 (1+ x))) ((error 'just-because) 'good)))
(errtest (do ((x 3 (1+ x)) (fn (lambda (y) (+ x y))))
	     ((error 'just-because) 'good)))

(define (bad-fn (x))
  (default! y 9)
  y)
(errtest (bad-fn))

(define (bad-fn-2 (x))
  (default! x (error 'just-because))
  x)
(define (bad-fn-3 (x))
  (default! x)
  x)
(define (bad-fn-4 (x))
  (default! x . bad-syntax)
  x)
(errtest (bad-fn-2))
(errtest (bad-fn-3))
(errtest (bad-fn-4))

(define (ok-fn-5 (x))
  (let ((y 3))
    (default! x y))
  x)
(applytest 3 ok-fn-5)

(define (bad-fn-6 (x))
  
  x)
(applytest 'err (lambda ((x)) (default!)))
(applytest 'err (lambda ((x)) (default! x)))
(applytest 'err (lambda ((x)) (default! x (error 'justbecause))))

(errtest (let ((x 3)) (defimport $count 'text/stringfmts)))
(errtest (defimport $count-chocula 'text/stringfmts))

;;; Defaults and locals

(define-tester (addbar x (y))
  ;; Check for leaks when the default value was actually provided
  (default! y (dbg (deep-copy "bar")))
  (glom x y))
(applytester "foobar" addbar "foo" "bar")
(applytester "foobaz" addbar "foo" "baz")
(applytester "foobar" addbar "foo")
