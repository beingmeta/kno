;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(load-component "common.scm")

(use-module 'parseltongue)
(CONFIG! 'PYPATH (get-component "python"))

(define (pp x y) (+ x y))

(define py_plus (py/fcn "fdtest" "py_plus"))
(define py_times (py/fcn "fdtest" "py_times"))
(define py_divide (py/fcn "fdtest" "py_divide"))
(define doubled (py/fcn "fdtest" "call_doubled"))
(define py_triple (py/fcn "fdtest" "py_triple"))

(applytest 42 py_times 6 7)
(applytest 21 py_plus 14 7)
(applytest 2 py_divide 4 2)
(applytest 2 py_divide 5 2)
(applytest 2.5 py_divide 5.0 2)
(applytest #(3 1 4) py_triple 3 1 4)


