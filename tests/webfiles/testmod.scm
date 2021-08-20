;;; -*- Module: Scheme; -*-

(in-module 'kno/tests/testmod)

(use-module '{logger varconfig})

(module-export! '{alt-minus})

(define (alt-minus x y) (- x y))

