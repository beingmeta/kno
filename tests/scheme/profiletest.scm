;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{reflection bench/miscfns optimize})

(define fib-iter (importvar 'bench/miscfns 'fib-iter))
(define profiled {fibi + fibr fib-iter})

(do-choices (fn profiled) (config! 'profiled fn))

(dotimes (i 10) (fibi 1000))

(applytest #t > (elt (reflect/getcalls fib-iter) 3) (elt (reflect/getcalls fibi) 3) )
;; "More" time is spent in fib-iter than bigi, because fibi is tail recursive
(applytest #t < (elt (reflect/getcalls fibi) 2) (elt (reflect/getcalls fib-iter) 2) )
(applytest #t > (elt (reflect/getcalls fib-iter) 3) (elt (reflect/getcalls +) 3) )

(reflect/profile-reset! profiled)

(applytest 0.0 (elt (reflect/getcalls +) 1))
(applytest 0 (elt (reflect/getcalls +) 2))
(applytest 0 (elt (reflect/getcalls +) 3))

(set-procedure-tailable! fibi #f)

(dotimes (i 10) (fibi 1000))

;; Since we've made fibi not tailable, the time for fibi is not greater than fib-iter
(applytest #t > (elt (reflect/getcalls fibi) 2) (elt (reflect/getcalls fib-iter) 2) )
