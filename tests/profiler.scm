;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{reflection bench/miscfns optimize})

(define fib-iter (importvar 'bench/miscfns 'fib-iter))
(define profiled {fibi + fibr fib-iter})

(do-choices (fn profiled) (config! 'profiled fn))

(dotimes (i 10) (fibi 1000))

(applytest #t > (profile/time (profile/getcalls fib-iter)) (profile/time (profile/getcalls fibi)) )
;; "More" time is spent in fib-iter than fibi, because fibi is tail recursive
(applytest #t < (profile/nsecs (profile/getcalls fibi)) (profile/nsecs (profile/getcalls fib-iter)) )
(applytest #t > (profile/ncalls (profile/getcalls fib-iter)) (profile/ncalls (profile/getcalls +)) )

(profile/reset! profiled)

(applytest 0.0 (profile/time (profile/getcalls +)))
(applytest 0 (profile/nsecs (profile/getcalls +)))
(applytest 0 (profile/ncalls (profile/getcalls +)))

(set-procedure-tailable! fibi #f)

(dotimes (i 10) (fibi 1000))

;; Since we've made fibi not tailable, the time for fibi is not greater than fib-iter
(applytest #t > (profile/time (profile/getcalls fibi)) (profile/time (profile/getcalls fib-iter)) )

(test-finished "PROFILETEST")
