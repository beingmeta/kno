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

(define explicit-cache (make-hashtable))
(applytest #t eq? 
	   (cachecall explicit-cache identity "foo")
	   (cachecall explicit-cache identity "foo"))

(applytest #t cachedcall? identity "foo")
(applytest #t cachedcall? explicit-cache identity "foo")

(applytest #f cachedcall? identity "bar")
(applytest #f cachedcall? explicit-cache identity "bar")

(applytest "foo" cachecall/probe identity "foo")
(applytest {} cachecall/probe identity "bar")
(applytest {} cachecall/probe explicit-cache identity "bar")

(clear-callcache!)
(applytest #f cachedcall? identity "foo")

(applytest #f eq? (thread/cachecall identity "foo") (thread/cachecall identity "foo"))
(with-threadcache 
 (applytest #t eq? 
	    (thread/cachecall identity "foo") 
	    (thread/cachecall identity "foo")))

(applytest #f cachedcall? identity "foo")
(applytest {} cachecall/probe identity "foo")

(test-finished "CACHECALL test successfuly completed")

