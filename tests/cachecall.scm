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

(define-tester (test-cache cache)
  (applytest #t eq? 
	     (cachecall cache identity "foo")
	     (cachecall cache identity "foo"))
  ;; (applytest #t eq? 
  ;; 	     (cachecall cache glom "foo" "bar")
  ;; 	     (cachecall cache glom "foo" "bar"))
  (applytest "foo" cachecall/probe cache identity "foo")
  ;; (applytest #t cachedcall? cache glom "foo" "bar")
  ;; (applytest #f cachedcall? cache glom "quux" "bar")
  (applytest #f cachedcall? cache + "quux" "bar")
  (applytest #t cachedcall? cache identity "foo")
  (applytest #f cachedcall? cache identity "bar")
  (applytest {} cachecall/probe cache identity "bar")
  ;; (applytest {} cachecall/probe cache glom "quux" "baz")
  )

(test-cache (make-hashtable))
(begin
  (when (file-exists? "callcache.index") (remove-file "callcache.index"))
  ;; This core test was moved from misctest.scm, but call caches need
  ;; to be generalized
  (let ((index (make-index "callcache.index" #[type fileindex slots 1000 create #t])))
    ;; (test-cache index)
    ;; (commit index)
    ;; (swapout index)
    ;; (applytest cachedcall? index glom "foo" "bar")
    (remove-file "callcache.index")))

(applytest #t cachedcall? identity "foo")
(applytest #f cachedcall? identity "bar")
(applytest "foo" cachecall/probe identity "foo")
(applytest {} cachecall/probe identity "bar")

(clear-callcache!)
(applytest #f cachedcall? identity "foo")

#|
(applytest #f eq? (thread/cachecall identity "foo") (thread/cachecall identity "foo"))
(with-threadcache 
 (applytest #t eq? 
	    (thread/cachecall identity "foo") 
	    (thread/cachecall identity "foo")))
|#

(applytest #f cachedcall? identity "foo")
(applytest {} cachecall/probe identity "foo")

(test-finished "CACHECALL test successfuly completed")

