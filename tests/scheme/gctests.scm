;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module 'reflection)

(optimization-leaks)

;;; This catches "leaks" in optional arguments

;;; In the error case we're testing for, each call to testfn increfs
;;; constval but fails to decref it.  Eventually this overflows the 
;;; refcount field in the string struct.

(define constval "const string")
(define-tester (testfn x (y constval))
  x)
(define (testoptfree)
  (message "Running big REFOVERFLOW test")
  ;; This is enough to overflow the refcount for 32-bit consdata
  (dotimes (i 17000000) (testfn "bar"))
  (message "Finished big REFOVERFLOW test (whew)")
  #t)
(unless (or (getenv "MEMCHECKING") (getenv "HEAPCHECK"))
  (applytest #t testoptfree))

