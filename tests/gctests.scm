;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

;;; This catches "leaks" in optional arguments

;;; In the error case we're testing for, each call to testfn increfs
;;; constval but fails to decref it.  Eventually this overflows the 
;;; refcount field in the string struct.

(define z '{a b c d})
(reftest z (lambda () (intersection z 'a)))
(reftest z (lambda () (intersection z 'a)) #t)
(reftest z (lambda () (intersection 'a z)))
(reftest z (lambda () (intersection 'a z)) #t)
