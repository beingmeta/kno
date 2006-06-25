;; -*- Mode: Scheme; -*-
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; File:         takl.sch
; Description:  TAKL benchmark from the Gabriel tests
; Author:       Richard Gabriel
; Created:      12-Apr-85
; Modified:     12-Apr-85 10:07:00 (Bob Shaw)
;               22-Jul-87 (Will Clinger)
; Language:     Scheme
; Status:       Public Domain
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
 
;;; TAKL -- The TAKeuchi function using lists as counters.
 
(define (listn n)
  (if (not (= 0 n))
      (cons n (listn (- n 1)))
      '()))
 
(define l18 (listn 18))
(define l12 (listn 12))
(define  l6 (listn 6))
 
(define (mas x y z)
  (if (not (shorterp y x))
      z
      (mas (mas (cdr x)
                 y z)
            (mas (cdr y)
                 z x)
            (mas (cdr z)
                 x y))))
 
(define (shorterp x y)
  (and y (not (null? y))
       (or (null? x)
	   (shorterp (cdr x)
		     (cdr y)))))

;;; call: (mas l18 12l 6l)
 
(run-benchmark "TAKL" (lambda () (mas l18 l12 l6)))
