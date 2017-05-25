;; -*- Mode: Scheme; -*-
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; File:         dderiv.sch
; Description:  DDERIV benchmark from the Gabriel tests
; Author:       Vaughan Pratt
; Created:      8-Apr-85
; Modified:     10-Apr-85 14:53:29 (Bob Shaw)
;               23-Jul-87 (Will Clinger)
;               9-Feb-88 (Will Clinger)
; Language:     Scheme (but see note below)
; Status:       Public Domain
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
 
; Note:  This benchmark uses property lists.  The procedures that must
; be supplied are get and put, where (put x y z) is equivalent to Common
; Lisp's (setf (get x y) z).

;;; DDERIV -- Symbolic derivative benchmark written by Vaughn Pratt.
 
;;; This benchmark is a variant of the simple symbolic derivative program
;;; (DERIV). The main change is that it is `table-driven.'  Instead of using a
;;; large COND that branches on the CAR of the expression, this program finds
;;; the code that will take the derivative on the property list of the atom in
;;; the CAR position. So, when the expression is (+ . <rest>), the code
;;; stored under the atom '+ with indicator DERIV will take <rest> and
;;; return the derivative for '+. The way that MacLisp does this is with the
;;; special form: (DEFUN (FOO BAR) ...). This is exactly like DEFUN with an
;;; atomic name in that it expects an argument list and the compiler compiles
;;; code, but the name of the function with that code is stored on the
;;; property list of FOO under the indicator BAR, in this case. You may have
;;; to do something like:
 
;;; :property keyword is not Common Lisp.
 
; Returns the wrong answer for quotients.
; Fortunately these aren't used in the benchmark.
 
(in-module 'bench/dderiv)

(use-module 'bench/lispish)

(define (dderiv-aux a)
  (list '/ (dderiv a) a))
 
(define (+dderiv a)
  (cons '+ (map dderiv a)))
 
(pput '+ 'dderiv +dderiv)    ; install procedure on the property list
 
(define (-dderiv a)
  (cons '- (map dderiv a)))
 
(pput '- 'dderiv -dderiv)    ; install procedure on the property list
 
(define (*dderiv a)
  (list '* (cons '* a)
        (cons '+ (map dderiv-aux a))))
 
(pput '* 'dderiv *dderiv)    ; install procedure on the property list
 
(define (/dderiv a)
  (list '-
        (list '/
              (dderiv (car a))
              (cadr a))
        (list '/
              (car a)
              (list '*
                    (cadr a)
                    (cadr a)
                    (dderiv (cadr a))))))
 
(pput '/ 'dderiv /dderiv)    ; install procedure on the property list
 
(define (dderiv a)
  (cond
    ((not (pair? a))
     (cond ((eq? a 'x) 1) (else 0)))
    (else (let ((dderiv (pget (car a) 'dderiv)))
         (cond (dderiv (dderiv (cdr a)))
               (else 'error))))))
 
(define (dderiv/test)
  (do ((i 0 (+ i 1)))
      ((= i 1000))
    (dderiv '(+ (* 3 x x) (* a x x) (* b x) 5))
    (dderiv '(+ (* 3 x x) (* a x x) (* b x) 5))
    (dderiv '(+ (* 3 x x) (* a x x) (* b x) 5))
    (dderiv '(+ (* 3 x x) (* a x x) (* b x) 5))
    (dderiv '(+ (* 3 x x) (* a x x) (* b x) 5))))
 
(define benchfn dderiv/test)

(module-export! '{dderiv/test benchfn})

;;; call:  (run)
 
;; (run-benchmark "Dderiv" (lambda () (run)))
