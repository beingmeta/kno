;; Copyright (C) 1991, 1992, 1993, 1994, 1995 Free Software Foundation, Inc.
;;
;; This program is free software; you can redistribute it and/or modify it
;; under the terms of the GNU General Public License as published by the
;; Free Software Foundation; either version 2, or (at your option) any
;; later version.
;;
;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; To receive a copy of the GNU General Public License, write to the
;; Free Software Foundation, Inc., 59 Temple Place - Suite 330,
;; Boston, MA 02111-1307, USA; or view
;; http://www-swiss.ai.mit.edu/~jaffer/GPL.html

;;;; "r4rstest.scm" Test correctness of scheme implementations.
;;; Author: Aubrey Jaffer

;;; This includes examples from
;;; William Clinger and Jonathan Rees, editors.
;;; Revised^4 Report on the Algorithmic Language Scheme
;;; and the IEEE specification.

;;; The input tests read this file expecting it to be named "r4rstest.scm".
;;; Files `tmp1', `tmp2' and `tmp3' will be created in the course of running
;;; these tests.  You may need to delete them in order to run
;;; "r4rstest.scm" more than once.

;;;   There are three optional tests:
;;; (TEST-CONT) tests multiple returns from call-with-current-continuation
;;;
;;; (TEST-SC4) tests procedures required by R4RS but not by IEEE
;;;
;;; (TEST-DELAY) tests DELAY and FORCE, which are not required by
;;;   either standard.

;;; If you are testing a R3RS version which does not have `list?' do:
;;; (define list? #f)

;;; send corrections or additions to jaffer@ai.mit.edu

(use-module 'reflection)

(define cur-section '())(define errs '())
(define SECTION (lambda args
		  (display "SECTION") (write args) (newline)
		  (set! cur-section args) #t))

(SECTION 2 1);; test that all symbol characters are supported.
'(+ - ... !.. $.+ %.- &.! *.: /:. :+. <-. =. >. ?. ~. _. ^.)

(SECTION 3 4)
(define disjoint-type-functions
  (list boolean? char? null? number? pair? procedure? string? symbol? vector?))
(define type-examples
  (list
   #t #f #\a '() 9739 '(applytest) SECTION "test" "" 'test '#() '#(a b c) ))
(define i 1)
(for-each (lambda (x) (display (make-string i #\ ))
		  (set! i (+ 3 i))
		  (write x)
		  (newline))
	  disjoint-type-functions)
(define type-matrix
  (map (lambda (x)
	 (let ((t (map (lambda (f) (f x)) disjoint-type-functions)))
	   (write t)
	   (write x)
	   (newline)
	   t))
       type-examples))
(set! i 0)
(define j 0)
(for-each (lambda (x y)
	    (set! j (+ 1 j))
	    (set! i 0)
	    (for-each (lambda (f)
			(set! i (+ 1 i))
			(cond ((and (= i j))
			       (cond ((not (f x))) (applytest #t f x)))
			      ((f x) (applytest #f f x)))
			(cond ((and (= i j))
			       (cond ((not (f y))) (applytest #t f y)))
			      ((f y) (applytest #f f y))))
		      disjoint-type-functions))
	  (list #t #\a '() 9739 '(applytest) SECTION "test" 'car '#(a b c))
	  (list #f #\newline '() -3252 '(t . t) car "" 'nil '#()))
(SECTION 4 1 2)
(applytest '(quote a) 'quote (quote 'a))
(applytest '(quote a) 'quote ''a)
(SECTION 4 1 3)
(applytest 12 (if #f + *) 3 4)
(SECTION 4 1 4)
(applytest 8 (lambda (x) (+ x x)) 4)
(define reverse-subtract
  (lambda (x y) (- y x)))
(applytest 3 reverse-subtract 7 10)
(define add4
  (let ((x 4))
    (lambda (y) (+ x y))))
(applytest 10 add4 6) ; ??
(applytest '(3 4 5 6) (lambda x x) 3 4 5 6)
(applytest '(5 6) (lambda (x y . z) z) 3 4 5 6)
(SECTION 4 1 5)
(applytest 'yes 'if (if (> 3 2) 'yes 'no))
(applytest 'no 'if (if (> 2 3) 'yes 'no))
(applytest '1 'if (if (> 3 2) (- 3 2) (+ 3 2)))
(SECTION 4 1 6)
(define x 2)
(applytest 3 'define (+ x 1))
(set! x 4)
(applytest 5 'set! (+ x 1))
(SECTION 4 2 1)
(applytest 'greater 'cond (cond ((> 3 2) 'greater)
			   ((< 3 2) 'less)))
(applytest 'equal 'cond (cond ((> 3 3) 'greater)
			 ((< 3 3) 'less)
			 (else 'equal)))
(applytest 2 'cond (cond ((assv 'b '((a 1) (b 2))) => cadr)
		     (else #f)))
(applytest 'composite 'case (case (* 2 3)
			 ((2 3 5 7) 'prime)
			 ((1 4 6 8 9) 'composite)))
(applytest 'consonant 'case (case (car '(c d))
			 ((a e i o u) 'vowel)
			 ((w y) 'semivowel)
			 (else 'consonant)))
(applytest #t 'and (and (= 2 2) (> 2 1)))
(applytest #f 'and (and (= 2 2) (< 2 1)))
(applytest '(f g) 'and (and 1 2 'c '(f g)))
(applytest #t 'and (and))
(applytest #t 'or (or (= 2 2) (> 2 1)))
(applytest #t 'or (or (= 2 2) (< 2 1)))
(applytest #f 'or (or #f #f #f))
(applytest #f 'or (or))
(applytest '(b c) 'or (or (memq 'b '(a b c)) (+ 3 0)))
(SECTION 4 2 2)
(applytest 6 'let (let ((x 2) (y 3)) (* x y)))
(applytest 35 'let (let ((x 2) (y 3)) (let ((x 7) (z (+ x y))) (* z x))))
(applytest 70 'let* (let ((x 2) (y 3)) (let* ((x 7) (z (+ x y))) (* z x))))
(applytest 70 'let* (let ((x 2) (y 3)) (let* ((x (+ x 5)) (z (+ x y))) (* z x))))
; (applytest #t 'letrec (letrec ((even?
; 			   (lambda (n) (if (zero? n) #t (odd? (- n 1)))))
; 			  (odd?
; 			   (lambda (n) (if (zero? n) #f (even? (- n 1))))))
; 		   (even? 88)))
(define x 34)
; (applytest 5 'let (let ((x 3)) (define x 5) x))
; (applytest 34 'let x)
; (applytest 6 'let (let () (define x 6) x))
; (applytest 34 'let x)
; (applytest 7 'let* (let* ((x 3)) (define x 7) x))
; (applytest 34 'let* x)
; (applytest 8 'let* (let* () (define x 8) x))
; (applytest 34 'let* x)
;(applytest 9 'letrec (letrec () (define x 9) x))
;(applytest 34 'letrec x)
;(applytest 10 'letrec (letrec ((x 3)) (define x 10) x))
;(applytest 34 'letrec x)
(SECTION 4 2 3)
(define x 0)
(applytest 6 'begin (begin (set! x 5) (+ x 1)))
(SECTION 4 2 4)
; (applytest '#(0 1 2 3 4) 'do (do ((vec (make-vector 5))
; 			    (i 0 (+ i 1)))
; 			   ((= i 5) vec)
; 			 (vector-set! vec i i)))
(applytest 25 'do (let ((x '(1 3 5 7 9)))
	       (do ((x x (cdr x))
		    (sum 0 (+ sum (car x))))
		   ((null? x) sum))))
(applytest 1 'let (let foo () 1))
; (applytest '((6 1 3) (-5 -2)) 'let
;       (let loop ((numbers '(3 -2 1 6 -5))
; 		 (nonneg '())
; 		 (neg '()))
; 	(cond ((null? numbers) (list nonneg neg))
; 	      ((negative? (car numbers))
; 	       (loop (cdr numbers)
; 		     nonneg
; 		     (cons (car numbers) neg)))
; 	      (else
; 	       (loop (cdr numbers)
; 		     (cons (car numbers) nonneg)
; 		     neg)))))
(SECTION 4 2 6)
(applytest '(list 3 4) 'quasiquote `(list ,(+ 1 2) 4))
(applytest '(list a (quote a)) 'quasiquote (let ((name 'a)) `(list ,name ',name)))
(applytest '(a 3 4 5 6 b) 'quasiquote `(a ,(+ 1 2) ,@(map abs '(4 -5 6)) b))
(applytest '((foo 7) . cons)
	'quasiquote
	`((foo ,(- 10 3)) ,@(cdr '(c)) . ,(car '(cons))))

;;; sqt is defined here because not all implementations are required to
;;; support it.
(define (sqt x) (do ((i 0 (+ i 1))) ((> (* i i) x) (- i 1))))

(applytest '#(10 5 2 4 3 8) 'quasiquote `#(10 5 ,(sqt 4) ,@(map sqt '(16 9)) 8))
(applytest 5 'quasiquote `,(+ 2 3))
(applytest '(a `(b ,(+ 1 2) ,(foo 4 d) e) f)
	   'quasiquote `(a `(b ,(+ 1 2) ,(foo ,(+ 1 3) d) e) f))
(applytest '(a `(b ,x ,'y d) e) 'quasiquote
	   (let ((name1 'x) (name2 'y)) `(a `(b ,,name1 ,',name2 d) e)))
(applytest '(list 3 4) 'quasiquote (quasiquote (list (unquote (+ 1 2)) 4)))
(applytest '`(list ,(+ 1 2) 4) 'quasiquote '(quasiquote (list (unquote (+ 1 2)) 4)))
(SECTION 5 2 1)
(define add3 (lambda (x) (+ x 3)))
(applytest 6 'define (add3 3))
(define first car)
(applytest 1 'define (first '(1 2)))
(define old-+ +)
(define + (lambda (x y) (list y x)))
(applytest '(3 6) add3 6)
(set! + old-+)
(applytest 9 add3 6)
(SECTION 5 2 2)
; (applytest 45 'define
; 	(let ((x 5))
; 		(define foo (lambda (y) (bar x y)))
; 		(define bar (lambda (a b) (+ (* a b) a)))
; 		(foo (+ x 3))))
(define x 34)
; (define (foo) (define x 5) x)
; (applytest 5 foo)
(applytest 34 'define x)
; (define foo (lambda () (define x 5) x))
; (applytest 5 foo)
(applytest 34 'define x)
; (define (foo x) ((lambda () (define x 5) x)) x)
; (applytest 88 foo 88)
; (applytest 4 foo 4)
(applytest 34 'define x)
(SECTION 6 1)
(applytest #f not #t)
(applytest #f not 3)
(applytest #f not (list 3))
(applytest #t not #f)
(applytest #f not '())
(applytest #f not (list))
(applytest #f not 'nil)

(applytest #t boolean? #f)
(applytest #f boolean? 0)
(applytest #f boolean? '())
(SECTION 6 2)
(applytest #t eqv? 'a 'a)
(applytest #f eqv? 'a 'b)
(applytest #t eqv? 2 2)
(applytest #t eqv? '() '())
(applytest #t eqv? '10000 '10000)
(applytest #f eqv? (cons 1 2)(cons 1 2))
(applytest #f eqv? (lambda () 1) (lambda () 2))
(applytest #f eqv? #f 'nil)
;; Break here.
(let ((p (lambda (x) x)))
  (applytest #t eqv? p p))
(define gen-counter
 (lambda ()
   (let ((n 0))
      (lambda () (set! n (+ n 1)) n))))
(let ((g (gen-counter))) (applytest #t eqv? g g))
(applytest #f eqv? (gen-counter) (gen-counter))
; (letrec ((f (lambda () (if (eqv? f g) 'f 'both)))
; 	 (g (lambda () (if (eqv? f g) 'g 'both))))
;   (applytest #f eqv? f g))

(applytest #t eq? 'a 'a)
(applytest #f eq? (list 'a) (list 'a))
(applytest #t eq? '() '())
(applytest #t eq? car car)
(let ((x '(a))) (applytest #t eq? x x))
(let ((x '#())) (applytest #t eq? x x))
(let ((x (lambda (x) x))) (applytest #t eq? x x))

(applytest #t equal? 'a 'a)
(applytest #t equal? '(a) '(a))
(applytest #t equal? '(a (b) c) '(a (b) c))
(applytest #t equal? "abc" "abc")
(applytest #t equal? 2 2)
(applytest #t equal? (make-vector 5 'a) (make-vector 5 'a))
(SECTION 6 3)
(applytest '(a b c d e) 'dot '(a . (b . (c . (d . (e . ()))))))
(define x (list 'a 'b 'c))
(define y x)
(and list? (applytest #t list? y))
; (set-cdr! x 4)
; (applytest '(a . 4) 'set-cdr! x)
; (applytest #t eqv? x y)
(applytest '(a b c . d) 'dot '(a . (b . (c . d))))
;(and list? (applytest #f list? y))
;(and list? (let ((x (list 'a))) (set-cdr! x x) (applytest #f 'list? (list? x))))

(applytest #t pair? '(a . b))
(applytest #t pair? '(a . 1))
(applytest #t pair? '(a b c))
(applytest #f pair? '())
(applytest #f pair? '#(a b))

(applytest '(a) cons 'a '())
(applytest '((a) b c d) cons '(a) '(b c d))
(applytest '("a" b c) cons "a" '(b c))
(applytest '(a . 3) cons 'a 3)
(applytest '((a b) . c) cons '(a b) 'c)

(applytest 'a car '(a b c))
(applytest '(a) car '((a) b c d))
(applytest 1 car '(1 . 2))

(applytest '(b c d) cdr '((a) b c d))
(applytest 2 cdr '(1 . 2))

(applytest '(a 7 c) list 'a (+ 3 4) 'c)
(applytest '() list)

(applytest 3 length '(a b c))
(applytest 3 length '(a (b) (c d e)))
(applytest 0 length '())

(applytest '(x y) append '(x) '(y))
(applytest '(a b c d) append '(a) '(b c d))
(applytest '(a (b) (c)) append '(a (b)) '((c)))
(applytest '() append)
;(applytest '(a b c . d) append '(a b) '(c . d))
;(applytest 'a append '() 'a)

(applytest '(c b a) reverse '(a b c))
(applytest '((e (f)) d (b c) a) reverse '(a (b c) d (e (f))))

(define list-ref elt)
(applytest 'c list-ref '(a b c d) 2)

(applytest '(a b c) memq 'a '(a b c))
(applytest '(b c) memq 'b '(a b c))
(applytest '#f memq 'a '(b c d))
(applytest '#f memq (list 'a) '(b (a) c))
(applytest '((a) c) member (list 'a) '(b (a) c))
(applytest '(101 102) memv 101 '(100 101 102))

(define e '((a 1) (b 2) (c 3)))
(applytest '(a 1) assq 'a e)
(applytest '(b 2) assq 'b e)
(applytest #f assq 'd e)
(applytest #f assq (list 'a) '(((a)) ((b)) ((c))))
(applytest '((a)) assoc (list 'a) '(((a)) ((b)) ((c))))
(applytest '(5 7) assv 5 '((2 3) (5 7) (11 13)))
(SECTION 6 4)
(applytest #t symbol? 'foo)
(applytest #t symbol? (car '(a b)))
(applytest #f symbol? "bar")
(applytest #t symbol? 'nil)
(applytest #f symbol? '())
(applytest #f symbol? #f)
;;; But first, what case are symbols in?  Determine the standard case:
(define char-standard-case char-upcase)
(if (string=? (symbol->string 'A) "a")
    (set! char-standard-case char-downcase))
(applytest #t 'standard-case
      (string=? (symbol->string 'a) (symbol->string 'A)))
(applytest #t 'standard-case
      (or (string=? (symbol->string 'a) "A")
	  (string=? (symbol->string 'A) "a")))
; (define (str-copy s)
;   (let ((v (make-string (string-length s))))
;     (do ((i (- (string-length v) 1) (- i 1)))
; 	((< i 0) v)
;       (string-set! v i (string-ref s i)))))
; (define (string-standard-case s)
;   (set! s (str-copy s))
;   (do ((i 0 (+ 1 i))
;        (sl (string-length s)))
;       ((>= i sl) s)
;       (string-set! s i (char-standard-case (string-ref s i)))))
; (applytest (string-standard-case "flying-fish") symbol->string 'flying-fish)
; (applytest (string-standard-case "martin") symbol->string 'Martin)
(applytest "Malvina" symbol->string (string->symbol "Malvina"))
(applytest #t 'standard-case (eq? 'a 'A))

(define x (string #\a #\b))
(define y (string->symbol x))
; (string-set! x 0 #\c)
; (applytest "cb" 'string-set! x)
(applytest "ab" symbol->string y)
(applytest y string->symbol "ab")

(applytest #t eq? 'mISSISSIppi 'mississippi)
(applytest #f 'string->symbol (eq? 'bitBlt (string->symbol "bitBlt")))
(applytest 'JollyWog string->symbol (symbol->string 'JollyWog))

(SECTION 6 5 5)
(applytest #t number? 3)
(applytest #t complex? 3)
(applytest #t real? 3)
(applytest #t rational? 3)
(applytest #t integer? 3)

(applytest #t exact? 3)
(applytest #f inexact? 3)

(applytest #t = 22 22 22)
(applytest #t = 22 22)
(applytest #f = 34 34 35)
(applytest #f = 34 35)
(applytest #t > 3 -6246)
(applytest #f > 9 9 -2424)
(applytest #t >= 3 -4 -6246)
(applytest #t >= 9 9)
(applytest #f >= 8 9)
(applytest #t < -1 2 3 4 5 6 7 8)
(applytest #f < -1 2 3 4 4 5 6 7)
(applytest #t <= -1 2 3 4 5 6 7 8)
(applytest #t <= -1 2 3 4 4 5 6 7)
(applytest #f < 1 3 2)
(applytest #f >= 1 3 2)

(applytest #t zero? 0)
(applytest #f zero? 1)
(applytest #f zero? -1)
(applytest #f zero? -100)
(applytest #t positive? 4)
(applytest #f positive? -4)
(applytest #f positive? 0)
(applytest #f negative? 4)
(applytest #t negative? -4)
(applytest #f negative? 0)
(applytest #t odd? 3)
(applytest #f odd? 2)
(applytest #f odd? -4)
(applytest #t odd? -1)
(applytest #f even? 3)
(applytest #t even? 2)
(applytest #t even? -4)
(applytest #f even? -1)

(applytest 38 max 34 5 7 38 6)
(applytest -24 min 3  5 5 330 4 -24)

(applytest 7 + 3 4)
(applytest '3 + 3)
(applytest 0 +)
(applytest 4 * 4)
(applytest 1 *)

(applytest -1 - 3 4)
(applytest -3 - 3)
(applytest 7 abs -7)
(applytest 7 abs 7)
(applytest 0 abs 0)

(applytest 5 quotient 35 7)
(applytest -5 quotient -35 7)
(applytest -5 quotient 35 -7)
(applytest 5 quotient -35 -7)
(applytest 1 modulo 13 4)
(applytest 1 remainder 13 4)
(applytest 3 modulo -13 4)
(applytest -1 remainder -13 4)
(applytest -3 modulo 13 -4)
(applytest 1 remainder 13 -4)
(applytest -1 modulo -13 -4)
(applytest -1 remainder -13 -4)
(define (divtest n1 n2)
  (= n1 (+ (* n2 (quotient n1 n2))
	   (remainder n1 n2))))
(applytest #t divtest 238 9)
(applytest #t divtest -238 9)
(applytest #t divtest 238 -9)
(applytest #t divtest -238 -9)

(applytest 4 gcd 0 4)
(applytest 4 gcd -4 0)
(applytest 4 gcd 32 -36)
;(applytest 0 gcd)
(applytest 288 lcm 32 -36)
;(applytest 1 lcm)

;;;;From: fred@sce.carleton.ca (Fred J Kaudel)
;;; Modified by jaffer, and Ken Haase.
(define (test-inexact)
  (let* ((f3.9 (string->number "3.9"))
	 (f4.0 (string->number "4.0"))
	 (f5.0 (string->number "5.0"))
	 (f-3.25 (string->number "-3.25"))
	 (f.25 (string->number "0.25"))
	 (f4.5 (string->number "4.5"))
	 (f3.5 (string->number "3.5"))
	 (f0.0 (string->number "0.0"))
	 (f0.8 (string->number "0.8"))
	 (f1.0 (string->number "1.0"))
	 (tinyfloat (string->number "2.0e-10"))
	 (reallytinyfloat (string->number "2.0e-20"))
	 (bigfloat (string->number "2.0e+10"))
	 (reallybigfloat (string->number "2.0e+20"))
	 (write-test-obj (list f.25 f-3.25))
	 (display-test-obj (list f.25 f-3.25))
	 (load-test-obj (list 'define 'foo (list 'quote write-test-obj))))
    (newline)
    (display ";testing inexact numbers; ")
    (newline)
    (SECTION 6 5 5)
    (applytest #t inexact? f3.9)
    (applytest #t 'inexact? (inexact? (max f3.9 4)))
    (applytest f4.0 'max (max f3.9 4))
    (applytest f4.0 'exact->inexact (exact->inexact 4))
    (applytest (- f5.0) round (- f4.5))
    (applytest (- f4.0) round (- f3.5))
    (applytest (- f4.0) round (- f3.9))
    (applytest f0.0 round f0.0)
    (applytest f0.0 round f.25)
    (applytest f1.0 round f0.8)
    (applytest f4.0 round f3.5)
    (applytest f5.0 round f4.5)
    (applytest 0.0000000002 * 2 0.0000000001)
    (applytest 2.0e-10 * 2 1.0e-10)
    (applytest 0.0000000002 * 2 1.0e-10)
    (applytest tinyfloat * 2 1.0e-10)
    (applytest reallytinyfloat * 2 1.0e-20)
    (applytest bigfloat * 2 1.0e+9 10.0)
    (applytest reallybigfloat * 2 1.0e+19 10)
    (comment
     (applytest #t call-with-output-file
		"tmp3"
		(lambda (applytest-file)
		  (write-char #\; test-file)
		  (display write-test-obj test-file)
		  (newline test-file)
		  (write load-test-obj test-file)
		  (output-port? test-file)))
     (check-test-file "tmp3")
     (set! write-test-obj wto)
     (set! display-test-obj dto)
     (set! load-test-obj lto))
    (let ((x (string->number "4195835.0"))
	  (y (string->number "3145727.0")))
      (applytest #t 'pentium-fdiv-bug (> f1.0 (- x (* (/ x y) y)))))))

(define bigtest
  (lambda (n1 n2)
    (= n1 (+ (* n2 (quotient n1 n2))
	     (remainder n1 n2)))))
(define (test-bignums)
  (newline)
  (display ";TESTING BIGNUMS; ")
  (newline)
  (SECTION 6 5 5)


  
  (applytest 24691357975308642
	     + 12345678987654321 12345678987654321)
  (applytest 0 - 12345678987654321 12345678987654321)
  (applytest -24691357975308642
	     + -12345678987654321 -12345678987654321)
  (applytest 0 - -12345678987654321 -12345678987654321)
  (applytest 0 + 12345678987654321 -12345678987654321)
  (applytest 0 + -12345678987654321 12345678987654321)
  (applytest 24691357975308642
	     - 12345678987654321 -12345678987654321)
  (applytest -24691357975308642
	     - -12345678987654321 12345678987654321)

  (applytest 0 modulo 3333333333 3)
  (applytest 0 modulo 3333333333 -3)
  (applytest 0 remainder 3333333333 3)
  (applytest 0 remainder 3333333333 -3)
  (applytest 2 modulo 3333333332 3)
  (applytest -1 modulo 3333333332 -3)
  (applytest 2 remainder 3333333332 3)
  (applytest 2 remainder 3333333332 -3)
  (applytest 1 modulo -3333333332 3)
  (applytest -2 modulo -3333333332 -3)
  (applytest -2 remainder -3333333332 3)
  (applytest -2 remainder -3333333332 -3)

  (applytest 3 modulo 3 3333333333)
  (applytest 3333333330 modulo -3 3333333333)
  (applytest 3 remainder 3 3333333333)
  (applytest -3 remainder -3 3333333333)
  (applytest -3333333330 modulo 3 -3333333333)
  (applytest -3 modulo -3 -3333333333)
  (applytest 3 remainder 3 -3333333333)
  (applytest -3 remainder -3 -3333333333)

  (applytest 0 modulo -2177452800 86400)
  (applytest 0 modulo 2177452800 -86400)
  (applytest 0 modulo 2177452800 86400)
  (applytest 0 modulo -2177452800 -86400)
  (applytest #t bigtest 281474976710655 65535)
  (applytest #t bigtest 281474976710654 65535)
  (SECTION 6 5 6)
  (applytest 281474976710655 string->number "281474976710655")
  (applytest "281474976710655" number->string 281474976710655)
  (applytest -281474976710655 string->number "-281474976710655")
  (applytest "-281474976710655" number->string -281474976710655)
  (applytest 281474976710655 string->number "FFFFFFFFFFFF" 16)
  (applytest 4503599627370495 string->number "FFFFFFFFFFFFF" 16)
  (applytest -100000000000000010 'big-minus (- (- 100000000000000000) 10)))

(SECTION 6 5 6)
(applytest "0" number->string 0)
(applytest "100" number->string 100)
(applytest "100" number->string 256 16)
(applytest 100 string->number "100")
(applytest 256 string->number "100" 16)
(applytest #f string->number "")
(applytest #f string->number ".")
(applytest #f string->number "d")
(applytest #f string->number "D")
(applytest #f string->number "i")
(applytest #f string->number "I")
; (applytest #f string->number "3i")
; (applytest #f string->number "3I")
; (applytest #f string->number "33i")
; (applytest #f string->number "33I")
; (applytest #f string->number "3.3i")
; (applytest #f string->number "3.3I")
(applytest #f string->number "-")
(applytest #f string->number "+")

(SECTION 6 6)
(applytest #t eqv? '#\  #\Space)
(applytest #t eqv? #\space '#\Space)
(applytest #t char? #\a)
(applytest #t char? #\()
(applytest #t char? #\ )
(applytest #t char? '#\newline)

(applytest #f char=? #\A #\B)
(applytest #f char=? #\a #\b)
(applytest #f char=? #\9 #\0)
(applytest #t char=? #\A #\A)

(applytest #t char<? #\A #\B)
(applytest #t char<? #\a #\b)
(applytest #f char<? #\9 #\0)
(applytest #f char<? #\A #\A)

(applytest #f char>? #\A #\B)
(applytest #f char>? #\a #\b)
(applytest #t char>? #\9 #\0)
(applytest #f char>? #\A #\A)

(applytest #t char<=? #\A #\B)
(applytest #t char<=? #\a #\b)
(applytest #f char<=? #\9 #\0)
(applytest #t char<=? #\A #\A)

(applytest #f char>=? #\A #\B)
(applytest #f char>=? #\a #\b)
(applytest #t char>=? #\9 #\0)
(applytest #t char>=? #\A #\A)

(applytest #f char-ci=? #\A #\B)
(applytest #f char-ci=? #\a #\B)
(applytest #f char-ci=? #\A #\b)
(applytest #f char-ci=? #\a #\b)
(applytest #f char-ci=? #\9 #\0)
(applytest #t char-ci=? #\A #\A)
(applytest #t char-ci=? #\A #\a)

(applytest #t char-ci<? #\A #\B)
(applytest #t char-ci<? #\a #\B)
(applytest #t char-ci<? #\A #\b)
(applytest #t char-ci<? #\a #\b)
(applytest #f char-ci<? #\9 #\0)
(applytest #f char-ci<? #\A #\A)
(applytest #f char-ci<? #\A #\a)

(applytest #f char-ci>? #\A #\B)
(applytest #f char-ci>? #\a #\B)
(applytest #f char-ci>? #\A #\b)
(applytest #f char-ci>? #\a #\b)
(applytest #t char-ci>? #\9 #\0)
(applytest #f char-ci>? #\A #\A)
(applytest #f char-ci>? #\A #\a)

(applytest #t char-ci<=? #\A #\B)
(applytest #t char-ci<=? #\a #\B)
(applytest #t char-ci<=? #\A #\b)
(applytest #t char-ci<=? #\a #\b)
(applytest #f char-ci<=? #\9 #\0)
(applytest #t char-ci<=? #\A #\A)
(applytest #t char-ci<=? #\A #\a)

(applytest #f char-ci>=? #\A #\B)
(applytest #f char-ci>=? #\a #\B)
(applytest #f char-ci>=? #\A #\b)
(applytest #f char-ci>=? #\a #\b)
(applytest #t char-ci>=? #\9 #\0)
(applytest #t char-ci>=? #\A #\A)
(applytest #t char-ci>=? #\A #\a)

(applytest #t char-alphabetic? #\a)
(applytest #t char-alphabetic? #\A)
(applytest #t char-alphabetic? #\z)
(applytest #t char-alphabetic? #\Z)
(applytest #f char-alphabetic? #\0)
(applytest #f char-alphabetic? #\9)
(applytest #f char-alphabetic? #\space)
(applytest #f char-alphabetic? #\;)

(applytest #f char-numeric? #\a)
(applytest #f char-numeric? #\A)
(applytest #f char-numeric? #\z)
(applytest #f char-numeric? #\Z)
(applytest #t char-numeric? #\0)
(applytest #t char-numeric? #\9)
(applytest #f char-numeric? #\space)
(applytest #f char-numeric? #\;)

(applytest #f char-whitespace? #\a)
(applytest #f char-whitespace? #\A)
(applytest #f char-whitespace? #\z)
(applytest #f char-whitespace? #\Z)
(applytest #f char-whitespace? #\0)
(applytest #f char-whitespace? #\9)
(applytest #t char-whitespace? #\space)
(applytest #f char-whitespace? #\;)

(applytest #f char-upper-case? #\0)
(applytest #f char-upper-case? #\9)
(applytest #f char-upper-case? #\space)
(applytest #f char-upper-case? #\;)

(applytest #f char-lower-case? #\0)
(applytest #f char-lower-case? #\9)
(applytest #f char-lower-case? #\space)
(applytest #f char-lower-case? #\;)

(applytest #\. integer->char (char->integer #\.))
(applytest #\A integer->char (char->integer #\A))
(applytest #\a integer->char (char->integer #\a))
(applytest #\A char-upcase #\A)
(applytest #\A char-upcase #\a)
(applytest #\a char-downcase #\A)
(applytest #\a char-downcase #\a)
(SECTION 6 7)
(applytest #t string? "The word \"recursion\\\" has many meanings.")
(applytest #t string? "")
; (define f (make-string 3 #\*))
; (applytest "?**" 'string-set! (begin (string-set! f 0 #\?) f))
(applytest "abc" string #\a #\b #\c)
(applytest "" string)
(define string-length length)
(define string-ref elt)
(define substring subseq)
(applytest 3 string-length "abc")
(applytest #\a string-ref "abc" 0)
(applytest #\c string-ref "abc" 2)
(applytest 0 string-length "")
(applytest "" substring "ab" 0 0)
(applytest "" substring "ab" 1 1)
(applytest "" substring "ab" 2 2)
(applytest "a" substring "ab" 0 1)
(applytest "b" substring "ab" 1 2)
(applytest "ab" substring "ab" 0 2)
(applytest "foobar" string-append "foo" "bar")
(applytest "foo" string-append "foo")
(applytest "foo" string-append "foo" "")
(applytest "foo" string-append "" "foo")
(applytest "" string-append)
(applytest "" make-string 0)
(applytest #t string=? "" "")
(applytest #f string<? "" "")
(applytest #f string>? "" "")
(applytest #t string<=? "" "")
(applytest #t string>=? "" "")
(applytest #t string-ci=? "" "")
(applytest #f string-ci<? "" "")
(applytest #f string-ci>? "" "")
(applytest #t string-ci<=? "" "")
(applytest #t string-ci>=? "" "")

(applytest #f string=? "A" "B")
(applytest #f string=? "a" "b")
(applytest #f string=? "9" "0")
(applytest #t string=? "A" "A")

(applytest #t string<? "A" "B")
(applytest #t string<? "a" "b")
(applytest #f string<? "9" "0")
(applytest #f string<? "A" "A")

(applytest #f string>? "A" "B")
(applytest #f string>? "a" "b")
(applytest #t string>? "9" "0")
(applytest #f string>? "A" "A")

(applytest #t string<=? "A" "B")
(applytest #t string<=? "a" "b")
(applytest #f string<=? "9" "0")
(applytest #t string<=? "A" "A")

(applytest #f string>=? "A" "B")
(applytest #f string>=? "a" "b")
(applytest #t string>=? "9" "0")
(applytest #t string>=? "A" "A")

(applytest #f string-ci=? "A" "B")
(applytest #f string-ci=? "a" "B")
(applytest #f string-ci=? "A" "b")
(applytest #f string-ci=? "a" "b")
(applytest #f string-ci=? "9" "0")
(applytest #t string-ci=? "A" "A")
(applytest #t string-ci=? "A" "a")

(applytest #t string-ci<? "A" "B")
(applytest #t string-ci<? "a" "B")
(applytest #t string-ci<? "A" "b")
(applytest #t string-ci<? "a" "b")
(applytest #f string-ci<? "9" "0")
(applytest #f string-ci<? "A" "A")
(applytest #f string-ci<? "A" "a")

(applytest #f string-ci>? "A" "B")
(applytest #f string-ci>? "a" "B")
(applytest #f string-ci>? "A" "b")
(applytest #f string-ci>? "a" "b")
(applytest #t string-ci>? "9" "0")
(applytest #f string-ci>? "A" "A")
(applytest #f string-ci>? "A" "a")

(applytest #t string-ci<=? "A" "B")
(applytest #t string-ci<=? "a" "B")
(applytest #t string-ci<=? "A" "b")
(applytest #t string-ci<=? "a" "b")
(applytest #f string-ci<=? "9" "0")
(applytest #t string-ci<=? "A" "A")
(applytest #t string-ci<=? "A" "a")

(applytest #f string-ci>=? "A" "B")
(applytest #f string-ci>=? "a" "B")
(applytest #f string-ci>=? "A" "b")
(applytest #f string-ci>=? "a" "b")
(applytest #t string-ci>=? "9" "0")
(applytest #t string-ci>=? "A" "A")
(applytest #t string-ci>=? "A" "a")
(SECTION 6 8)
(define vector-length length)
(define vector-ref elt)
(applytest #t vector? '#(0 (2 2 2 2) "Anna"))
(applytest #t vector? '#())
(applytest '#(a b c) vector 'a 'b 'c)
(applytest '#() vector)
(applytest 3 vector-length '#(0 (2 2 2 2) "Anna"))
(applytest 0 vector-length '#())
(applytest 8 vector-ref '#(1 1 2 3 5 8 13 21) 5)
; (applytest '#(0 ("Sue" "Sue") "Anna") 'vector-set
; 	(let ((vec (vector 0 '(2 2 2 2) "Anna")))
; 	  (vector-set! vec 1 '("Sue" "Sue"))
; 	  vec))
(applytest '#(hi hi) make-vector 2 'hi)
(applytest '#() make-vector 0)
(applytest '#() make-vector 0 'a)
(SECTION 6 9)
(applytest #t procedure? car)
(applytest #f procedure? 'car)
(applytest #t procedure? (lambda (x) (* x x)))
(applytest #f procedure? '(lambda (x) (* x x)))
;(applytest #t call-with-current-continuation procedure?)
(applytest 7 apply + (list 3 4))
(applytest 7 apply (lambda (a b) (+ a b)) (list 3 4))
(applytest 17 apply + 10 (list 3 4))
(applytest '() apply list '())
(define compose (lambda (f g) (lambda args (f (apply g args))))) ;; ??
(applytest 30 (compose sqt *) 12 75) ;; ??

(applytest '(b e h) map cadr '((a b) (d e) (g h)))
(applytest '(5 7 9) map + '(1 2 3) '(4 5 6))
; (applytest '#(0 1 4 9 16) 'for-each
; 	(let ((v (make-vector 5)))
; 		(for-each (lambda (i) (vector-set! v i (* i i)))
; 			'(0 1 2 3 4))
; 		v))
; (applytest -3 call-with-current-continuation
; 		(lambda (exit)
; 		 (for-each (lambda (x) (if (negative? x) (exit x)))
; 		 	'(54 0 37 -3 245 19))
; 		#t))
; (define list-length
;  (lambda (obj)
;   (call-with-current-continuation
;    (lambda (return)
;     (letrec ((r (lambda (obj) (cond ((null? obj) 0)
; 				((pair? obj) (+ (r (cdr obj)) 1))
; 				(else (return #f))))))
; 	(r obj))))))
(define list-length
 (lambda (obj)
   (do ((i 0 (1+ i))
	(l obj (cdr l)))
       ((not (pair? l)) (and (null? l) i)))))
(applytest 4 list-length '(1 2 3 4))
(applytest #f list-length '(a b . c))
(applytest '() map cadr '())

;;; This tests full conformance of call-with-current-continuation.  It
;;; is a separate test because some schemes do not support call/cc
;;; other than escape procedures.  I am indebted to
;;; raja@copper.ucs.indiana.edu (Raja Sooriamurthi) for fixing this
;;; code.  The function leaf-eq? compares the leaves of 2 arbitrary
;;; trees constructed of conses.
(define (next-leaf-generator obj eot)
  (letrec ((return #f)
	   (cont (lambda (x)
		   (recur obj)
		   (set! cont (lambda (x) (return eot)))
		   (cont #f)))
	   (recur (lambda (obj)
		      (if (pair? obj)
			  (for-each recur obj)
			  (call-with-current-continuation
			   (lambda (c)
			     (set! cont c)
			     (return obj)))))))
    (lambda () (call-with-current-continuation
		(lambda (ret) (set! return ret) (cont #f))))))
(define (leaf-eq? x y)
  (let* ((eot (list 'eot))
	 (xf (next-leaf-generator x eot))
	 (yf (next-leaf-generator y eot)))
    (letrec ((loop (lambda (x y)
		     (cond ((not (eq? x y)) #f)
			   ((eq? eot x) #t)
			   (else (loop (xf) (yf)))))))
      (loop (xf) (yf)))))
(define (applytest-cont)
  (newline)
  (display ";testing continuations; ")
  (newline)
  (SECTION 6 9)
  (applytest #t leaf-eq? '(a (b (c))) '((a) b c))
  (applytest #f leaf-eq? '(a (b (c))) '((a) b c d))
  (report-errs))

;;;; Simple continuation testing

(define (test-call/cc)
  (newline)
  (display ";TESTING CALL/CC ")
  (newline)
  (evaltest 6 (call/cc (lambda (quit)
			 (dotimes (i 10) (if (> i 5) (quit i))) #f)))
  (evaltest #f (call/cc (lambda (quit)
			  (dotimes (i 3) (if (> i 5) (quit i))) #f)))
  (evaltest 7 (let ((l '()))
		(call/cc (lambda (quit)
			   (dotimes (i 10)
			     (set! l (cons i l))
			     (if (> i 5) (quit i)))))
		(length l)))
  (evaltest '(5 4 3 2 1 0)
	    (let ((l '()))
	      (call/cc (lambda (quit)
			 (let ((i 0))
			   (while #t ;; forever
			     (set! l (cons i l))
			     (set! i (+ i 1))
			     (if (> i 5) (quit i))))))
	      l)))

;;; Test Optional R4RS DELAY syntax and FORCE procedure
(define (applytest-delay)
  (newline)
  (display ";testing DELAY and FORCE; ")
  (newline)
  (SECTION 6 9)
  (applytest 3 'delay (force (delay (+ 1 2))))
  (applytest '(3 3) 'delay (let ((p (delay (+ 1 2))))
			(list (force p) (force p))))
  (applytest 2 'delay (letrec ((a-stream
			   (letrec ((next (lambda (n)
					    (cons n (delay (next (+ n 1)))))))
			     (next 0)))
			  (head car)
			  (tail (lambda (stream) (force (cdr stream)))))
		   (head (tail (tail a-stream)))))
  (letrec ((count 0)
	   (p (delay (begin (set! count (+ count 1))
			    (if (> count x)
				count
				(force p)))))
	   (x 5))
    (applytest 6 force p)
    (set! x 10)
    (applytest 6 force p))
  (applytest 3 'force
	(letrec ((p (delay (if c 3 (begin (set! c #t) (+ (force p) 1)))))
		 (c #f))
	  (force p)))
  (report-errs))

; (SECTION 6 10 1)
; (applytest #t input-port? (current-input-port))
; (applytest #t output-port? (current-output-port))
; (applytest #t call-with-input-file (get-component "r4rstest.scm") input-port?)
; (define this-file (open-input-file (get-component "r4rstest.scm")))
; (applytest #t input-port? this-file)
; (SECTION 6 10 2)
; (applytest #\; peek-char this-file)
; (applytest #\; read-char this-file)
; (applytest '(define cur-section '()) read this-file)
; (applytest #\( peek-char this-file)
; (applytest '(define errs '()) read this-file)
; (close-input-port this-file)
; (close-input-port this-file)
; (define (check-test-file name)
;   (define test-file (open-input-file name))
;   (applytest #t 'input-port?
; 	(call-with-input-file
; 	    name
; 	  (lambda (applytest-file)
; 	    (applytest load-test-obj read test-file)
; 	    (applytest #t eof-object? (peek-char test-file))
; 	    (applytest #t eof-object? (read-char test-file))
; 	    (input-port? test-file))))
;   (applytest #\; read-char test-file)
;   (applytest display-test-obj read test-file)
;   (applytest load-test-obj read test-file)
;   (close-input-port test-file))
; (SECTION 6 10 3)
; (define write-test-obj
;   '(#t #f #\a () 9739 -3 . #((applytest) "te \" \" st" "" test #() b c)))
; (define display-test-obj
;   '(#t #f a () 9739 -3 . #((applytest) te " " st test #() b c)))
; (define load-test-obj
;   (list 'define 'foo (list 'quote write-test-obj)))
; (applytest #t call-with-output-file
;       "tmp1"
;       (lambda (applytest-file)
; 	(write-char #\; test-file)
; 	(display write-test-obj test-file)
; 	(newline test-file)
; 	(write load-test-obj test-file)
; 	(output-port? test-file)))
; (check-test-file "tmp1")

; (define test-file (open-output-file "tmp2"))
; (write-char #\; test-file)
; (display write-test-obj test-file)
; (newline test-file)
; (write load-test-obj test-file)
; (applytest #t output-port? test-file)
; (close-output-port test-file)
; (check-test-file "tmp2")
; (define (applytest-sc4)
;   (newline)
;   (display ";testing scheme 4 functions; ")
;   (newline)
;   (SECTION 6 7)
;   (applytest '(#\P #\space #\l) string->list "P l")
;   (applytest '() string->list "")
;   (applytest "1\\\"" list->string '(#\1 #\\ #\"))
;   (applytest "" list->string '())
;   (SECTION 6 8)
;   (applytest '(dah dah didah) vector->list '#(dah dah didah))
;   (applytest '() vector->list '#())
;   (applytest '#(dididit dah) list->vector '(dididit dah))
;   (applytest '#() list->vector '())
;   (SECTION 6 10 4)
;   (load "tmp1")
;   (applytest write-test-obj 'load foo)
;   (report-errs))

; (report-errs)

; (if (and (string->number "0.0") (inexact? (string->number "0.0")))
;     (applytest-inexact))

; (let ((n (string->number "281474976710655")))
;   (if (and n (exact? n))
;       (applytest-bignum)))
; (newline)
; (applytest-sc4)
; (newline)
; (applytest-delay)
; (display "To fully test continuations, Scheme 4, and DELAY/FORCE do:")
; (newline)
; (display "(applytest-cont) (applytest-sc4) (applytest-delay)")
; (newline)

(test-call/cc)
(test-bignums)
(test-inexact)

(message "Partial R4RS tests completed")

