;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'bench/miscfns)

;;; This provides various sample functions for testing and benchmarking

(module-export! '{fibr fibi fibix fibflt factr facti slowfib fibwhile})
(module-export! '{fibr/random fibi/random fibix/random fibflt/random})
(module-export! '{ack tak takflt})
(module-export! 'spectral-norm)
(module-export! 'square)

;;; Fibonacci

(define (fibr n)
  (if (< n 3) 1 (+ (fibr (- n 1)) (fibr (- n 2)))))

(define (fib-iter i cur prev)
  (if (= i 1) cur (fib-iter (-1+ i) (+ cur prev) cur)))
(define (fibi n)
  (if (= n 0) 0 (fib-iter n 1 0)))

;; This is fibi all in one (no fib-iter) using defaults
(define (fibix i (cur 1) (prev 0))
  (if (> i 1) (fibix (-1+ i) (+ cur prev) cur)
      (if (= i 1) cur 0)))

(define (fibwhile n (i 1) (sum 1) (prev 0) (next))
  (if (or (= n 0) (= n 1)) n
      (begin 
	(while (< i n)
	  (set! next (+ sum prev))
	  (set! prev sum)
	  (set! sum next)
	  (set! i (1+ i)))
	sum)))

(define (fibflt n)
  (cond ((< n 2.0) 1.0)
	(else (+ (fibflt (- n 2.0)) (fibflt (- n 1.0))))))

(define (fibr/random n)
  (fibr (+ 1 (quotient n 2) (random (quotient n 2)))))
(define (fibi/random n)
  (fibi (+ 1 (quotient n 2) (random (quotient n 2)))))
(define (fibix/random n)
  (fibix (+ 1 (quotient n 2) (random (quotient n 2)))))
(define (fibflt/random n)
  (fibflt (+ 1 (quotient n 2) (random (quotient n 2)))))

;;; Factorial

(define (factr n)
  (if (= n 0) 1 (* n (factr (-1+ n)))))

(define (fact-iter n accum)
  (if (= n 0) accum
      (fact-iter (-1+ n) (* n accum))))
(define (facti n) (fact-iter n 1))

;;; Ackermann

(define (ack m n)
    (cond ((zero? m) (+ n 1))
	  ((zero? n) (ack (- m 1) 1))
	  (else (ack (- m 1) (ack m (- n 1))))))

;;; TAK

(define (tak x y z)
  (if (not (< y x)) z
      (tak (tak (- x 1) y z)
	   (tak (- y 1) z x)
	   (tak (- z 1) x y))))

(define (takflt x y z)
  (cond ((not (< y x)) z)
	(else (takflt (takflt (- x 1.0) y z)
		      (takflt (- y 1.0) z x)
		      (takflt (- z 1.0) x y)))))

;;; Spectral norm

(define (eval-a i j)
  (/~ 1.0 (+ (1+ i) (* (+ i j) (/~ (+ (+ i j) 1) 2)))))

(define (eval-a-times-u u)
  (let ((result (make-vector (length u))))
    (doseq (v u ukey)
      (let ((sum 0))
	(doseq (v u key)
	  (set! sum (+ sum (* v (eval-a ukey key)))))
	(vector-set! result ukey sum)))
    result))

(define (eval-at-times-u u)
  (let ((result (make-vector (length u))))
    (doseq (v u ukey)
      (let ((sum 0))
	(doseq (v u key)
	  (set! sum (+ sum (* v (eval-a key ukey)))))
	(vector-set! result ukey sum)))
    result))

(define (eval-ata-times-u u)
  (eval-at-times-u (eval-a-times-u u)))

(define (spectral-norm n)
  (let ((u (make-vector n 1))
	(v (make-vector n 1)))
    (dotimes (i 10)
      (set! v (eval-ata-times-u u))
      (set! u (eval-ata-times-u v)))
    (let ((vBv 0) (vv 0))
      (doseq (value u i)
	(set! vBv (+ vBv (* value (elt v i)))))
      (doseq (value v)
	(set! vv (+ vv (* value value))))
      (sqrt (/ vbv vv)))))

;;; Odd functions for different tests

(define (slowfib n (wait #f))
  (sleep (or wait n))
  (fibi n))

;;; Real simple

(define (square n) (* n n))
