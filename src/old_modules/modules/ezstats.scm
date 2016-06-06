;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'ezstats)

(module-export! '{mean median variance stddev}) 

(define (mean vec)
  (/~ (reduce + vec) (length vec)))

(define (median vec)
  (let* ((vec (sortvec vec)) 
	 (len (length vec))
	 (middle (quotient len 2)))
    (if (zero? (remainder len 2))
	(/~ (+ (elt vec middle) (elt vec (1+ middle))) 2)
	(elt vec middle))))

(define (variance vec)
  (let ((avg (mean vec)))
    (/~ (reduce + (map (lambda (x) (* (- x avg) (- x avg))) vec))
	(length vec))))

(define (stddev vec)
  (sqrt (variance vec)))
