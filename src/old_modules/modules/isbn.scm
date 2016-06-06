;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

;;; Recognizing valid ISBNs
;;; Based on descriptions in:
;;;  https://uva.onlinejudge.org/external/3/333.html
;;;  https://en.wikipedia.org/wiki/International_Standard_Book_Number

(in-module 'isbn)
(module-export! 'isbn?)

(define gs1-prefixes {"978" "979"})

(define (char-weight char (code))
  (set! code (char->integer char))
  (if (< 47 code 58) (- code 48)
      (if (= code 88) 10
	  #f)))

(define (isbn10-sum vec)
  (let ((sum 0) (sums '()))
    (doseq (n vec)
      (set! sum (+ sum n))
      (set! sums (cons sum sums)))
    (apply + sums)))

(define (isbn13-sum vec)
  (let ((sum 0))
    (doseq (n vec i)
      (set! sum (+ sum (* n (if (even? i) 1 3)))))
    sum))

(define (isbn? string (checkprefix #t))
  (and
   (or (not checkprefix)
       (has-prefix string gs1-prefixes)
       (has-prefix string (pickstrings checkprefix)))
   (let ((weights (remove #f (map char-weight string))))
     (if (= (length weights) 10)
	 (zero? (remainder (isbn10-sum weights) 11))
	 (if (= (length weights) 13)
	     (zero? (remainder (isbn13-sum weights) 10))
	     #f)))))
