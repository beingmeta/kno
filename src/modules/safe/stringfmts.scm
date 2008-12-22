;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'stringfmts)

;;; Generation of strings from various other kinds of values

(define version "$Id$")

(module-export! '{get% show%
		       interval-string short-interval-string
		       padnum printnum numstring})

;; Percentages

(define (get% num (den #f) (prec 2))
  (if den
      (inexact->string (/ (* num 100.0) den) prec)
      (inexact->string (* num 100.0) prec)))

(define (show% num (den #f) (prec 2))
  (printout
    (if den
	(inexact->string (/ (* num 100.0) den) prec)
	(inexact->string (* num 100.0) prec))
    "%"))

;;; Padded numbers

(define (padnum num digits (base 10))
  (let* ((s (number->string num base))
	 (len (length s))
	 (padlen (- digits len)))
    (if (> padlen 0)
	(string-append (make-string padlen #\0) s)
	s)))

;; Temporal intervals

(define (interval-string secs (precise #t))
  (let* ((days (inexact->exact (floor (/ secs (* 3600 24)))))
	 (hours (inexact->exact
		 (floor (/ (- secs (* days 3600 24))
			   3600))))
	 (minutes (inexact->exact
		   (floor (/ (- secs (* days 3600 24) (* hours 3600))
			     60))))
	 (secs (- secs (* days 3600 24) (* hours 3600) (* minutes 60))))
    (stringout
	(cond ((= days 1) "one day, ")
	      ((> days 0) (printout days " days, ")))
      (cond ((= hours 1) "one hour, ")
	    ((> hours 0) (printout hours " hours, ")))
      (cond ((= minutes 1) "one minute, ")
	    ((> minutes 0) (printout minutes " minutes, ")))
      (cond ((= secs 1) "one second")
	    ((< secs 1) (printout secs " seconds"))
	    (else (printout (inexact->string secs 2) " seconds"))))))

(define (short-interval-string secs (precise #t))
  (if (< secs 180)
      (stringout (cond ((< secs 0) secs)
		       ((< secs 10) (inexact->string secs 3))
		       (else (inexact->string secs 2)))
	" secs")
      (let* ((days (inexact->exact (floor (/ secs (* 3600 24)))))
	     (hours (inexact->exact
		     (floor (/ (- secs (* days 3600 24))
			       3600))))
	     (minutes (inexact->exact
		       (floor (/ (- secs (* days 3600 24) (* hours 3600))
				 60))))
	     (seconds (- secs (* days 3600 24) (* hours 3600) (* minutes 60))))
	(stringout
	    (cond ((= days 1) "one day, ")
		  ((> days 0) (printout days " days, ")))
	  (when (> hours 0) (printout hours ":"))
	  (printout 
	    (if (and (> hours 0) (< minutes 10)) "0")
	    minutes ":")
	  (printout (if (< seconds 10) "0")
	    (cond ((> secs 600) (inexact->exact (round seconds)))
		  ((>= secs 10) (inexact->string seconds 2))
		  (else seconds)))))))

;;; Numbers with commas

;; This should probably get this from the locale somehow
(define printnum-sep ",")

(define (printnum num (pad #f))
  (cond ((inexact? num) (number->string num))
	((>= num 1000)
	 (printnum (quotient num 1000) (>= num 1000000))
	 (printout printnum-sep (printnum (remainder num 1000) #t)))
	((>= num 100) (printout num))
	((>= num 10) (printout (if pad "0") num))
	((>= num 0) (printout (if pad "00") num))
	(else (printout "-" (printnum (- num))))))

(define (numstring n) (stringout (printnum n)))

