;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'stringfmts)

;;; Generation of strings from various other kinds of values

(module-export! '{get% show%
		       interval-string short-interval-string
		       minimal-interval-string
		       padnum printnum numstring
		       $count $num $size})

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

;;; Numbers with commas

;; This should default this from the locale
(define printnum-sep ",")

(define (printnum num (pad #f) (sep printnum-sep))
  (cond ((inexact? num) (number->string num))
	((>= num 1000)
	 (printnum (quotient num 1000) (>= num 1000000) sep)
	 (printout sep (printnum (remainder num 1000) #t)))
	((>= num 100) (printout num))
	((>= num 10) (printout (if pad "0") num))
	((>= num 0) (printout (if pad "00") num))
	(else (printout "-" (printnum (- num))))))

(define (numstring . args) (stringout (apply printnum args)))

(define $num printnum)

;;; Plural stuff (automatic stuff is just English)

(define ($count n (singular #f) (plural #f) (spellout #t))
  (printout
    (if spellout
	(if (= n 0) "zero"
	    (if (= n 1) "one"
		(printnum n)))
	n)
    (when singular
      (printout " "
		(if (= n 1) singular
		    (or plural (string-append singular "s")))))))

(defambda ($size values (word #f) (plural #f))
  ($count (choice-size values) word plural))

;; Temporal intervals

(define (interval-string secs (precise #t))
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
      (cond ((= hours 1) "one hour, ")
	    ((> hours 0) (printout hours " hours, ")))
      (cond ((= minutes 1) "one minute, ")
	    ((> minutes 0) (printout minutes " minutes, ")))
      (cond ((= seconds 1) "one second")
	    ((< secs 1) (printout seconds " seconds"))
	    (precise (printout seconds " seconds"))
	    ((> secs 1800)
	     ($count (inexact->exact (round seconds)) " second" " seconds"))
	    ((> seconds 60)
	     ($count (inexact->string seconds 2) "second" "seconds"))
	    (else ($count (inexact->string seconds 2) "second" "seconds"))))))

(define (short-interval-string secs (precise #t))
  (if (< secs 180)
      (stringout (cond ((< secs 0) secs)
		       ((and (not precise) (> secs 2))
			(printout (inexact->exact (round secs))))
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
	     (raw-seconds (- secs (* days 3600 24)
			     (* hours 3600)
			     (* minutes 60)))
	     (seconds (if precise raw-seconds
			  (inexact->exact (round raw-seconds)))))
	(stringout
	  (cond ((= days 1) "one day, ")
		((> days 0) (printout days " days, ")))
	  (when (> hours 0) (printout hours ":"))
	  (printout 
	    (if (and (> hours 0) (< minutes 10)) "0")
	    minutes ":")
	  (printout (if (< seconds 10) "0")
		    (cond (precise seconds)
			  ((> secs 600) (inexact->exact (round seconds)))
			  ((>= secs 10) (inexact->string seconds 2))
			  (else seconds)))))))

(define (minimal-interval-string secs (precise #t))
  (cond ((< secs 180)
	 (stringout secs " seconds"))
	((< secs 3600)
	 (stringout (round (/ secs 60)) " minutes"))
	((< secs (* 24 3600))
	 (stringout (round (/ secs 3600)) " hours"))
	((< secs (* 14 24 3600))
	 (stringout (round (/ secs (* 24 3600))) " days"))
	((< secs (* 60 24 3600))
	 (stringout (round (/ secs (* 7 24 3600))) " weeks"))
	((< secs (* 720 24 3600))
	 (stringout (round (/ secs (* 30 24 3600))) " months"))
	(else
	 (stringout (round (/ secs (* 365 24 3600))) " years"))))

