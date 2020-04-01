;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'stringfmts)

;;; Generation of strings from various other kinds of values

(module-export!
 '{;; Now defined with DEFEXPORT
   ;; get% show%
   interval-string
   short-interval-string
   minimal-interval-string
   compact-interval-string
   padnum printnum numstring
   $count $countstring
   $num $numstring
   $size $sizestring
   $bytes $bytestring
   $bytes/sec
   $rate})

;; Percentages

(defexport (get% num (den #f) (prec 2))
  (cond ((zero? den) den)
	(den (inexact->string (/ (* num 100.0) den) prec))
	(else (inexact->string (* num 100.0) prec))))

(defexport (show% num (den #f) (prec 2))
  (cond ((not den) (printout (inexact->string (* num 100.0) prec) "%"))
	((zero? den)
	 (if (zero? num)
	     (printout "0%")
	     (printout num "/0%")))
	(else (printout (inexact->string (/ (* num 100.0) den) prec) "%"))))

;;; Padded numbers

(define (padnum num digits (base 10))
  (let* ((s (number->string num base))
	 (len (length s))
	 (padlen (- digits len)))
    (if (> padlen 0)
	(string-append (make-string padlen #\0) s)
	s)))

;;; Remainders for inexact numbers

(define (quotient~ num den)
  (if (and (exact? num) (exact? den))
      (quotient num den)
      ((if (< (* num den) 0) ceiling floor)
       (inexact->exact (/~ num den)))))

(define (rem~ num den)
  (if (and (exact? num) (exact? den))
      (remainder num den)
      (- num (* den (floor (/~ num den))))))

;;; Numbers with commas

;; This should default this from the locale
(define printnum-sep ",")

(define (printnum num (prec #f) (pad #f) (sep printnum-sep))
  (cond ((< num 0) (printout "-" (printnum (- num) prec sep)))
	((>= num 1000)
	 (printnum (quotient~ num 1000) prec (>= num 1000000) sep)
	 (printout sep (printnum (rem~ num 1000) prec #t)))
	(else 
	 (printout (dbg) (if pad (getpad num))
	   (if (and prec (inexact? num))
	       (if (> num 1)
		   (inexact->string num prec)
		   (inexact->string num))
	       num)))))

(define (getpad num)
  (if (>= num 100) ""
      (if (>= num 10) "0" "00")))

(define (numstring . args) (stringout (apply printnum args)))
(define ($numstring . args) (stringout (apply printnum args)))

(define $num (fcn/alias printnum))

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
(define ($countstring n (singular #f) (plural #f) (spellout #t))
  (stringout (apply $count n singular plural spellout)))

(defambda ($size values (word #f) (plural #f))
  ($count (choice-size values) word plural))

;;; Byte sizes

(define KiB 1024)
(define MiB (* 1024 1024))
(define GiB (* 1024 1024 1024))
(define TiB (* 1024 1024 1024 1024))
(define PiB (* 1024 1024 1024 1024 1024))

(define ($bytes bytes (space " ") (sign ""))
  (if (not space) (set! space ""))
  (when (< bytes 0)
    (set! sign "-") (set! bytes (- bytes)))
  (if (<= bytes 4096)
      (printout sign bytes space "bytes")
      (if (< bytes MiB)
	  (printout sign (printnum (/~ bytes 1024) 1) space "KiB")
	  (if (< bytes (* 2 GiB))
	      (printout sign (printnum (/~ bytes MiB) 1) space "MiB")
	      (if (< bytes (* 2 TiB))	      
		  (printout sign (printnum (/~ bytes GiB) 1) space "GiB")
		  (if (< bytes (* 2 PiB))	      
		      (printout sign (printnum (/~ bytes TiB) 1) space "TiB")
		      (printout  sign
			(printnum (/~ bytes PiB) 1) space "PiB")))))))

(define ($bytestring bytes (space ""))
  (stringout ($bytes bytes space)))

(define ($bytes/sec bytes (secs #f) (rate))
  (if secs 
      (set! rate (/~ bytes secs))
      (set! rate bytes))
  (if (< rate 10000)
      (printout (printnum rate 0) " bytes/sec")
      (if (< rate (* 2 MiB))
	  (printout (printnum (/~ rate KiB) 1) " KiB/sec")
	  (if (< rate (* 2 GiB))
	      (printout (printnum (/~ bytes MiB) 1) " MiB/sec")
	      (if (< rate (* 2 TiB))
		  (printout (printnum (/~ bytes GiB) 1) " GiB/sec")
		  (if (< rate (* 2 TiB))
		      (printout (printnum (/~ bytes TiB) 1) " TiB/sec")
		      (printout (printnum (/~ bytes PiB) 1) " PiB/sec")))))))

(define ($rate count ticks (precision 2))
  (let ((ratio (/~ count ticks)))
    (if (> ratio (/~ (pow 10 precision)))
	(inexact->string ratio precision)
	(inexact->string ratio))))

;; Temporal intervals

(define (interval-string secs (precise #t))
  (let* ((days (inexact->exact (/ secs (* 3600 24))))
	 (hours (inexact->exact (/ (- secs (* days 3600 24))
				   3600)))
	 (minutes (inexact->exact
		   (/ (- secs (* days 3600 24) (* hours 3600))
		      60)))
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
      (let* ((days (inexact->exact (/ secs (* 3600 24))))
	     (hours (inexact->exact (/ (- secs (* days 3600 24))
				       3600)))
	     (minutes (inexact->exact
		       (/ (- secs (* days 3600 24) (* hours 3600))
			  60)))
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

(define (compact-interval-string total (precise #t) (started #f) (secs))
  (default! secs (->exact (floor total)))
  (stringout
    (when (> secs (* 7 24 3600))
      (printout (quotient secs (* 7 24 3600)) "w")
      (set! secs (remainder secs (* 7 24 3600)))
      (set! started #t))
    (when (or started (> secs (* 24 3600)))
      (printout (quotient secs (* 24 3600)) "d")
      (set! secs (remainder secs (* 24 3600)))
      (set! started #t))
    (when (or started (> secs 3600))
      (printout (quotient secs 3600) "h")
      (set! secs (remainder secs 3600))
      (set! started #t))
    (when (or started (> secs 60))
      (printout (quotient secs 60) "m")
      (set! secs (remainder secs 60))
      (set! started #t))
    (if (< total 15) 
	(printout total "s")
	(if (< total 60)
	    (printout (inexact->string total 1) "s")
	    (printout secs "s")))))

