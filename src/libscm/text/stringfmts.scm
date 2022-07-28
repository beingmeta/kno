;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu).

(in-module 'text/stringfmts)

(use-module '{defmacro reflection logger varconfig})

;;; Generation of strings from various other kinds of values

(module-export!
 '{;; Now defined with DEFEXPORT
   ;; get% show%
   interval-string
   short-interval-string
   minimal-interval-string
   compact-interval-string
   compact-interval-string+
   padnum printnum numstring
   xround $xround
   $count $countstring
   $num $numstring
   $size $sizestring $nelts
   $bytes $bytestring
   $bytes/sec
   $rate
   $filetimes
   $fileinfo
   $fn
   $pid})

(module-export!
 '{$lines $lines/indent
   $indented $table})

(module-export! '{$singular $plural})

(define (xround x (factor #f))
  (if factor
      (->exact (* factor (round (/~ x factor))))
      (->exact (round x))))
(define $xround (fcn/alias xround))

;; Percentages

(defexport (get% num (den #f) (prec 2))
  (cond ((zero? den) 0)
	(den (inexact->string (/ (* num 100.0) den) prec))
	(else (inexact->string (* num 100.0) prec))))

(defexport (show% num (den #f) (prec 2))
  (cond ((not (number? num)) 
	 (printout "NotANumber:" num (when den (printout"%/" den)) ))
	((not den) (printout (inexact->string (* num 100.0) prec) "%"))
	((not (number? den)) 
	 (printout "NotANumber:" den (printout"%under" num) ))
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
	 (printout (if pad (getpad num))
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

;;; Plural stuff, just for English now

(define (strip-plural string)
  (try (tryif (has-suffix string "ies") (glom (slice string 0 -3) "y"))
       (tryif (has-suffix string "es") (slice string 0 -2))
       (tryif (has-suffix string "s") (slice string 0 -1))))

(define (make-plural string)
  (try (tryif (has-suffix string "y") (glom (slice string 0 -1) "ies"))
       (tryif (has-suffix string "x") (glom string "es"))
       (glom string "s")))

(define ($plural string)
  (if (has-suffix string "s") string (make-plural string)))

(define ($singular string)
  (if (has-suffix string "s") (strip-plural string) string))

;;; Plural stuff (automatic stuff is just English)

;;; TODO: Generalize singular/plural/etc stuff into 'measurements'
(define ($count n (singular #f) (plural #f) (spellout #t))
  ;; This may be too hairy, but it simplifies some things
  (when (symbol? singular) (set! singular (downcase singular)))
  (when (symbol? plural) (set! plural (downcase plural)))
  (cond ((and (pair? singular) (string? (car singular)) (string? (cdr singular)))
	 (let ((pair singular))
	   (cond ((has-suffix (car pair) "s")
		  (set! plural (car pair))
		  (set! singular (cdr pair)))
		 (else
		  (set! singular (car pair))
		  (set! plural (cdr pair))))))
	((and (pair? singular) (= (length singular) 2)
	      (string? (car singular)) (string? (second singular)))
	 (let ((lst singular))
	   (cond ((has-suffix (first lst) "s")
		  (set! plural (first lst))
		  (set! singular (second lst)))
		 (else
		  (set! singular (first lst))
		  (set! plural (second lst))))))
	((and (string? singular) (not plural) (has-suffix singular "s"))
	 (let ((base (strip-plural singular)))
	   (set! plural singular)
	   (set! singular base)))
	((and (string? singular) (not plural))
	 (set! plural (make-plural singular)))
	(else
	 (unless (or (not singular) (string? singular))
	   (logwarn |BadCountTerm/singular| singular)
	   (set! singular #f))
	 (unless (or (not plural) (string? plural))
	   (logwarn |BadCountTerm/plural| plural)
	   (set! plural #f))))
  (printout
    (if spellout
	(if (= n 0) "zero"
	    (if (= n 1) "one"
		(printnum n 2)))
	n)
    (when singular
      (printout " "
		(if (= n 1) singular plural)))))
(define ($countstring n (singular #f) (plural #f) (spellout #t))
  (stringout (apply $count n singular plural spellout)))

(defambda ($size values (word #f) (plural #f))
  ($count (choice-size values) word plural))
(defambda ($nelts values (word #f) (plural #f))
  ($count (choice-size values) word plural))

;;; Displaying Rates

;;; TODO: Add ability to generate labels (e.g. bytes/sec)
;;; TODO: Add ability to scale ticks to coarser units, e.g. minutes, hours, etc
;;; TODO: Add ability to scale count to coarser units, e.g. MB, MiB, etc
(define ($rate count ticks (precision 2))
  "Returns an inexact ratio of *count* to *ticks* limited by precision"
  (let ((ratio (/~ count ticks)))
    (if (> ratio (/~ (pow 10 precision)))
	(inexact->string ratio precision)
	(inexact->string ratio))))

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

;; Temporal intervals

(define (interval-string secs (precise #t))
  (when (number? precise)
    (if (< precise 0) (set! secs (- secs)))
    (set! secs (xround secs precise)))
  (let* ((days (inexact->exact (floor (/~ secs (* 3600 24)))))
	 (hours (inexact->exact (floor (/~ (- secs (* days 3600 24)) 3600))))
	 (minutes (inexact->exact
		   (floor (/~ (- secs (* days 3600 24) (* hours 3600)) 60))))
	 (seconds (- secs (* days 3600 24) (* hours 3600) (* minutes 60))))
    (stringout
	(cond ((= days 1) "one day, ")
	      ((> days 0) (printout days " days, ")))
      (cond ((= hours 1) "one hour, ")
	    ((> hours 0) (printout hours " hours, ")))
      (cond ((= minutes 1) "one minute, ")
	    ((> minutes 0)
	     (printout minutes " minutes"
	       (if (> seconds 0) ", "))))
      (cond ((zero? seconds))
	    ((= seconds 1) "one second")
	    ((< secs 1) (printout seconds " seconds"))
	    (precise (printout seconds " seconds"))
	    ((> secs 1800)
	     ($count (xround seconds) " second" " seconds"))
	    ((> seconds 60)
	     ($count (inexact->string seconds 2) "second" "seconds"))
	    (else ($count (inexact->string seconds 2) "second" "seconds"))))))

(define (short-interval-string secs (precise #t))
  (when (number? precise)
    (if (< precise 0) (set! secs (- secs)))
    (set! secs (xround secs precise)))
  (if (< secs 180)
      (stringout (cond ((< secs 0) secs)
		       ((and (not precise) (> secs 2))
			(printout (xround secs)))
		       ((< secs 10) (inexact->string secs 3))
		       (else (inexact->string secs 2)))
		 " secs")
      (let* ((days (inexact->exact (/~ secs (* 3600 24))))
	     (hours (inexact->exact (/~ (- secs (* days 3600 24))
				       3600)))
	     (minutes (inexact->exact
		       (/~ (- secs (* days 3600 24) (* hours 3600))
			  60)))
	     (raw-seconds (- secs (* days 3600 24)
			     (* hours 3600)
			     (* minutes 60)))
	     (seconds (if precise raw-seconds (xround raw-seconds))))
	(stringout
	  (cond ((= days 1) "one day, ")
		((> days 0) (printout days " days, ")))
	  (when (> hours 0) (printout hours ":"))
	  (printout 
	    (if (and (> hours 0) (< minutes 10)) "0")
	    minutes ":")
	  (printout (if (< seconds 10) "0")
		    (cond (precise seconds)
			  ((> secs 600) (xround seconds))
			  ((>= secs 10) (inexact->string seconds 2))
			  (else seconds)))))))

(define (minimal-interval-string secs (precise #t))
  (when (number? precise)
    (when (< precise 0)
      (set! secs (- secs))
      (set! precise (- precise)))
    (set! secs (xround secs precise)))
  (cond ((< secs 180)
	 (stringout secs " seconds"))
	((< secs 3600)
	 (stringout (xround (/~ secs 60)) " minutes"))
	((< secs (* 24 3600))
	 (stringout (xround (/~ secs 3600)) " hours"))
	((< secs (* 14 24 3600))
	 (stringout (xround (/~ secs (* 24 3600))) " days"))
	((< secs (* 60 24 3600))
	 (stringout (xround (/~ secs (* 7 24 3600))) " weeks"))
	((< secs (* 720 24 3600))
	 (stringout (xround (/~ secs (* 30 24 3600))) " months"))
	(else
	 (stringout (xround (/~ secs (* 365 24 3600))) " years"))))

(define (compact-interval-string total (precise #t) (started #f))
  (local secs (->exact (floor total)))
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

(define (compact-interval-string+ total (precise #t) (started #f))
  "Displays an interval as a compact string XhXmXs (Ys) in both broken down and simple form"
  (local secs (->exact total -1))
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
	    (printout secs "s")))
    (when (> total 60) (printout " (" (->exact total -1) "s)"))))

;;; Mutli-line output

(define (field->string field)
  (escape-string (stringout (write field))))

(define ($lines . lines)
  (dolist (line lines) (lineout line)))
(define ($lines/indent indent . lines)
  (indentout indent (dolist (line lines) (lineout line))))
(define $indented (fcn/alias indentout))
(defambda ($table obj (fields) (opts #f))
  (default! fields (getkeys obj))
  (when (ambiguous? fields) (set! fields (sorted fields)))
  (let* ((names (map field->string fields))
	 (maxwidth (max (length (elts names))))
	 (indent-string (make-string maxwidth #\Space)))
    (doseq (name names i)
      (let ((values (get obj (elts fields i))))
	(do-choices (v values j)
	  (lineout 
	    (if (= j 0)
		(printout name (dotimes (i (- maxwidth (length name))) (display " ")) v)
		(printout indent-string v))))))))

;;;; Display a PID (for no separator)

(define ($pid pid)
  (if (number? pid)
      (number->string pid 10 #f)
      (if (subproc? pid)
	  (number->string (proc-pid pid) 10 #f)
	  pid)))

;;;; Fileinfo

(define ($filetimes file (opts #f))
  (let* ((mtime (file-modtime file))
	 (ctime (file-creationtime file))
	 (mdelta (- (difftime mtime)))
	 (cdelta (- (difftime ctime)))
	 (showstamps (getopt opts 'timestamps (config 'timestamps {} config:boolean))))
    (printout "modified " 
      (if (> mdelta 600) (interval-string mdelta 60) (interval-string mdelta))
      " ago"
      (when showstamps (printout " (" (get mtime 'string) ")"))
      ", created " 
      (if (> cdelta 600) (interval-string cdelta 60) (interval-string cdelta))
      " ago (" (get mtime 'string) ")")))

(define ($fileinfo file (opts #f))
  (let* ((mtime (file-modtime file))
	 (ctime (file-creationtime file))
	 (mdelta (- (difftime mtime)))
	 (cdelta (- (difftime ctime)))
	 (size (file-size file))
	 (showstamps (getopt opts 'timestamps (config 'timestamps {} config:boolean))))
    (printout "size " ($bytes size) ", "
      "modified " 
      (if (> mdelta 600) (interval-string mdelta 60) (interval-string mdelta))
      " ago"
      (when showstamps (printout " (" (get mtime 'string) ")"))
      ", created " 
      (if (> cdelta 600) (interval-string cdelta 60) (interval-string cdelta))
      " ago (" (get mtime 'string) ")")))

;;;; Displaying procedures

(define ($fn fn)
  (cond ((not (procedure-name fn)) fn)
	((procedure-module fn) 
	 (printout (procedure-name fn) "(" (procedure-module fn) ")"))
	((procedure-filename fn) 
	 (printout (procedure-name fn) "(" (procedure-filename fn) ")"))
	(else (procedure-name fn))))
