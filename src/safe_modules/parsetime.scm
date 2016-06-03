;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'parsetime)

;;; Simple generic time parser making heavy use of TEXTTOOLS
;;; pattern matching and extraction.

(use-module 'texttools)
(use-module '{logger varconfig})
(define %used_modules 'varconfig)

(define-init %loglevel %notice%)

(module-export! 
 '{parsetime parsegmtime timeparser
   time-patterns time-pattern})

(define us-format-default #f)
(varconfig! parsetime/usafmt us-format-default)

(define month-names
  (vector (qc "Jan" "January")
	  (qc "Feb" "February")
	  (qc "Mar" "March")
	  (qc "Apr" "April")
	  (qc "May")
	  (qc "Jun" "June")
	  (qc "Jul" "July")
	  (qc "August" "Aug")
	  (qc "September" "Sept" "Sep")
	  (qc "Oct" "October")
	  (qc "Nov" "November")
	  (qc "Dec" "December")))

(define monthnums
  (let ((table (make-hashtable)))
    (doseq (v month-names i)
      (store! table v i)
      (store! table (downcase v) i))
    table))
(define monthstrings (apply choice (->list month-names)))

(define (monthnum string)
  (try (1+ (get monthnums string))
       (tryif (textmatch '(isdigit+) string)
	      (string->lisp string))))

(define timezones {"GMT" "UTC" "EDT" "EST" "PST" "PDT" "CST" "CDT"})

(define (add1900 string)
  (string-append "19" string))
(define (add2000 string)
  (string-append "20" string))

(define (parseyear string)
  (if (= (length string) 2)
      (let ((num (string->number string)))
	(if (< num 35) (+ num 2000) (+ num 1900)))
      (string->number string)))

(define generic-patterns
  (choice `#({(bol) (spaces) ">"}
	     (label year #({"19" "20"} (isdigit) (isdigit)) #t))
	  `#({(bol) (spaces) ">"}
	     (label DATE #((isdigit) (opt (isdigit)) (opt {"st" "th" "nd"})) #t)
	     (spaces)
	     (IC (label MONTH ,monthstrings ,monthnum)) (opt #({"" (spaces)} ","))
	     (spaces)
	     (opt (label YEAR #({"1" "2"} (isdigit) (isdigit) (isdigit)) #t)))
	  `#({(bol) (spaces) ">"}
	     (IC (label MONTH ,monthstrings ,monthnum))
	     (spaces*)
	     (label DATE #((isdigit) (opt (isdigit)) (opt {"st" "th" "nd"})) #t)
	     {"" (spaces) #("," (spaces))}
	     (opt (label YEAR #({"1" "2"} (isdigit) (isdigit) (isdigit)) #t)))
	  `#({(bol) (spaces) ">"}
	     (IC (label MONTH ,monthstrings ,monthnum))
	     (spaces)
	     (opt (label YEAR #({"1" "2"} (isdigit) (isdigit) (isdigit)) #t)))
	  `#({(spaces) (bol) ">"}
	     (label HOURS #((isdigit) (opt (isdigit))) #t)
	     {(spaces) (eol) "<"
	      #(":" (label MINUTES #((isdigit) (isdigit)) #t)
		{(spaces) (eol)
		 #(":" (label SECONDS #((isdigit) (isdigit)) #t)
		   {(spaces) (eol)
		    #("." (label FRACTION (isdigit+)))
		    "<"})})})
	  `#({(bol) (spaces) ">"}
	     (label AMPM (IC {"AM" "PM"})) {(eol) (spaces) "<"})
	  `#({(bol) (spaces) ">"}
	     (label TIMEZONE
		    {(IC ,timezones)
		     #({"+" "-"} (isdigit) (opt (isdigit))
		       (opt #(":" (isdigit) (isdigit))))})
	     {(eol) (spaces) "<"})
	  `#({(bol) (spaces) ">"}
	     (label year #("19" (isdigit) (isdigit)) #t) {"/" "-"}
	     (label year #((isdigit) (isdigit)) ,add1900))
	  	  `#({(bol) (spaces)}
	     (label year #("20" (isdigit) (isdigit)) #t) {"/" "-"}
	     (label year #((isdigit) (isdigit)) ,add2000)))
  )

(define us-patterns
  (choice generic-patterns
	  #((label MONTH #((isdigit) (opt (isdigit))) #t) "/"
	    (label DATE #((isdigit) (opt (isdigit))) #t)
	    (opt #("/" (label YEAR (isdigit+) ,parseyear))))))
(define terran-patterns
  (choice generic-patterns
	  #((label DATE #((isdigit) (opt (isdigit))) #t) "/"
	    (label MONTH #((isdigit) (opt (isdigit))) #t)
	    (opt #("/" (label YEAR (isdigit+) ,parseyear))))
	  #((label DATE #((isdigit) (opt (isdigit))) #t) "."
	    (label MONTH #((isdigit) (opt (isdigit))) #t)
	    #("." (label YEAR (isdigit+) ,parseyear)))))

(define time-patterns
  (choice generic-patterns us-patterns terran-patterns))
(define time-pattern
  `#(,time-patterns (* #(,time-patterns (opt ",") {(spaces) "" (eol) "<"}))))

(define (merge-matches-loop matches fields)
  (let ((slotids (sorted (getkeys matches))))
    (if (null? fields) (frame-create #f)
	(let* ((slotid (car fields))
	       (values (get matches slotid))
	       (remainder (merge-matches-loop (qc matches) (cdr fields))))
	  (for-choices (r remainder)
	    (if (ambiguous? values)
		(for-choices (value values)
		  (add! (deep-copy remainder) slotid value))
		(begin (add! remainder slotid values)
		       remainder)))))))
(define (merge-matches matches)
  (merge-matches-loop (qc matches)
		      (->list (sorted (getkeys matches)))))

(define (timeparser string (us us-format-default))
  (text->frames (qc (if us us-patterns terran-patterns))
		string))

(defambda (getfields matches)
  (try (tryif (test matches 'second) '(second minute hour date month year))
       (tryif (test matches 'minute) '(minute hour date month year))
       (tryif (test matches 'hour) '(hour date month year))
       (tryif (test matches 'date) '(date month year))
       (tryif (test matches 'month) '(month year))
       (tryif (test matches 'year) '(year))))

(defambda (matches->timestamps matches fields base)
  (%debug "base=" base "; tick=" (get base '%tick) "; fields=" fields)
  (if (null? fields) base
      (if (exists? (get matches (car fields)))
	  (for-choices (v (get matches (car fields)))
	    (let* ((mod (frame-create #f (car fields) v))
		   (newbase (modtime mod base #f)))
	      (%debug "mod=" mod "; newbase=" newbase "; newtick="
		      (get newbase '%tick))
	      (matches->timestamps matches (cdr fields) newbase)))
	  (matches->timestamps matches (cdr fields) base))))

(define (parsetime string (base #f) (us us-format-default))
  (if (string? string)
      (let ((matches
	     (text->frames (qc (if us us-patterns terran-patterns))
			   string)))
	(when (test matches 'ampm '{"PM" "pm"})
	  (let ((hours (get matches 'hours)))
	    (unless (> (+ 12 hours) 24)
	      (store! matches 'hours (+ 12 hours)))))
	(when (test matches 'year)
	  (do-choices (year (pickstrings (get matches 'year)))
	    (if (= (length year) 2)
		(store! matches 'year (+ 1900 (string->number year)))
		(store! matches 'year (string->number year)))))
	;; Handle the case where people swapped the month and date according
	;;  to US conventions .vs. the rest of the world
	(when (and (test matches 'month) (test matches 'date)
		   (> (get matches 'month) 12)
		   (<= (get matches 'date) 12))
	  (let ((m (get matches 'month)) (d (get matches 'date)))
	    (store! matches 'month d) (store! matches 'date m)))
	(let* ((fields (getfields matches))
	       (base (timestamp (car fields))))
	  (%debug "parsetime matches=" matches ", fields=" fields)
	  (matches->timestamps matches fields base)))
      (timestamp string)))

(define (parsegmtime string (base #f) (us us-format-default))
  (parsetime string (or base (gmtimestamp)) us))

;;;; Time delta functions

(define (+minutes n (base (timestamp 'seconds)))
  (timestamp+ base (* n 60)))
(define (-minutes n (base (timestamp 'seconds)))
  (timestamp+ base (* n -60)))
(define (+hours n (base (timestamp 'seconds)))
  (timestamp+ base (* n 3600)))
(define (-hours n (base (timestamp 'seconds)))
  (timestamp+ base (* n -3600)))
(define (+days n (base (timestamp 'seconds)))
  (timestamp+ base (* n 3600 24)))
(define (-days n (base (timestamp 'seconds)))
  (timestamp+ base (* n -3600 24)))
(define (+weeks n (base (timestamp 'seconds)))
  (timestamp+ base (* n 3600 24 7)))
(define (-weeks n (base (timestamp 'seconds)))
  (timestamp+ base (* n -3600 24 7)))

(module-export! '{+minutes -minutes
		  +hours -hours
		  +days -days
		  +weeks -weeks})
