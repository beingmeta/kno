;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

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

(define iso-pattern
  '(GREEDY
    #((label year #((isdigit)  (isdigit) (isdigit) (isdigit)))
      (opt #("-"  (label month #((isdigit) (isdigit)))
	     (opt #( "-" (label day #((isdigit) (isdigit)))
		     (opt #("T" (label hour #((isdigit) (isdigit)))
			    (opt #(":" (label minute #((isdigit) (isdigit)))
				   (opt #(":" (label secs #((isdigit) (isdigit)
							    (opt #("." (isdigit+)))))))))))))))
      (label gmtoff
	     {(opt #({"+" "-"} (isdigit) (opt (isdigit)) 
		     (opt #(":" (isdigit) (isdigit)))))
	      "GMT" "Z"}))))

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

(define rfc822-pattern
  `(GREEDY
    #({"Mon" "Tue" "Wed" "Thu" "Fri" "Sat" "Sun"} ","
      (spaces) (isdigit) (opt (isdigit)) (spaces)
      ,(pick (elts month-names) length 3)
      (spaces)
      (isdigit) (isdigit) (isdigit) (isdigit)
      (spaces) (rest))))

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

(define (parseyear string)
  (if (= (length string) 2)
      (let ((num (string->number string)))
	(if (< num 35) (+ num 2000) (+ num 1900)))
      (string->number string)))

(define date-in-month
  '(word #((isdigit) (opt (isdigit)) (opt {"st" "th" "nd"}))))

(define generic-patterns
  `{#({(bol) (spaces) ">"}
      (label year #({"19" "20"} (isdigit) (isdigit)) #t))
    #({(bol) (spaces) ">"}
      (label DATE ,date-in-month #t)
      (spaces)
      (IC (label MONTH ,monthstrings ,monthnum)) (opt #({"" (spaces)} ","))
      (spaces)
      (opt (label YEAR #({"1" "2"} (isdigit) (isdigit) (isdigit)) #t)))
    #({(bol) (spaces) ">"}
      (IC (label MONTH ,monthstrings ,monthnum))
      (spaces*)
      (label DATE ,date-in-month #t)
      {"" (spaces) #("," (spaces))}
      (opt (label YEAR #({"1" "2"} (isdigit) (isdigit) (isdigit)) #t)))
    #({(bol) (spaces) ">"}
      (IC (label MONTH ,monthstrings ,monthnum))
      (spaces)
      (opt (label YEAR #({"1" "2"} (isdigit) (isdigit) (isdigit)) #t)))
    (PREF #({(bol) (isspace)}
	    (label HOUR #((isdigit) (opt (isdigit))) #t) ":"
	    (label MINUTE #((isdigit) (isdigit)) #t) ":"
	    (label SECOND #((isdigit) (isdigit)) #t)
	    (opt #("." (label FRACTION (isdigit+) #t))))
	  #({(bol) (isspace)}
	    (label HOUR #((isdigit) (opt (isdigit))) #t) ":"
	    (label MINUTE #((isdigit) (isdigit)) #t) ":"
	    (label SECOND #((isdigit) (isdigit)) #t))
	  #({(bol) (isspace)}
	    (label HOUR #((isdigit) (opt (isdigit))) #t) ":"
	    (label MINUTE #((isdigit) (isdigit)) #t)))
    #({(bol) (spaces) ">"}
      (label AMPM (IC {"AM" "PM"})) {(eol) (spaces) "<"})
    #({(bol) (spaces) ">"}
      (label TIMEZONE
	     {(IC ,timezones)
	      #({"+" "-"} (isdigit) (opt (isdigit))
		(opt #(":" (isdigit) (isdigit))))})
      {(eol) (spaces) "<"})
    #({(bol) (spaces) ">"}
      (label year #("19" (isdigit) (isdigit)) ,parseyear)
      {"/" "-"}
      (label year #((isdigit) (isdigit)) ,parseyear))
    #({(bol) (spaces)}
      (label year #("20" (isdigit) (isdigit)) #t) 
      {"/" "-"}
      (label year #((isdigit) (isdigit)) ,parseyear))})

(define us-patterns
  (choice generic-patterns
	  `#((label MONTH #((isdigit) (opt (isdigit))) #t) "/"
	     (label DATE #((isdigit) (opt (isdigit))) #t)
	     (opt #("/" (label YEAR (isdigit+) ,parseyear))))))
(define terran-patterns
  (choice generic-patterns
	  `#((label DATE #((isdigit) (opt (isdigit))) #t) "/"
	     (label MONTH #((isdigit) (opt (isdigit))) #t)
	     (opt #("/" (label YEAR (isdigit+) ,parseyear))))
	  `#((label DATE #((isdigit) (opt (isdigit))) #t) "."
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
  (text->frames (if us us-patterns terran-patterns)
		string))

(defambda (getfields matches)
  (try (tryif (test matches 'second) 
	 '(seconds second minute hour date month year))
       (tryif (test matches 'minute) '(minutes minute hour date month year))
       (tryif (test matches 'hour) '(hours hour date month year))
       (tryif (test matches 'date) '(date date month year))
       (tryif (test matches 'month) '(month month year))
       (tryif (test matches 'year) '(year year))))

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
  (cond ((not (string? string)) (timestamp string))
	((textmatch '(isdigit+) string)
	 (timestamp (string->number string)))
	((textmatch iso-pattern string) (timestamp string))
	((textmatch rfc822-pattern string) (timestamp string))
	(else (let ((matches
		     (text->frames (if us us-patterns terran-patterns) string)))
		(when (test matches 'ampm '{"PM" "pm"})
		  (let ((hours (get matches 'hour)))
		    (unless (> (+ 12 hours) 24)
		      (store! matches 'hours (+ 12 hours)))))
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
		  (matches->timestamps matches (cdr fields) base))))))

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
