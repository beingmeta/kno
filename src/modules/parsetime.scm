(in-module 'parsetime)

(use-module 'texttools)

(module-export! '{parsetime parsegmtime})

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
	  (qc "Nov" "Novemember")
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

(define generic-patterns
  (choice `#({(bol) (spaces)}
	     (label DATE #((isdigit) (opt (isdigit))) #t)
	     (spaces)
	     (IC (label MONTH ,monthstrings ,monthnum)) (opt #({"" (spaces)} ","))
	     (spaces)
	     (opt (label YEAR #({"1" "2"} (isdigit) (isdigit) (isdigit)) #t)))
	  `#({(bol) (spaces)}
	     (IC (label MONTH ,monthstrings ,monthnum))
	     (spaces*)
	     (label DATE #((isdigit) (opt (isdigit))) #t) (opt #({"" (spaces)} ","))
	     (spaces)
	     (opt (label YEAR #({"1" "2"} (isdigit) (isdigit) (isdigit)) #t)))
	  `#({(bol) (spaces)}
	     (IC (label MONTH ,monthstrings ,monthnum))
	     (spaces)
	     (opt (label YEAR #({"1" "2"} (isdigit) (isdigit) (isdigit)) #t)))
	  `#({(spaces) (bol)}
	     (label HOURS #((isdigit) (opt (isdigit))) #t)
	     {(spaces) (eol) 
	      #(":" (label MINUTES #((isdigit) (isdigit)) #t)
		{(spaces) (eol)
		 #(":" (label SECONDS #((isdigit) (isdigit)) #t)
		   {(spaces) (eol) #("." (label FRACTION (isdigit+)))})})})
	  `#({(bol) (spaces)} (label AMPM (IC {"AM" "PM"})) {(eol) (spaces)})
	  `#({(bol) (spaces)}
	     (label TIMEZONE
		    {(IC ,timezones)
		     #({"+" "-"} (isdigit) (opt (isdigit))
		       (opt #(":" (isdigit) (isdigit))))})
	     {(eol) (spaces)}))
  )

(define us-patterns
  (choice generic-patterns
	  #((label MONTH #((isdigit) (opt (isdigit))) #t) "/"
	    (label DATE #((isdigit) (opt (isdigit))) #t)
	    (opt #("/" (label YEAR (isdigit+)))))))
(define terran-patterns
  (choice generic-patterns
	  #((label DATE #((isdigit) (opt (isdigit))) #t) "/"
	    (label MONTH #((isdigit) (opt (isdigit))) #t)
	    (opt #("/" (label YEAR (isdigit+)))))
	  #((label DATE #((isdigit) (opt (isdigit))) #t) "."
	    (label MONTH #((isdigit) (opt (isdigit))) #t)
	    (opt #("." (label YEAR (isdigit+)))))))

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

(define (parsetime string (base #f) (us #f))
  (let ((matches
	 (text->frames (qc (if us us-patterns terran-patterns))
		       string)))
    (when (test matches 'ampm '{"PM" "pm"})
      (let ((hours (get matches 'hours)))
	(unless (> (+ 12 hours) 24)
	  (store! matches 'hours (+ 12 hours)))))
    (when (and (test matches 'year) (string? (get matches 'year)))
      (if (= (length (get matches 'year)) 2)
	  (store! matches 'year (+ 1900 (string->number (get matches 'year))))
	  (store! matches 'year (string->number (get matches 'year)))	  ))
    ;; Handle the case where people swapped the month and date according
    ;;  to US conventions .vs. the rest of the world
    (when (and (test matches 'month) (test matches 'date)
	       (> (get matches 'month) 12)
	       (<= (get matches 'date) 12))
      (let ((m (get matches 'month)) (d (get matches 'date)))
	(store! matches 'month d) (store! matches 'date m)))
    (if (or (ambiguous? (get matches 'year))
	    (ambiguous? (get matches 'month))
	    (ambiguous? (get matches 'date))
	    (ambiguous? (get matches 'hours))
	    (ambiguous? (get matches 'minutes)))
	(modtime (pick matches 'year) (or base (timestamp)))
	(modtime (qc matches) (or base (timestamp))))))

(define (parsegmtime string (base #f) (us #f))
  (parsetime string (or base (gmtimestamp)) us))


