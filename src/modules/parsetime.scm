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
	     (IC (label MONTH ,monthstrings ,monthnum)) (opt ",")
	     (spaces)
	     (opt (label YEAR #({"1" "2"} (isdigit) (isdigit) (isdigit)) #t)))
	  `#({(bol) (spaces)}
	     (IC (label MONTH ,monthstrings ,monthnum))
	     (spaces*)
	     (label DATE #((isdigit) (opt (isdigit))) #t) (opt ",")
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
	  #((label MONTH #((isdigit) (opt (isdigit)))) "/"
	    (label DATE #((isdigit) (opt (isdigit))))
	    (opt (label YEAR #("/" (isdigit+)))))))
(define terran-patterns
  (choice generic-patterns
	  #((label DATE #((isdigit) (opt (isdigit)))) "/"
	    (label MONTH #((isdigit) (opt (isdigit))))
	    (opt (label YEAR #("/" (isdigit+)))))
	  #((label DATE #((isdigit) (opt (isdigit)))) "."
	    (label MONTH #((isdigit) (opt (isdigit))))
	    (opt (label YEAR #("." (isdigit+)))))))

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

(define (parsetime string (base #f) (us #t))
  (let ((matches
	 (text->frames (qc (if us us-patterns terran-patterns))
		       string)))
    (when (test matches 'ampm '{"PM" "pm"})
      (let ((hours (get matches 'hours)))
	(unless (> (+ 12 hours) 24)
	  (store! matches 'hours (+ 12 hours)))))
    (if (or (ambiguous? (get matches 'year))
	    (ambiguous? (get matches 'month))
	    (ambiguous? (get matches 'date))
	    (ambiguous? (get matches 'hour))
	    (ambiguous? (get matches 'minutes)))
	(modtime (pick matches 'year) (or base (timestamp)))
	(modtime (qc matches) (or base (timestamp))))))

;; This needs to do something special.
(define (parsegmtime string (base #f) (us #t))
  (parsetime string base us))

