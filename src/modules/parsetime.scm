(in-module 'parsetime)

(use-module 'texttools)

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
  (choice `#((label DATE #((isdigit) (opt (isdigit))) #t)
	     (spaces*)
	     (label MONTH ,monthstrings ,monthnum)
	     (spaces*)
	     (opt (label YEAR #({"1" "2"} (isdigit) (isdigit) (isdigit)) #t)))
	  `#((label MONTH ,monthstrings ,monthnum)
	     (spaces*)
	     (label DATE #((isdigit) (opt (isdigit))) #t)
	     (spaces)
	     (opt (label YEAR #({"1" "2"} (isdigit) (isdigit) (isdigit)) #t)))
	  `#((label HOUR #((isdigit) (opt (isdigit))) #t) ":"
	     (label MINUTES #((isdigit) (isdigit)) #t)
	     (opt #(":" (label SECONDS #((isdigit) (isdigit)) #t)))
	     (spaces*)
	     (opt (label AMPM (IC {"AM" "PM"})))
	     (spaces*)
	     (opt (label TIMEZONE (IC ,timezones))))
	  )
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

(define (parsetime string (base #f) (us #t))
  (let ((matches (text->frames (if us (qc us-patterns) (qc terran-patterns))
			       string)))
    (if (exists? matches)
	(modtime (qc matches) (or base (gmtimestamp)))
	(fail))))



