(in-module 'jsonout)

(use-module 'reflection)

(define json-lisp-prefix ":")

(defambda (jsonelt value (prefix #f) (initial #f))
  (printout (unless initial ",") (if prefix prefix)
	    (if (ambiguous? value) (printout "["))
	    (do-choices (value value i)
	      (when (> i 0) (printout ","))
	      (jsonout value #f))
	    (if (ambiguous? value) (printout "]"))))
(define (jsonvec vector)
  (printout "[" (doseq (elt vector i)
		  (if (> i 0) (printout ", "))
		  (jsonout elt #f))
    "]"))

(defambda (jsonfield field value (valuefn #f) (prefix #f) (context #f))
  (unless (fail? value)
    (printout
      (if prefix prefix)
      (if (symbol? field)
	  (write (downcase (symbol->string field)))
	  (if (string? field) (write field)
	      (write (unparse-arg field))))
      ": "
      (if (ambiguous? value) (printout "["))
      (do-choices (value value i)
	(when (> i 0) (printout ","))
	(if valuefn
	    (jsonout (valuefn value context) #f)
	    (jsonout value #f)))
      (if (ambiguous? value) (printout "]")))))
(define (jsontable table (valuefn #f) (context #f))
  (printout "{"
	    (let ((initial #t))
	      (do-choices (key (getkeys table) i)
		(let ((v (get table key)))
		  (when (exists? v)
		    (unless initial (printout ", "))
		    (jsonfield key v valuefn "" context)
		    (set! initial #f)))))
	    
	    "}"))

(defambda (jsonout value (onfail "[]"))
  (cond ((ambiguous? value)
	 (printout "[" (do-choices (v value i)
			 (printout (if (> i 0) "," (jsonout v))))
		   "]"))
	((fail? value) (if onfail (printout onfail)))
	((number? value) (printout value))
	((string? value) (printout (write value)))
	((vector? value) (jsonvec value))
	((eq? value #t) (printout "true"))
	((eq? value #f) (printout "false"))
	((timestamp? value) (printout (get value 'tick)))
	((oid? value)
	 (printout "\"" (oid->string value) "\""))
	((table? value) (jsontable value))
	(else (printout "\"" json-lisp-prefix (write value) "\""))))

(module-export! '{jsonout jsonvec jsontable jsonfield jsonelt})

;;; Support for JSON responses

(define (jsonp/open (var #f) (assign #f) (callback #f))
  (if var (printout "var " var "=")
      (if assign
	  (printout assign "=")
	  (if callback (printout callback "(")))))
(define (jsonp/close (var #f) (assign #f) (callback #f))
  (if (or var assign callback)
      (printout (if callback ")") ";")))

(module-export! '{jsonp/open jsonp/close})
