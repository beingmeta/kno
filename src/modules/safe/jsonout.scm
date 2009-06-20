(in-module 'jsonout)

(define (jsonvec vector)
  (printout "[" (dotimes (elt vector i)
		  (if (> i 0) (printout ", "))
		  (jsonout elt)) "]"))
(define (jsontable table)
  (printout "{"
	    (do-choices (key (getkeys table) i)
	      (let ((v (get table key)))
		(when (exists? v)
		  (if (> i 0) (printout ", "))
		  (if (string? key) (write key)
		      (if (symbol? key)
			  (printout "\"" (downcase (symbol->string key)) "\"")
			  (printout "\"" key "\"")))
		  (if (ambiguous? v)
		      (printout ": ["
				(do-choices (v v i)
				  (printout (if (> i 0) ", ") (jsonout v))))
		      (printout ": " (jsonout v))))))
	    "}"))

(defambda (jsonout value)
  (cond ((ambiguous? value))
	((fail? value) (printout "[]"))
	((number? value) (printout value))
	((string? value) (write value))
	((vector? value) (jsonvec value))
	((table? value) (jsontable value))
	(else (write value))))

(module-export! 'jsonout)

