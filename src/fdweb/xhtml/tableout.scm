(in-module 'xhtml/tableout)

(use-module '{fdweb xhtml})

(module-export! 'tableout)

(define (tableout arg (class "fdjtdata") (skipempty #f))
  (let ((keys (getkeys arg)))
    (table* ((class "fdjtdata"))
      (do-choices (key keys)
	(let ((values (get arg key)))
	  (cond ((empty? values)
		 (unless skipempty
		   (tr (th key) (td* ((class "empty")) "No values"))))
		((singleton? values)
		 (tr (th key) (td values)))
		(else (do-choices (value values i)
			(if (zero? i)
			    (tr (th* ((rowspan (choice-size values)))
				  key)
				(td value))
			    (tr (td value)))))))))))

