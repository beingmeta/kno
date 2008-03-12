(in-module 'logctl)

(use-module 'logger)

(module-export! 'logctl!)

(define (convert-log-arg arg)
  (if (and (pair? arg) (eq? (car arg) 'quote) (pair? (cdr arg))
	      (symbol? (cadr arg)))
      (getloglevel (cadr arg))
      (getloglevel arg)))

(define logctl!
  (macro expr
    (let ((modname (second expr))
	  (loglevel (convert-log-arg (third expr))))
      (if (and (pair? modname) (eq? (car modname) 'quote)
	       (pair? (cdr modname)))
	  (set! modname (cadr modname)))
      (cond ((fail? loglevel)
	     (error "Bad LOGLEVEL" expr)))
      (if (symbol? modname)
	  `(within-module ',modname (define %loglevel ,loglevel))
	  `(within-module ,modname (define %loglevel ,loglevel))))))


