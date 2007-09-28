(in-module 'logctl)

(module-export! 'logctl!)

(define loglevel-map
  {(DEBUG . 7) (DBG . 7) (INFO . 6)
   (NOTICE . 5) (NOTE . 5) (NOTIFY . 5)
   (WARN 4) (WARNING 4)
   (ERROR 3) (ERR 3)
   (ERROR 3) (ERR 3)
   (CRITICAL 2) (CRIT 2)
   (ALERT 1) (EMERGENCY 0) (EMERG 0)})

(define loglevel-table
  (let ((table (make-hashtable)))
    (do-choices (map loglevel-map)
      (add! table (downcase (symbol->string (car map))) (cdr map)))
    table))

(define (convert-log-arg arg)
  (cond ((integer? arg) arg)
	((string? arg) (get loglevel-table (downcase arg)))
	((symbol? arg) (get loglevel-table (downcase (symbol->string arg))))
	((and (pair? arg) (eq? (car arg) 'quote) (pair? (cdr arg))
	      (symbol? (cadr arg)))
	 (get loglevel-table (downcase (symbol->string (cadr arg)))))
	(else (fail))))

(define logctl!
  (macro expr
    (let ((modname (second expr))
	  (loglevel (third expr)))
      (if (and (pair? modname) (eq? (car modname) 'quote)
	       (pair? (cdr modname)))
	  (set! modname (cadr modname)))
      (if (and (pair? loglevel) (eq? (car loglevel) 'quote)
	       (pair? (cdr loglevel)))
	  (set! loglevel (cadr loglevel)))
      (set! loglevel (convert-log-arg loglevel))
      (cond ((not (symbol? modname))
	     (error "Bad LOGCTL! call" expr))
	    ((fail? loglevel)
	     (error "Bad LOGLEVEL" expr)))
      `(within-module ',modname (define %loglevel ,loglevel)))))

