;;;; -*- Mode: Scheme; -*-

(in-module 'kno/primdecls)

(use-module '{reflection texttools logger})

(module-export! '{output-cprim-decl output-cprim-link
		  get-cprim-decl get-cprim-link
		  getallprimsbyfile getallprims})

(define (output-cprim-decl f (kno-prefix "KNO_"))
  (let ((typeinfo (procedure-typeinfo f))
	(defaults (procedure-defaults f))
	(arity (procedure-arity f)))
    (printout kno-prefix "DEFPRIM" (or (and typeinfo defaults arity) "")
      "\n(\"" (downcase (procedure-name f)) "\"," (procedure-cname f) ","
      (get-cprim-flags f kno-prefix)
      (dolist (line (get-docstrings f) i)
	(printout (if (= i 0) ",\n " "\n ") (write line)))
      (when (and typeinfo defaults)
	(dotimes (i arity)
	  (printout 
	    (if (zero? (remainder i 2))
		",\n "
		",")
	    (output-type (elt typeinfo i)) ","
	    (output-default (elt defaults i))))
	"\n"))
    (printout ")\n")))

(define (output-cprim-link f (kno-prefix "KNO_"))
  (if (procedure-arity f)
      (printout "LINK_PRIM("
	(downcase (procedure-name f)) "\"," (procedure-cname f) ","
	(procedure-arity f) "," (glom (procedure-module f) "_module") ");")
      (printout "LINK_VARARGS("
	(downcase (procedure-name f)) "\"," (procedure-cname f) "," 
	(glom (procedure-module f) "_module") ");")))

(define (get-cprim-decl f (kno-prefix "KNO_"))
  (stringout (output-prim-decl f kno-prefix)))
(define (get-cprim-link f) (stringout (output-cprim-link f)))

(define type-name-map
  #[])

(define (output-type type)
  (if (not type) "kno_any_type"
      (try (get type-name-map type)
	   (glom "kno_" type "_type"))))

(define (output-default val)
  (cond ((fail? val) "KNO_EMPTY")
	((eq? val '%void) "KNO_VOID")
	((eq? val #t) "KNO_TRUE")
	((eq? val #f) "KNO_FALSE")
	((eq? val #default) "KNO_DEFAULT")
	((nil? val) "KNO_EMPTY_LIST")
	((fixnum? val)
	 (stringout "KNO_INT(" val ")"))
	(else (logwarn |BadDefaultValue| "Couldn't render " val)
	      "KNO_VOID")))

(define (get-cprim-flags f kno-prefix)
  (let ((flags '()))
    (when (non-deterministic? f)
      (set! flags (cons "KNO_NDCALL" flags)))
    (set! flags
      (cons (glom kno-prefix "MIN_ARGS(" (procedure-min-arity f) ")") flags))
    (set! flags
      (cons (if (procedure-arity f) 
		(glom kno-prefix "MAX_ARGS(" (procedure-arity f) ")")
		"KNO_VARARGS")
	    flags))
    (stringout (dolist (flag flags i)
		 (printout  (if (> i 0) "|") flag)))))

(define (get-docstrings f (string))
  (set! string (procedure-documentation f))
  (cond ((or (not (string? string)) (empty-string? string))
	 (list
	  (stringout "`(" (procedure-name f)
	    (dotimes (i (procedure-min-arity f))
	      (printout " *arg" i "*"))
	    (if (< (procedure-arity f) 0)
		(printout " *args...*")
		(dotimes (i (- (procedure-arity f) (procedure-min-arity f)))
		  (printout " [*arg" (+ (procedure-min-arity f) i) "*]")))
	    ")` **undocumented**")))
	((textmatch #((spaces*) (opt "`") "(" (not> ")") ")" (opt "`")
		      (spaces*))
		    string)
	 (list (glom string " **undocumented**")))
	(else (breakup-docstring string))))


(define (breakup-docstring string)
  (let* ((call-ends 
	  (textmatcher #((spaces*) (opt "`") "(" (not> ")") ")" (opt "`")
			 (spaces*))
		       string))
	 (words (if (fail? call-ends)
		    (textslice string '(isspace))
		    (textslice (slice string (largest call-ends)) '(isspace))))
	 (lines (if (fail? call-ends) '()
		    (list (slice string 0 (largest call-ends)))))
	 (line '())
	 (len 0))
    (while (pair? words)
      (let ((word (car words)))
	(when (>  (+ len (length word)) 50)
	  (set! lines (cons (apply glom (reverse line)) lines))
	  (set! line '())
	  (set! len 0))
	(set! line (cons word line))
	(set! len (+ len (length word))))
      (set! words (cdr words)))
    (set! lines (cons (apply glom (reverse line)) lines))
    (reverse lines)))

;;;; Getting all prims

(define (get-modprims table)
  (pick (getvalues table) primitive?))

(define (getallprims)
  (get-modprims (pick (cdr (all-modules)) hashtable?)))

(define (getallprimsbyfile)
  (let ((table (make-hashtable)))
    (do-choices (prim (get-modprims (pick (cdr (all-modules)) hashtable?)))
      (add! table (procedure-filename prim) prim))
    table))

(define primsbyfile (getallprimsbyfile))

(define (annotate-file file)
  (let* ((content (filestring file))
	 (prims (get primsbyfile file))
	 (cprim-names (procedure-cname prims))
	 (pattern `#((bol) (opt "static") (spaces*) "lispval" (spaces) ,cprim-names (spaces*) "("))
	 (extract `#((bol) (opt "static") (spaces*) "lispval" (spaces) (label cprim ,cprim-names) (spaces*) "("))
	 (declared '()))
    (dolist (block (textslice content pattern 'prefix))
      (let ((match (text->frames extract block)))
	(when (singleton? (get match 'cprim))
	  (let ((cprim (pick prims procedure-cname (get match 'cprim))))
	    (output-cprim-decl cprim)
	    (set! declared (cons cprim declared))))
	(printout block)))
    (printout "\n\nstatic void init_cprims(){\n"
      (dolist (prim declared)
	(printout "  " (output-cprim-link prim) "\n"))
      "}\n")))
(module-export! 'annotate-file)

(define (convert-file file)
  (let ((new-content (annotate-file file)))
    (write-file file new-content)))
(module-export! 'convert-file)
