;;;; -*- Mode: Scheme; -*-

(in-module 'kno/primdecls)

(use-module '{reflection texttools logger})

(define add-header
  "#include \"kno/cprims.h\"")

(module-export! '{output-cprim-decl output-cprim-link
		  get-cprim-decl get-cprim-link
		  getallprimsbyfile getallprims})

(define (output-cprim-decl f (kno-prefix "KNO_"))
  (let ((typeinfo (procedure-typeinfo f))
	(defaults (procedure-defaults f))
	(arity (procedure-arity f)))
    (printout kno-prefix "DEFPRIM" (or (and typeinfo defaults arity) "")
      "(\"" (downcase (procedure-name f)) "\"," (procedure-cname f) ","
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
    (printout ");\n")))

(define (output-cprim-link f (kno-prefix "KNO_"))
  (if (procedure-arity f)
      (printout kno-prefix "LINK_PRIM("
	"\"" (downcase (procedure-name f)) "\"," (procedure-cname f) ","
	(procedure-arity f) "," (glom (procedure-module f) "_module") ");")
      (printout kno-prefix "LINK_VARARGS("
	"\"" (downcase (procedure-name f)) "\"," (procedure-cname f) "," 
	(glom (procedure-module f) "_module") ");")))

(define (output-cprim-alias f (kno-prefix "KNO_"))
  (printout kno-prefix "LINK_ALIAS("
    "\"" (downcase (procedure-name f)) "\"," (procedure-cname f) ","
    (glom (procedure-module f) "_module") ");"))

(define (get-cprim-decl f (kno-prefix "KNO_"))
  (stringout kno-prefix (output-cprim-decl f kno-prefix)))
(define (get-cprim-link f (kno-prefix "KNO_"))
  (stringout (output-cprim-link f kno-prefix)))

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
	((eq? val #\\) "KNO_CODE2CHAR('\\\\')")
	((character? val)
	 (stringout "KNO_CODE2CHAR('" (string val) "')"))
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
		"KNO_VAR_ARGS")
	    flags))
    (stringout (dolist (flag flags i)
		 (printout  (if (> i 0) "|") flag)))))

(define (get-docstrings f (string))
  (set! string (procedure-documentation f))
  (cond ((or (not (string? string)) (empty-string? string))
	 (list
	  (stringout "`(" (procedure-name f)
	    (when (and (procedure-min-arity f) (>= (procedure-min-arity f) 0))
	      (dotimes (i (procedure-min-arity f))
		(printout " *arg" i "*"))
	      (if (or (not (procedure-arity f)) (< (procedure-arity f) 0))
		  (printout " *args...*")
		  (dotimes (i (- (procedure-arity f) (procedure-min-arity f)))
		    (printout " [*arg" (+ (procedure-min-arity f) i) "*]"))))
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

;;;; Getting the end of the header

(define (get-header-end string)
  (let ((start 0)
	(pos (textsearch #((bol) "#include \"" (not> (eol))) string))
	(last #f))
    (while (and (exists? pos) pos)
      (set! last (textmatcher #((bol) "#include " (not> (eol))) string pos))
      (set! start last)
      (set! pos (textsearch #((bol) "#include \"" (not> (eol))) string start)))
    last))

(define (proc-name-length p) (length (procedure-name p)))

(define (get-aliases moduleids)
  (let ((table (make-hashtable)))
    (do-choices (moduleid (pick moduleids symbol?))
      (let ((module (get-module moduleid)))
	(do-choices (key (getkeys module))
	  (let ((value (get module key)))
	    (when (primitive? value)
	      (unless (eq? key (procedure-symbol value))
		(add! table value (cons key moduleid))))))))
    table))

(define (annotate-file file (content))
  (default! content (filestring file))
  (let* ((prims (get primsbyfile file))
	 (aliases (get-aliases (procedure-module prims)))
	 (cprim-names (procedure-cname prims))
	 (pattern `#((bol) (opt "static") (spaces*) "lispval" (spaces) ,cprim-names (spaces*) "("))
	 (extract `#((bol) (opt "static") (spaces*) "lispval" (spaces) (label cprim ,cprim-names) (spaces*) "("))
	 (header-end (get-header-end content))
	 (converted {})
	 (declared '()))
    (printout (slice content 0 header-end))
    (printout "\n" add-header "\n")
    (dolist (block (textslice (slice content header-end) pattern 'prefix))
      (let ((match (text->frames extract block)))
	(when (singleton? (get match 'cprim))
	  (let ((cprim (pick prims procedure-cname (get match 'cprim))))
	    (cond ((singleton? cprim)
		   (output-cprim-decl cprim)
		   (set! declared (cons cprim declared)))
		  ((ambiguous? cprim)
		   (let ((top (pick-one (largest cprim proc-name-length))))
		     (logwarn |Ambigous prims|
		       (procedure-name cprim) " main definition is " (procedure-name top))
		     (output-cprim-decl top)
		     (set! declared (cons top declared))
		     (do-choices (alias (difference cprim top))
		       (set! declared (cons (list alias) declared))))))
	    (do-choices (cp cprim)
	      (when (test aliases cp)
		(logwarn |Aliases| "Emitting alises for " cp ": " (get aliases cp))
		(do-choices (alias (get aliases cp))
		  (printout " KNO_LINK_ALIAS(\"" (car alias) "\"," (procedure-cname cp) ","
		    (cdr alias) "_module);\n"))))))
	(printout block)))
    (printout "\n\nstatic void link_local_cprims()\n{\n"
      (dolist (prim declared)
	(if (pair? prim)
	    (printout "  " (output-cprim-alias (car prim)) "\n")
	    (printout "  " (output-cprim-link prim) "\n")))
      "}\n")
    (do-choices (missing (difference prims {(pick (elts declared) primitive?) (car (pick (elts declared) pair?))}))
      (lineout (output-cprim-decl missing)))))
(module-export! 'annotate-file)

(define (convert-file file)
  (let ((content (filestring file)))
    (fileout file (annotate-file file content))))
(module-export! 'convert-file)
