;;;; -*- Mode: Scheme; -*-

(in-module 'kno/primdecls)

(use-module '{reflection texttools logger})

(define add-header
  "#include \"kno/cprims.h\"")
(define output-suffix ".new")
(define output-suffix #f)

(module-export! '{output-cprim-decl output-cprim-link
		  get-cprim-decl get-cprim-link get-cprim-argnames
		  getallprimsbyfile getallprims
		  primsbyfile update-primsbyfile
		  convert-file convert-all
		  update-file update-all
		  getsourcepath})

(define-init kno-root "/src/kno/")

(define-init source-roots
  {kno-root (getdirs (mkpath kno-root "contrib/"))})
(define-init source-sigs (make-hashtable))

(define (getsourcepath path)
  (try (tryif (file-exists? path) path)
       (try-choices (root source-roots)
	 (tryif (file-exists? (mkpath root path)) (mkpath root path)))))

;;; Outputting expressions

(define (output-cprim-decl f (kno-prefix "KNO_") (suffix "\n"))
  (let ((typeinfo (procedure-typeinfo f))
	(defaults (procedure-defaults f))
	(argnames (get-cprim-argnames f))
	(arity (procedure-arity f)))
    (printout kno-prefix (if arity "DEFCPRIM" "DEFCPRIMN")
      "(\"" (downcase (procedure-name f)) "\"," (procedure-cname f) 
      ",\n " (get-cprim-flags f kno-prefix)
      (dolist (line (get-docstrings f) i)
	(printout (if (= i 0) ",\n " "\n ") (write line)))
      (when arity
	(dotimes (i arity)
	  (printout
	    ",\n "
	    (write (elt argnames i)) ","
	    (output-type (and typeinfo (elt typeinfo i))) ","
	    (output-default (if defaults (elt defaults i) '%void)))))))
    (printout ")\n"))

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
	   (downcase (glom "kno_" type "_type")))))

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

;;;; Getting arg names

(define sig-pattern 
  {#({(isspace) (bol)}
     "lispval " (spaces*) (csymbol) (spaces*) "("
     #("lispval" (spaces) (* #({"U8_MAYBE_UNUSED"} (spaces))) #((csymbol)))
     (* #((spaces*) "," (spaces*) "lispval" (spaces) (* #({"U8_MAYBE_UNUSED"} (spaces))) #((csymbol))))
     ")")
   #({(isspace) (bol)}
     "lispval " (spaces*) (csymbol) (spaces*) "(" (spaces*) ")")})

(define (get-argname extracted)
  (first (pick (elts extracted) vector?)))

(define (get-argnames string)
  (let* ((extract (textract (qc sig-pattern) string))
	 (name (elt extract 3)))
    (if (vector? (elt extract 6))
	(let ((arg1 (get-argname (elt extract 6)))
	      (args (map get-argname (cdr (elt extract 7)))))
	  (cons* name arg1 args))
	(list name))))

(define (getsigs filename (srcfile))
  (default! srcfile (getsourcepath filename))
  (try (get source-sigs filename)
       (get source-sigs srcfile)
       (let* ((contents (filestring srcfile))
	      (decls (gather (qc sig-pattern) contents))
	      (table (make-hashtable)))
	 (do-choices (decl decls)
	   (let ((argnames (get-argnames decl)))
	     ;; (%watch filename "fcn" (car argnames) "args" (cdr argnames))
	     (store! table (car argnames) (cdr argnames))))
	 (store! source-sigs filename table)
	 (store! source-sigs (getsourcepath filename) table)
	 table)))

(define (get-cprim-argnames f)
  (get (getsigs (procedure-filename f))
       (procedure-cname f)))

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
(define (update-prims-by-file)
  (set! primsbyfile (getallprimsbyfile)))

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
  (set! file (getsourcepath file))
  (default! content (filestring file))
  (let* ((prims (get primsbyfile file))
	 (aliases (get-aliases (procedure-module prims)))
	 (cprim-names (procedure-cname prims))
	 (pattern `#((bol) (opt {"static" "KNO_INLINE_FCN" "KNO_EXPORT"}) (spaces*)
		     "lispval" (spaces) ,cprim-names (spaces*) "("))
	 (extract `#((bol) (opt {"static" "KNO_INLINE_FCN"}) (spaces*)
		     "lispval" (spaces) (label cprim ,cprim-names) (spaces*) "("))
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

(define defprim-pattern
  {#({"DEFPRIM" "KNO_DEFPRIM"}
     (opt (isdigit)) (spaces*) "("
     (not> ",") "," (spaces*)
     (not> ",") "," (spaces*)
     (not> ",") "," (spaces*)
     (* #("\"" (not> "\"") "\"" (spaces*)))
     (opt #("," (not> {");" ")\n"}))) 
     ")" (opt ";"))
   #({"DEFPRIM" "KNO_DEFPRIM"}
     (opt #((isdigit) (isdigit))) (spaces*) "("
     (not> ",") "," (spaces*)
     (not> ",") "," (spaces*)
     (not> ",") "," (spaces*)
     (* #("\"" (not> "\"") "\"" (spaces*)))
     (opt #("," (not> {");" ")\n"}))) 
     ")" (opt ";"))})

(define (update-annotations file (content))
  (default! content (filestring (getsourcepath file)))
  (set! content (textsubst content (qc defprim-pattern) ""))
  (set! content (textsubst content "KNO_LINK_PRIM" "KNO_LINK_CPRIM"))
  (let* ((prims (get primsbyfile file))
	 (aliases (get-aliases (procedure-module prims)))
	 (cprim-names (procedure-cname prims))
	 (pattern `#((bol) (opt {"static" "KNO_INLINE_FCN" "KNO_EXPORT"}) (spaces*)
		     "lispval" (spaces) ,cprim-names (spaces*) "("))
	 (extract `#((bol) (opt {"static" "KNO_INLINE_FCN" "KNO_EXPORT"}) (spaces*)
		     "lispval" (spaces) (label cprim ,cprim-names) (spaces*) "("))
	 (header-end (get-header-end content))
	 (converted {})
	 (declared '()))
    (printout (slice content 0 header-end))
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
		       (set! declared (cons (list alias) declared))))))))
	(printout block)))
    (do-choices (missing (difference prims 
				     {(pick (elts declared) primitive?)
				      (car (pick (elts declared) pair?))}))
      (lineout (output-cprim-decl missing)))))
(module-export! 'update-annotations)

(define (convert-file file)
  (set! file (getsourcepath file))
  (let ((content (filestring file)))
    (fileout file (annotate-file file content))
    (logwarn |ConvertedFile| file)))
(module-export! 'convert-file)

(define (update-file file)
  (let* ((usepath (getsourcepath file))
	 (content (filestring usepath)))
    (logwarn |Updating| usepath " for " file)
    (unless output-suffix
      (unless (file-exists? (glom usepath ".bak"))
	(system "cp " usepath " "  (glom usepath ".bak"))))
    (let ((updated (stringout (update-annotations file content))))
      (write-file (glom usepath output-suffix) updated))
    (logwarn |Updated| usepath " for " file)))
(module-export! 'update-file)

(define (convert-all)
  (do-choices (file (getkeys primsbyfile))
    (if (test primsbyfile file)
	(convert-file file)
	(logwarn |NoPrims| file))))

(define (update-all)
  (do-choices (file (getkeys primsbyfile))
    (if (test primsbyfile file)
	(update-file file)
	(logwarn |NoPrims| file))))

;;; Cases

(comment
 (use-module '{exif qrcode imagick nng sundown hyphenate
	       mongodb mariadb rocksdb leveldb tidy zeromq
	       ziptools odbc crypto sqlite archivetools testcapi zlib})
 (use-module 'kno/primdecls))
