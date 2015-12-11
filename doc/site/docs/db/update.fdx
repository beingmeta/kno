(use-module 'htmlgen)
(use-module 'xmlgen)
(use-module 'fdtext)
(use-module 'osprims)
(use-module 'fdinternals)

(auto-cache-file-pools)
(auto-cache-file-indices)

(set+! %stop-slots 'documentation)

(define docdb (use-pool (get-component "docdb")))
(define docindex (use-index (get-component "docdb")))

(define (get-frame-for-symbol symbol)
  (try (find-frames docindex 'symbol symbol)
       (frame-create docdb
	 'symbol symbol
	 'documentation ""
	 'keywords 'fdscript)))

(define (get-value-type x)
  (cond ((and (pair? x) (eq? (car x) 'macro)) 'macro)
	((not (procedure? x)) 'variable)
	((special-form? x) 'special-form)
	((primitive? x) 'primitive)
	((continuation? x) 'continuation)
	((applicable? x) 'procedure)
	(else 'script)))

(define (arglist->argtypes arglist)
  (if (symbol? arglist) 'lisp
    (if (pair? arglist)
	(cons (arglist->argtypes (car arglist))
	      (arglist->argtypes (cdr arglist)))
      arglist)))

(define (document-symbol symbol)
  (if (bound? symbol)
      (let* ((frame (get-frame-for-symbol symbol))
	     (value (if (symbol-bound? symbol) (eval symbol) 'unbound))
	     (type (if (not (symbol-bound? symbol)) 'unbound
		       (get-value-type (eval symbol)))))
	(pset! frame 'obj-name (list type symbol))
	(frame-add! frame 'type type)
	(when (empty? (fget frame 'arglist))
	  (when (applicable? value)
	    (frame-add! frame 'arglist (arglist value))
	    (frame-add! frame 'argtypes (arglist->argtypes (arglist value)))
	    (frame-add! frame 'value-type 'lisp)
	    (fdd frame)))
	frame)
      (fail)))

(define (comparison-documentation symbol-name)
  (stringout "Compares two "
	     (cond ((tx-search "CHAR" symbol-name) "characters")
		   ((tx-search "STRING" symbol-name) "strings"))
	     (if (tx-search "CI" symbol-name) " case insensitively" "")))

(define (guess-properties frame)
  (let* ((symbol (fget frame 'symbol))
	 (type   (fget frame 'type))
	 (symbol-name (symbol->string symbol)))
    (cond ((or (eq? type 'unbound) (eq? type 'variable)))
	  ((eq? type 'special-form)
	   (when (or (tx-search "DO" symbol-name)
		     (tx-search "FOR" symbol-name))
	     (fadd! frame 'keywords 'iteration)))
	  (else (when (has-suffix "?" symbol-name)
		  (frame-set! frame 'value-type 'boolean))
		(when (has-suffix "!" symbol-name)
		  (frame-add! frame 'keywords 'side-effect))
		(when (tx-match #("C" (+ {"A" "D"}) "R") symbol-name)
		  (frame-set! frame 'arglist '(pair))
		  (frame-set! frame 'argtypes '(pair))
		  (frame-add! frame 'keywords 'r4rs)
		  (frame-add! frame 'keywords 'common-lisp))
		(if (tx-search "POOL" symbol-name)
		    (frame-add! frame 'keywords (amb 'framerd 'pools)))
		(if (tx-search "OID" symbol-name)
		    (frame-add! frame 'keywords (amb 'framerd 'OIDS)))
		(if (tx-search "HASH" symbol-name)
		    (frame-add! frame 'keywords 'HASH))
		(if (tx-search "INDEX" symbol-name)
		    (frame-add! frame 'keywords (amb 'framerd 'INDICES)))
		(if (tx-search "STRING" symbol-name)
		    (frame-add! frame 'keywords 'STRINGS))
		(if (tx-search "CHARACTER" symbol-name)
		    (frame-add! frame 'keywords 'CHARACTERS))
		(if (tx-search "CHAR" symbol-name)
		    (frame-add! frame 'keywords 'CHARACTERS))    
		(if (tx-search "DTYPE" symbol-name)
		    (frame-add! frame 'keywords 'DTYPES))
		(if (tx-search "FRAME" symbol-name)
		    (frame-add! frame 'keywords (amb 'FRAMES 'FRAMERD)))
		(if (tx-search "FILE" symbol-name)
		    (frame-add! frame 'keywords (amb 'FILESYSTEM 'IO)))
		(if (tx-search "PORT" symbol-name)
		    (frame-add! frame 'keywords 'IO))
		(when (tx-match #((+ {(isalnum) #\-}) (+ {"=" ">" "<"}) "?")
				symbol-name)
		  (frame-set! frame 'documentation
			      (comparison-documentation symbol-name))
		  (when (tx-search "CHAR" symbol-name)
		    (frame-set! frame 'argtypes '(character character))
		    (frame-set! frame 'arglist '(char0 char1)))
		  (when (tx-search "STRING" symbol-name)
		    (frame-set! frame 'argtypes '(string string))
		    (frame-set! frame 'arglist '(string0 string1)))
		  )))
    frame))

(define (pass2)
  (do-choices (frame (pool-elts docdb))
    (let* ((sym (fget frame 'symbol)) (pname (symbol->string sym)))
      (when (tx-match #("C" (+ {"A" "D"}) "R") pname)
	(frame-set! frame 'documentation
		    "Retrieves a component of a CONS structure"))
      (when (has-prefix "CHAR-" pname)
	(fadd! frame 'keywords 'r4rs))
      (when (has-prefix "STRING-" pname)
	(fadd! frame 'keywords 'r4rs)))))

(define (examine-symbol sym)
  (let ((exists (find-frames docindex 'symbol sym)))
    (if (empty? exists)
	(guess-properties (document-symbol sym)))))

(define (html-decl x)
  (let* ((sym (string->symbol x))
         (frame (get-frame-for-symbol sym)))
    (fadd! frame 'keywords 'html)
    (fadd! frame 'arglist '(env-args . html-body))
    (fadd! frame 'argtypes '(env-bindings . expr-list))
    (fadd! frame 'value-type 'void)
    (let* ((starv (stringout x "*"))
           (starsym (probe-symbol starv)))
      (unless (empty? starsym)
	(fadd! (get-frame-for-symbol starsym) 'keywords 'html)
	(fadd! (get-frame-for-symbol starsym)
	       'arglist '(env-args . html-body))
	(fadd! (get-frame-for-symbol starsym)
	       'argtypes '(env-bindings . expr-list))
	(fadd! (get-frame-for-symbol starsym)
	       'value-type 'void)
	(pset! frame 'documentation
	       (stringout "Generates HTML text wrapped in a " x " tag"))
	(pset! (get-frame-for-symbol starsym) 'documentation
	       (stringout "Generates HTML text wrapped in a " x 
			  " tag (with arguments)"))))))
  
(define htmlprims
  (amb "START-HTML" "HTML" "HTMLDOC" "HTMLFILE" "HTMLSTRING"
       "SHOW-OIDS!" "SET-COOKIE!" "USE-BROWSE-SCRIPT!"
       "DECLARE-LOCAL-FRAME!" "HTML" "BODY-ARGS!" "HTMLDOC"
       "USE-BROWSE-SCRIPT" "TITLE" "LISTING"
       "H1" "H2" "H3" "H4"
       "CENTER" "TABLE" "P" "BLOCKQUOTE" "OL" "UL" "LI"
       "HR" "BR" "CODEBLOCK" "EM" "STRONG" "U" "SUB" "SUP"
       "TT" "ITALIC" "BOLD" "FONT" "SPAN" "FORM"
       "SELECT" "TEXTAREA" "INPUT" "OPTION" "TR" "TD" "TH"
       "ANCHOR" "TAG" "IMAGE" "HTMLENV" "HTMLINCLUDE"
       "DESCRIBE-OID" "SCRIPTURL" "SCRIPTINVOKE" "SCRIPT-INVOKE"
       "INDEX-URL" "WEBINDEX" "CGI-INIT"))

(define (make-link section symbol)
  (if (bound? symbol)
      (fadd! (get-frame-for-symbol symbol)
	     'r4rs-ref (stringout "r4rs.html#" section))
      (lineout symbol " undefined")))
(define (make-ref-link string)
  (let ((name (vector-ref
	       (tx-extract #("<A NAME=\"" (char-not "\"") "\">")
			   string)
			  1)))
    (unless (find #\Space name)
      (let* ((sym (string->symbol (string-upcase name)))
	     (frame (find-frames docindex 'symbol sym)))
	(when (frame? frame)
	  (fadd! frame 'fdscript-ref
		 (stringout "fdscript.html#" name)))))))

(define (build-cproc-table)
  (let ((cprocs (make-hashtable)))
    (do-choices (symbol (all-symbols))
      (if (and (symbol-bound? symbol) (primitive? (eval symbol)))
	  (hashtable-add! cprocs (eval symbol) symbol)))
    cprocs))

(define (merge-docs docs)
  (let* ((first-oid (first (sorted docs oid-addr-low)))
	 (first-name (first (reverse (sorted (get docs 'symbol))))))
    (notify "Merging into " first-oid " -- " docs)
    (set-oid-value!
     first-oid
     (frame-create #f
       'obj-name first-name 'symbol first-name 'type 'primitive
       'aliases (difference (get docs 'symbol) first-name)
       'documentation (get docs 'documentation)
       'value-type (get docs 'value-type)
       'argtypes (get docs 'argtypes) 'arglist (get docs 'arglist)
       'keywords (get docs 'keywords)
       'fdscript-ref (get docs 'fdscript-ref)))
    (cleanup-merger first-oid)
    (zap-oid! (difference docs first-oid))))
(define default-arglists
  (choice '(arg0) '(arg0 arg1) '(arg0 arg1 arg2) '(arg0 arg1 arg2 arg3)
	  'args 'arglist 'ndarglist))
(define default-argtypes
  (choice 'lisp '(lisp) '(lisp lisp) '(lisp lisp lisp)
	  '(lisp lisp lisp lisp) '(lisp lisp lisp lisp lisp)))
(define default-value-types 'lisp)
(define default-documentations "")

(define (cleanup-merger doc)
  (let ((arglist (get doc 'arglist))
	(argtypes (get doc 'argtypes))
	(value-type (get doc 'value-type))
	(documentation (get doc 'documentation)))
  (unless (or (empty? arglist) (singleton? arglist))
    (let ((nval (difference arglist default-arglists)))
      (unless (empty? nval) (%set! doc 'arglist nval))
      (unless (singleton? nval)
	(notify "Ambiguity for ARGLIST of " doc " -- " nval))))
  (unless (or (empty? argtypes) (singleton? argtypes))
    (let ((nval (difference argtypes default-argtypes)))
      (unless (empty? nval) (%set! doc 'argtypes nval))
      (unless (singleton? nval)
	(notify "Ambiguity for ARGTYPES of " doc " -- " nval))))
  (unless (or (empty? value-type) (singleton? value-type))
    (let ((nval (difference value-type default-value-types)))
      (unless (empty? nval) (%set! doc 'value-type nval))
      (unless (singleton? nval)
	(notify "Ambiguity for VALUE-TYPE of " doc " -- " nval))))
  (unless (or (empty? documentation) (singleton? documentation))
    (let ((nval (difference documentation default-documentations)))
      (unless (empty? nval) (%set! doc 'documentation nval))
      (unless (singleton? nval)
	(notify "Ambiguity for DOCUMENTATION of " doc " -- " nval)
	(%set! doc 'documentation (first (reverse (sorted nval length)))))))))

(define (zap-oid! oid)
  (set-oid-value! oid (frame-create #f 'type 'deleted)))

(define (do-merger)
  (let* ((table (build-cproc-table))
	 (cprocs (hashtable-keys table)))
    (do-choices (cproc cprocs)
      (unless (singleton? (find-frames docindex 'symbol (get table cproc)))
	(merge-docs (qc (find-frames docindex 'symbol (get table cproc))))))))

(define (update-db)
  (let ((cproc-table (build-cproc-table)))
    (do-choices (symbol (all-symbols))
      (when (and (symbol-bound? symbol) (primitive? (eval symbol)))
	(let* ((other-names (get cproc-table (eval symbol)))
	       (existing-frame 
		(find-frames docindex '{symbol aliases} (choice symbol other-names))))
	  (cond ((fail? existing-frame)
		 (lineout "Generating frame for " symbol)
		 (examine-symbol symbol))
		((test existing-frame '{symbol aliases} symbol))
		(else (lineout ";; Adding alias for " symbol " to " existing-frame)
		      (add! existing-frame 'aliases symbol))))))))

(define (index-all into)
  (let ((ix (make-file-index into 1000000)))
    (do-pool (oid docdb)
      (lineout oid)
      (index-frame ix oid)
      (unless (equal? (fget oid 'documentation) "")
	(let* ((doc (fget oid 'documentation))
	       (words (elts (segment doc))))
	  (index-frame ix oid 'textkeys
		       (amb words (english-stem words))))))))

(define etags-command
  "etags -o TAGS --regex=\"/Documentation of \\([^ ]+\\)\/\\1/\"")
(define (write-documentation into)
  (let ((file (open-output-file into)))
    (do-pool (oid docdb)
      (when (member (fget oid 'type)
		    '(special-form primitive variable slot))
	(printout-to file "Documentation of " (fget oid 'symbol)
		     "  [" (fget oid 'type) "]\n")
	(when (member (fget oid 'type) '(special-form primitive))
	  (unless (empty? (fget oid 'arglist))
	    (printout-to file
	      "\tReturns: \t" (fget oid 'result-type) "\n"))
	  (unless (empty? (fget oid 'arglist))
	    (printout-to file
	      "\tArglist: \t" (fget oid 'arglist) "\n"))
	  (unless (empty? (fget oid 'argtypes))
	    (printout-to file
	      "\tTypes: \t\t" (fget oid 'argtypes) "\n")))
	(printout-to file (fget oid 'documentation) "\n\n"))))
  (system etags-command " " into))

(define (export-doc-frame frame)
  (when (exists? (get frame 'value-type))
    `(,frame (documentation ,(get frame 'documentation))
	     (value-type ,(get frame 'value-type)) (argtypes ,(get frame 'argtypes)) (arglist ,(get frame 'arglist))
	     ,@(get-other-slots frame))))

(define (get-other-slots frame)
  (let ((slots '()))
    (doslots (f s v frame)
      (unless (member? s '(value-type argtypes arglist documentation))
	(set! slots (cons `(,s ,@(choice->list v)) slots))))
    slots))

(define (declare-obsolete! frame)
  (frame-add! frame 'type 'obsolete)
  (frame-set! frame 'obj-name (cons 'obsolete (get frame 'obj-name))))

(define (obsolete? frame)
  (and (not (contains? 'win32 (get frame 'keywords)))
       (or (not (exists? (get frame 'type)))
	   (contains? (get frame 'type) '{primitive variable}))
       (not (exists (sym (get frame 'symbol)) (symbol-bound? sym)))))

(define (mark-all-obsolete)
  (do-pool (f docdb)
    (when (obsolete? f) (declare-obsolete! f))))

(define (process-doc-entry doc)
  (lineout doc)
  (if (null? (cdr doc))
      (zap-oid! (find-frames docindex 'symbol (car doc)))
      (let ((f (find-frames docindex
		 '{symbol aliases} (choice (car doc) (cadr doc)))))
	(%set! f 'symbol (car doc))
	(%set! f 'aliases (cadr doc))
	(%set! f 'keywords (caddr doc))
	(do ((sv (cdddr doc) (cddr sv)))
	    ((null? sv))
	  (%set! f (car sv) (cadr sv))))))


(define (merge-keywords primary . secondary)
  (let ((docs (find-frames docindex
		'keywords (choice primary (elts secondary)))))
    (add! docs 'keywords primary)
    (drop! docs 'keywords (elts secondary))))

(define (load-all)
  (prefetch (pool-elts docdb)))

(define (get-needy)
  (filter-choices (doc (pool-elts docdb))
    (test doc 'documentation "")
    (not (test doc 'type 'obsolete))))
(define (get-needy-bound)
  (filter-choices (doc (pool-elts docdb))
    (test doc 'documentation "")
    (not (test doc 'type 'obsolete))
    (exists? (get doc 'symbol))
    (symbol? (get doc 'symbol))
    (symbol-bound? (get doc 'symbol))))

(define (fix-type doc)
  (when (and (test doc 'type 'unbound)
	     (exists?  (get doc 'symbol)) (symbol? (get doc 'symbol))
	     (symbol-bound? (get doc 'symbol))
	     (not (test doc 'type 'obsolete)))
    (lineout "Fixing up " doc)
    (%set! doc 'type (get-value-type (eval (get doc 'symbol))))
    (%set! doc 'obj-name (list (get-value-type (eval (get doc 'symbol)))
			       (get doc 'symbol)))
    (lineout "Fixed up " doc)))

(define prim-pattern
  (choice
   #("fd_add_cproc" {"" (spaces)} "(" (csymbol) ","
     "\"" (label name (lsymbol)) "\","
     (isdigit+) "," (label cfcn (csymbol)))
   #("fd_add_lexpr" {"" (spaces)} "(" (csymbol) ","
     "\"" (label name (lsymbol)) "\","
     (csymbol) "," (label cfcn (csymbol)))
   #("fd_add_special_form" {"" (spaces)} "(" (csymbol) ","
     "\"" (label name (lsymbol)) "\","
     (label cfcn (csymbol)))
   #("fd_add_alias" {"" (spaces)}
     "(" (csymbol) ",\"" (label name (lsymbol)) "\"")
   #("fd_add_restricted_cproc" {"" (spaces)} "(" 
     "\"" (label name (lsymbol)) "\","
     (isdigit+) "," (label cfcn (csymbol)))
   #("fd_add_restricted_lexpr" {"" (spaces)} "(" 
     "\"" (label name (lsymbol)) "\","
     (csymbol) "," (label cfcn (csymbol)))
   #("fd_add_restricted_special_form" {"" (spaces)} "(" 
     "\"" (label name (lsymbol)) "\","
     (label cfcn (csymbol)))))

(define (add-keyword! doc keyword)
  (unless (test doc 'keywords keyword) (add! doc 'keywords keyword)))

(define (get-prims c-file)
  (let* ((content (filestring c-file))
	 (patterns (tx-gather prim-pattern content)))
    (for-choices (frag patterns)
      (textlet prim-pattern frag
	name))))

(define (get-implementation-info c-file)
  (let* ((content (filestring c-file))
	 (patterns (tx-gather prim-pattern content)))
    (for-choices (frag patterns)
      (textlet prim-pattern frag
	(when (bound? cfcn)
	  (let ((frame (find-frames docindex 'symbol (string->symbol name))))
	    (lineout frame " is implemented by " cfcn " in " c-file)
	    (add! frame 'implemented-in c-file)
	    (add! frame 'implemented-by cfcn)))))))