;;; -*- Mode: Scheme; -*-

(in-module 'kno/help)

(use-module '{logger varconfig text/stringfmts knodb reflection})

(module-export! '{findsyms apropos.command})

(defambda (valsum val)
  (cond ((empty? val) "empty")
	((constant? val) val)
	((number? val) val)
	((ambiguous? val) (choicesum val))
	((string? val) (glom "a " (length string) " character string"))
	((pool? val) (stringout "pool@" (dbctl val 'source)))
	((index? val) (stringout "index@" (dbctl val 'source)))
	((sequence? val) (glom "a " (typeof val) " of " (length val) " elements"))
	(else (glom "a " (typeof val)))))

(defambda (choicesum val)
  (let ((size (|| val)))
    (cond ((<= size 7) (for-choices (v val) (valsum v)))
	  ((<= size 1000)
	   (let ((types (for-choices (v val) (typeof v))))
	     (cond ((singleton? types)
		    (stringout size " choices of type " types))
		   ((<= (|| types) 7)
		    (stringout size " choices of types "
		      (do-choices (type types) (printout " " type))))
		   (else (stringout size " choices of " (|| types) " types")))))
	  (else (stringout (|| val) "choices")))))

(define (get-symbol-info modpair symbol)
  (let* ((modname (car modpair))
	 (module (cdr modpair))
	 (bindings (if (hashtable? module) module
		       (or (module-bindings module) module)))
	 (exports (module-exports module)))
    (tryif (and bindings (test bindings symbol))
      (let ((val (get bindings symbol)))
	(frame-create #f
	  'pname (symbol->string symbol)
	  'symbol symbol
	  'module module
	  'exported (tryif (and exports (test exports symbol)) symbol)
	  'procname (or (tryif (applicable? val) (procedure-name val)) {})
	  'dbid (tryif (or (pool? val) (index? val)) (dbctl val 'id))
	  'dbname (tryif (or (pool? val) (index? val)) (dbctl val 'source))
	  'args (tryif (applicable? val) (procedure-args val))
	  'doc (tryif (applicable? val) (procedure-documentation val))
	  'cname (tryif (and (applicable? val) (procedure-cname val))
		   (procedure-cname val))
	  'type (typeof val)
	  'valsum (valsum val))))))

(define (findsyms pattern)
  (let ((modules (all-modules))
	(symbols (getsyms pattern)))
    (get-symbol-info modules symbols)))

(define header-string "------------------------------------------------------------\n")

(define (pname-length item) (vector (length (get item 'pname)) (get item 'pname)))

(define (apropos.command pattern)
  (let ((info (findsyms pattern)))
    (doseq (item (sorted info pname-length))
      (lineout header-string
	(get item 'pname)
	(if (test item 'valsum) (printout " (" (get item 'valsum) ")"))
	(if (test item 'exported)
	    " exported from "
	    " in ")
	"the "
	(write (try (picksyms (get (get item 'module) '%moduleid))
		    (get (get item 'module) '%moduleid)))
	" module"
	(when (test item '{procname args})
	  (printout "\n    (" (try (get item 'procname) (get item 'symbol))
	    (if (test item 'cname) (printout " <" (get item 'cname) "> "))
	    (when (test item 'args)
	      (let ((args (get item 'args)))
		(if (sequence? args)
		    (doseq (arg args)
		      (cond ((symbol? arg) (printout " *" arg "*"))
			    ((and (pair? arg) (pair? (cdr arg)))
			     (printout " [*" (car arg) "*=" (cadr arg) "]"))
			    ((pair? arg)
			     (printout " [*" (car arg) "*]"))
			    (else (printout " " arg))))
		    (if (symbol? args)
			(printout " " args "...")
			(printout " " args)))))
	    ")"))
	(when (test item 'doc)
	  (printout "\n    " (trim-spaces (get item 'doc))))))))

