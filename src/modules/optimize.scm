;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

;;; Optimizing code structures for the interpreter, including
;;;  use of constant OPCODEs and relative lexical references
(in-module 'optimize)

(define-init standard-modules
  (choice (get (get-module 'reflection) 'getmodules)
	  'scheme 'xscheme 'fileio 'filedb 'history 'logger))
(define-init check-module-usage #f)

;; This module optimizes an expression or procedure by replacing
;; certain variable references with their values directly, which
;; avoids many environment lookups.  The trick is to not replace
;; anything which will change and so produce an equivalent expression
;; or function which just runs faster.

(use-module 'reflection)
(use-module 'varconfig)
(use-module 'logger)

(define-init %loglevel %warning%)
(define-init useopcodes #t)
(define-init optdowarn #t)
(define-init lexrefs-dflt #t)
(define-init rails-dflt #f)
(varconfig! optimize:lexrefs lexrefs-dflt)
(varconfig! optimize:rails rails-dflt)

(defslambda (codewarning warning)
  (threadset! 'codewarnings (choice warning (threadget 'codewarnings))))

(define (module? arg)
  (and (table? arg) (not (environment? arg))))

(define opcodeopt-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) useopcodes)
	  (val (set! useopcodes #t))
	  (else (set! useopcodes #f)))))
(config-def! 'opcodeopt opcodeopt-config)

(define optdowarn-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) optdowarn)
	  (val (set! optdowarn #t))
	  (else (set! optdowarn #f)))))
(config-def! 'optdowarn optdowarn-config)

(module-export! '{optimize! optimize-procedure! optimize-module!})

(define %volatile '{optdowarn useopcodes %loglevel})

(varconfig! optimize:checkusage check-module-usage)

;;; Utility functions

(define-init special-form-tighteners (make-hashtable))

(define (arglist->vars arglist)
  (if (pair? arglist)
      (cons (if (pair? (car arglist)) (car (car arglist)) (car arglist))
	    (arglist->vars (cdr arglist)))
      (if (null? arglist) '() (list arglist))))

(define (get-lexref sym bindlist base)
  (if (null? bindlist) #f
      (if (pair? (car bindlist))
	  (let ((pos (position sym (car bindlist))))
	    (if pos (%lexref base pos)
		(get-lexref sym (cdr bindlist) (1+ base))))
	  (if (null? (car bindlist))
	      (get-lexref sym (cdr bindlist) (1+ base))
	      (if (eq? (car bindlist) sym) sym
		  (get-lexref sym (cdr bindlist) base))))))

(define (isbound? var bindlist)
  (if (null? bindlist) #f
      (if (pair? (car bindlist))
	  (position var (car bindlist))
	  (if (null? (car bindlist))
	      (isbound? var (cdr bindlist))
	      (or (eq? (car bindlist) var)
		  (isbound? var (cdr bindlist)))))))

;;; Opcode mapping

;;; When opcodes are introduced, the idea is that a given primitive 
;;;  may map into an opcode to be zippily executed in the evaluator.
;;; The opcode-map is a table for identifying the opcode for a given procedure.
;;;  It's keys are either primitive (presumably) procedures or pairs
;;;   of procedures and integers (number of subexpressions).  This allows
;;;   opcode substitution to be limited to simpler forms and allows opcode
;;;   execution to make assumptions about the number of arguments.  Note that
;;;   this means that this file needs to be synchronized with src/scheme/eval.c

(define opcode-map (make-hashtable))

(define (map-opcode value length)
  (if useopcodes
      (try (get opcode-map (cons value length))
	   (get opcode-map value)
	   value)
      value))

(define (def-opcode prim code (n-args #f))
  (if n-args
      (add! opcode-map (cons prim n-args) (make-opcode code))
      (add! opcode-map prim (make-opcode code))))

(when (bound? make-opcode)
  (def-opcode QUOTE      0x00)
  (def-opcode BEGIN      0x01)
  (def-opcode AND        0x02)
  (def-opcode OR         0x03)
  (def-opcode NOT        0x04)
  (def-opcode FAIL       0x05)
  (def-opcode %MODREF    0x06)
  (def-opcode COMMENT    0x07)

  (def-opcode IF         0x10)
  (def-opcode WHEN       0x11)
  (def-opcode UNLESS     0x12)
  (def-opcode IFELSE     0x13)

  (def-opcode AMBIGUOUS? 0x20 1)
  (def-opcode SINGLETON? 0x21 1)
  (def-opcode FAIL?      0x22 1)
  (def-opcode EMPTY?     0x22 1)
  (def-opcode EXISTS?    0x23 1)
  (def-opcode SINGLETON  0x24 1)
  (def-opcode CAR        0x25 1)
  (def-opcode CDR        0x26 1)
  (def-opcode LENGTH     0x27 1)
  (def-opcode QCHOICE    0x28 1)
  (def-opcode CHOICE-SIZE 0x29 1)
  (def-opcode PICKOIDS    0x2A 1)
  (def-opcode PICKSTRINGS 0x2B 1)
  (def-opcode PICK-ONE    0x2C 1)
  (def-opcode IFEXISTS    0x2D 1)

  (def-opcode 1-         0x40 1)
  (def-opcode -1+        0x40 1)
  (def-opcode 1+         0x41 1)
  (def-opcode NUMBER?    0x42 1)
  (def-opcode ZERO?      0x43 1)
  (def-opcode VECTOR?    0x44 1)
  (def-opcode PAIR?      0x45 1)
  (def-opcode NULL?      0x46 1)
  (def-opcode STRING?    0x47 1)
  (def-opcode OID?       0x48 1)
  (def-opcode SYMBOL?    0x49 1)
  (def-opcode FIRST      0x4A 1)
  (def-opcode SECOND     0x4B 1)
  (def-opcode THIRD      0x4C 1)
  (def-opcode ->NUMBER   0x4D 1)

  (def-opcode =          0x60 2)
  (def-opcode >          0x61 2)
  (def-opcode >=         0x62 2)
  (def-opcode <          0x63 2)
  (def-opcode <=         0x64 2)
  (def-opcode +          0x65 2)
  (def-opcode -          0x66 2)
  (def-opcode *          0x67 2)
  (def-opcode /~         0x68 2)

  (def-opcode EQ?        0x80 2)
  (def-opcode EQV?       0x81 2)
  (def-opcode EQUAL?     0x82 2)
  (def-opcode ELT        0x83 2)

  (def-opcode GET        0xA0 2)
  (def-opcode TEST       0xA1 3)
  ;; (def-opcode XREF       0xA2 3)
  (def-opcode %GET       0xA3 2)
  (def-opcode %TEST      0xA4 3)

  (def-opcode IDENTICAL?   0xC0)
  (def-opcode OVERLAPS?    0xC1)
  (def-opcode CONTAINS?    0xC2)
  (def-opcode UNION        0xC3)
  (def-opcode INTERSECTION 0xC4)
  (def-opcode DIFFERENCE   0xC5)
  )

;;; The core loop

(defambda (dotighten expr env bound lexrefs w/rails)
  (logdebug "Optimizing " expr " given " bound)
  (cond ((ambiguous? expr)
	 (for-choices (each expr) (dotighten each env bound lexrefs w/rails)))
	((fail? expr) expr)
	((symbol? expr)
	 (let ((lexref (get-lexref expr bound 0)))
	   (debug%watch "DOTIGHTEN/SYMBOL" expr lexref env bound)
	   (if lexref (if lexrefs lexref expr)
	       (let ((module (wherefrom expr env)))
		 (debug%watch "DOTIGHTEN/SYMBOL/module" expr module env bound)
		 (when (and module (module? env))
		   (add! env '%free_vars expr)
		   (when (and module (table? module))
		     (add! env '%used_modules
			   (pick (get module '%moduleid) symbol?))))
		 (if module
		     (cond ((%test module '%nosubst expr) expr)
			   ((%test module '%constants expr)
			    (let ((v (%get module expr)))
			      (if (or (pair? v) (symbol? v) (ambiguous? v))
				  (list 'quote (qc v))
				  v)))
			   (w/rails (->rail `(,%modref ,module ,expr)))
			   (else `(,%modref ,module ,expr)))
		     (begin
		       (when optdowarn
			 (codewarning (cons* 'UNBOUND expr bound))
			 (when env
			   (add! env '%warnings (cons* 'UNBOUND expr bound)))
			 (warning "The symbol " expr
				  " appears to be unbound given bindings "
				  (apply append bound)))
		       expr))))))
	((not (pair? expr)) expr)
	;; This will break when the ambiguous head includes
	;;  a special form.  Tricky to code cleanly.
	;;  (and probably should be an error)
	((or (pair? (car expr)) (ambiguous? (car expr)))
	 (tighten-call expr env bound lexrefs w/rails))
	;; Don't optimize calls to local variable references for the
	;;  same reason
	((and (symbol? (car expr)) (get-lexref (car expr) bound 0))
	 expr)
	((and (symbol? (car expr)) (not (symbol-bound? (car expr) env)))
	 (when optdowarn
	   (codewarning (cons* 'UNBOUND expr bound))
	   (warning "The symbol " (car expr) " in " expr
		    " appears to be unbound given bindings "
		    (apply append bound)))
	 expr)
	(else (let* ((head (get-arg expr 0))
		     (n-exprs (-1+ (length expr)))
		     (value (if (symbol? head)
				(or (get-lexref head bound 0) (get env head))
				head))
		     (from (and (symbol? head)
				(not (get-lexref head bound 0))
				(wherefrom head env))))
		(when (and from (module? env))
		  (add! env '%free_vars expr)
		  (when (and from (table? from))
		    (add! env '%used_modules
			  (pick (get from '%moduleid) symbol?))))
		(cond ((if from 
			   (test from '{%unoptimized %volatile %nosubst}
				 head)
			   (and env 
				(test env '{%unoptimized %volatile %nosubst}
				      head)))
		       expr)
		      ((and from (%test from '%rewrite)
			    (%test (get from '%rewrite) head))
		       (dotighten
			((%get (get from '%rewrite) head) expr)
			env bound lexrefs w/rails))
		      ((or (applicable? value) (opcode? value)
			   (and (ambiguous? value)
				(singleton? (applicable? value))
				(applicable? value)))
		       (when (and optdowarn (procedure? value))
			 (when (and (procedure-min-arity value)
				    (< n-exprs (procedure-min-arity value)))
			   (codewarning (list 'TOOFEWARGS expr value))
			   (warning "The call to " expr
				    " provides too few arguments "
				    "(" n-exprs ") for " value))
			 (when (and (procedure-arity value)
				    (> n-exprs (procedure-arity value)))
			   (codewarning (list 'TOOMANYARGS expr value))
			   (warning "The call to " expr " provides too many "
				    "arguments (" n-exprs ") for " value)))
		       (callcons (qc (map-opcode
				      (cond ((not from) value)
					    ((test from '%nosubst head) head)
					    ((test from '%volatile head)
					     (if w/rails
						 (->rail `(,%modref ,from ,head))
						 `(,%modref ,from ,head)))
					    (else value))
				      n-exprs))
				 (tighten-args (cdr expr) env bound lexrefs w/rails)
				 w/rails))
		      ((and (ambiguous? value)
			    (exists applicable? value)
			    (not (singleton? (applicable? value))))
		       ;; Theoretically, we could possibly apply the
		       ;; special form optimizers, but it's not worth
		       ;; it.
		       (warning
			"Inconsistent ND call (mixed special/applicable)")
		       (if w/rails
			   (->rail (cons (qc value) (cdr (->list expr))))
			   (cons (qc value) (cdr (->list expr)))))
		      ((ambiguous? value)
		       ;; This should optimize the arguments in most
		       ;; cases.  It doesn't right now.
		       (if w/rails
			   (->rail (cons (qc value) (cdr (->list expr))))
			   (cons (qc value) (cdr (->list expr)))))
		      ((special-form? value)
		       (let ((tightener
			      (try (get special-form-tighteners value)
				   (get special-form-tighteners
					(procedure-name value)))))
			 (if (exists? tightener)
			     (tightener value expr env bound lexrefs w/rails)
			     expr)))
		      ((macro? value)
		       (dotighten (macroexpand value expr) env
				  bound lexrefs w/rails))
		      (else
		       (when (and optdowarn (not (test from '%nosubst head)))
			 (codewarning (cons* 'NOTFCN expr value))
			 (warning "The value of " head  " for "
				  expr ", " value ","
				  " doesn't appear to be a applicable given "
				  (apply append bound)))
		       expr))))))

(define (callcons head tail w/rails)
  (if w/rails
      (->rail (cons head (->list tail)))
      (cons head tail)))

(define (ident x) x)
(define (->rail x) (apply make-rail x))

(defambda (tighten-call expr env bound lexrefs w/rails)
  (if (pair? expr)
      ((if w/rails ->rail ident)
       (if (or (symbol? (car expr)) (pair? (car expr))
	       (ambiguous? (car expr)))
	   `(,(dotighten (car expr) env bound lexrefs w/rails)
	     . ,(if (pair? (cdr expr))
		    (tighten-args (cdr expr) env bound lexrefs w/rails)
		    (cdr expr)))
	   (tighten-args expr env bound lexrefs w/rails)))
      expr))
(defambda (tighten-args expr env bound lexrefs w/rails)
  (if (pair? expr)
      (forseq (arg expr)
	(if (qchoice? arg) arg
	    (dotighten arg env bound lexrefs w/rails)))
      expr))

(define (optimize-procedure! proc (lexrefs lexrefs-dflt) (w/rails rails-dflt))
  (let* ((env (procedure-env proc))
	 (arglist (procedure-args proc))
	 (body (procedure-body proc))
	 (initial (and (pair? body) (car body)))
	 (bound (list (arglist->vars arglist))))
    (unless (and initial (pair? initial)
		 (eq? (car initial) 'COMMENT)
		 (pair? (cdr initial))
		 (eq? (cadr initial) '|original|))
      (threadset! 'codewarnings #{})
      (set-procedure-body!
       proc `((comment |original| ,@body)
	      (comment |originalargs| ,arglist)
	      ,@(map (lambda (b)
		       (dotighten b env bound lexrefs w/rails))
		     body)))
      (when (pair? arglist)
	(let ((optimized-args (optimize-arglist arglist env lexrefs w/rails)))
	  (unless (equal? arglist optimized-args)
	    (set-procedure-args! proc optimized-args))))
      (when (exists? (threadget 'codewarnings))
	(warning "Errors optimizing " proc ": "
		 (do-choices (warning (threadget 'codewarnings))
		   (printout "\n\t" warning)))
	(threadset! 'codewarnings #{})))))

(define (optimize-arglist arglist env lexrefs w/rails)
  (if (pair? arglist)
      (cons
       (if (and (pair? (car arglist)) (pair? (cdr (car arglist)))
		(singleton? (cadr (car arglist))))
	   `(,(caar arglist) 
	     ,(dotighten (cadr (car arglist)) env '() lexrefs w/rails))
	   (car arglist))
       (optimize-arglist (cdr arglist) env lexrefs w/rails))
      arglist))
  
(define (optimize-get-module spec)
  (onerror (get-module spec)
    (lambda (ex) (irritant+ spec |GetModuleFailed| optimize-module
			    "Couldn't load module " spec))))

(define (optimize-module! module (lexrefs lexrefs-dflt) (w/rails rails-dflt))
  (loginfo "Optimizing module " module)
  (when (symbol? module)
    (set! module (optimize-get-module module)))
  (let ((bindings (module-bindings module))
	(count 0))
    (do-choices (var bindings)
      (loginfo "Optimizing module binding " var)
      (let ((value (get module var)))
	(when (and (exists? value) (compound-procedure? value))
	  (set! count (1+ count))
	  (optimize-procedure! value lexrefs w/rails))))
    (when (exists symbol? (get module '%moduleid))
      (let* ((referenced-modules (get module '%used_modules))
	     (used-modules
	      (eval `(within-module ',(pick (get module '%moduleid) symbol?)
				    (,getmodules))))
	     (unused (difference used-modules referenced-modules standard-modules
				 (get module '%moduleid))))
	(when (and check-module-usage (exists? unused))
	  (logwarn "Module " (try (pick (get module '%moduleid) symbol?)
				  (get module '%moduleid))
		   " declares " (choice-size unused) " possibly unused modules: "
		   (do-choices (um unused i) (printout (if (> i 0) ", ") um))))))
    count))

(define (optimize-bindings! bindings (lexrefs lexrefs-dflt) (w/rails rails-dflt))
  (logdebug "Optimizing bindings " bindings)
  (let ((count 0))
    (do-choices (var (getkeys bindings))
      (logdebug "Optimizing binding " var)
      (let ((value (get bindings var)))
	(if (bound? value)
	    (when (compound-procedure? value)
	      (set! count (1+ count)) (optimize-procedure! value lexrefs w/rails))
	    (warning var " is unbound"))))
    count))

(defambda (module-arg? arg)
  (or (fail? arg) (string? arg)
      (and (pair? arg) (eq? (car arg) 'quote))
      (and (ambiguous? arg) (fail? (reject arg module-arg?)))))

(define (optimize*! . args)
  (dolist (arg args)
    (cond ((compound-procedure? arg) (optimize-procedure! arg))
	  ((table? arg) (optimize-module! arg))
	  (else (error '|TypeError| 'optimize*
			 "Invalid optimize argument: " arg)))))

(define optimize!
  (macro expr
    (if (null? (cdr expr))
	`(,optimize-bindings! (,%bindings))
	(cons optimize*!
	      (map (lambda (x)
		     (if (module-arg? x)
			 `(,optimize-get-module ,x)
			 x))
		   (cdr expr))))))


;;;; Special form handlers

(define (tighten-block handler expr env bound lexrefs w/rails)
  (cons (map-opcode handler (length (cdr expr)))
	(map (lambda (x) (dotighten x env bound lexrefs w/rails))
	     (cdr expr))))
(define (tighten-block->rail handler expr env bound lexrefs w/rails)
  (if w/rails
      (->rail (tighten-block handler expr env bound lexrefs w/rails))
      (tighten-block handler expr env bound lexrefs w/rails)))

(define (tighten-let handler expr env bound lexrefs w/rails)
  (let ((bindexprs (cadr expr)) (body (cddr expr)))
    `(,handler ,(map (lambda (x)
		       `(,(car x) ,(dotighten (cadr x) env bound lexrefs w/rails)))
		     bindexprs)
	       ,@(let ((bound (cons (map car bindexprs) bound)))
		   (map (lambda (b) (dotighten b env bound lexrefs w/rails))
			body)))))
(define (tighten-doexpression handler expr env bound lexrefs w/rails)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler (,(car bindspec)
		,(dotighten (cadr bindspec) env bound lexrefs w/rails)
		,@(cddr bindspec))
	       ,@(let ((bound
			(if (= (length bindspec) 3)
			    (cons (list (first bindspec) (third bindspec))
				  bound)
			    (cons (list (first bindspec)) bound))))
		   (map (lambda (b) (dotighten b env bound lexrefs w/rails))
			body)))))
(define (tighten-do2expression handler expr env bound lexrefs w/rails)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler ,(cond ((pair? bindspec)
		       `(,(car bindspec)
			 ,(dotighten (cadr bindspec) env bound lexrefs w/rails)
			 ,@(cddr bindspec)))
		      ((symbol? bindspec)
		       `(,bindspec
			 ,(dotighten bindspec env bound lexrefs w/rails)))
		      (else (error 'syntax "Bad do-* expression")))
	       ,@(let ((bound
			(if (symbol? bindspec)
			    (cons (list bindspec) bound)
			    (if (= (length bindspec) 3)
				(cons (list (first bindspec)
					    (third bindspec)) bound)
				(cons (list (first bindspec)) bound)))))
		   (map (lambda (b) (dotighten b env bound lexrefs w/rails))
			body)))))

(define (tighten-dosubsets handler expr env bound lexrefs w/rails)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler (,(car bindspec)
		,(dotighten (cadr bindspec) env bound lexrefs w/rails)
		,@(cddr bindspec))
	       ,@(let ((bound (if (= (length bindspec) 4)
				  (cons (list (first bindspec) (fourth bindspec))
					bound)
				  (cons (list (first bindspec)) bound))))
		   (map (lambda (b) (dotighten b env bound lexrefs w/rails))
			body)))))

(define (tighten-let*-bindings bindings env bound lexrefs w/rails)
  (if (null? bindings) '()
      `((,(car (car bindings))
	 ,(dotighten (cadr (car bindings)) env bound lexrefs w/rails))
	,@(tighten-let*-bindings
	   (cdr bindings) env
	   (cons (append (car bound) (list (car (car bindings))))
		 (cdr bound))
	   lexrefs w/rails))))

(define (tighten-let* handler expr env bound lexrefs w/rails)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler
      ,(tighten-let*-bindings (cadr expr) env (cons '() bound) lexrefs w/rails)
      ,@(let ((bound (cons (map car bindspec) bound)))
	  (map (lambda (b) (dotighten b env bound lexrefs w/rails))
	       body)))))
(define (tighten-set-form handler expr env bound lexrefs w/rails)
  `(,handler ,(cadr expr) ,(dotighten (third expr) env bound lexrefs w/rails)))

(define (tighten-lambda handler expr env bound lexrefs w/rails)
  `(,handler ,(cadr expr)
	     ,@(let ((bound (cons (arglist->vars (cadr expr)) bound)))
		 (map (lambda (b) (dotighten b env bound lexrefs w/rails))
		      (cddr expr)))))

(define (tighten-cond handler expr env bound lexrefs w/rails)
  (cons handler (map (lambda (clause)
		       (cond ((eq? (car clause) 'else)
			      `(ELSE
				,@(map (lambda (x)
					 (dotighten x env bound lexrefs w/rails))
				       (cdr clause))))
			     ((and (pair? (cdr clause)) (eq? (cadr clause) '=>))
			      `(,(dotighten (car clause) env bound lexrefs w/rails)
				=>
				,@(map (lambda (x)
					 (dotighten x env bound lexrefs w/rails))
				       (cddr clause))))
			     (else (map (lambda (x)
					  (dotighten x env bound lexrefs w/rails))
					clause))))
		     (cdr expr))))

(define (tighten-case handler expr env bound lexrefs w/rails)
  `(,handler ,(dotighten (cadr expr) env bound lexrefs w/rails)
	     ,@(map (lambda (clause)
		      (cons (car clause)
			    (map (lambda (x)
				   (dotighten x env bound lexrefs w/rails))
				 (cdr clause))))
		    (cddr expr))))

(define (tighten-unwind-protect handler expr env bound lexrefs w/rails)
  `(,handler ,(dotighten (cadr expr) env bound lexrefs w/rails)
	     ,@(map (lambda (uwclause)
		      (dotighten uwclause env bound lexrefs w/rails))
		    (cddr expr))))

(define (tighten-quasiquote handler expr env bound lexrefs w/rails)
  `(,handler ,(tighten-quasiquote-node (cadr expr) env bound lexrefs w/rails)))
(defambda (tighten-quasiquote-node expr env bound lexrefs w/rails)
  (cond ((ambiguous? expr)
	 (for-choices (elt expr)
	   (tighten-quasiquote-node elt env bound lexrefs w/rails)))
	((and (pair? expr) 
	      (or (eq? (car expr) 'unquote)  (eq? (car expr) 'unquote*)))
	 `(,(car expr) ,(dotighten (cadr expr) env bound lexrefs w/rails)))
	((pair? expr)
	 (forseq (elt expr)
	   (tighten-quasiquote-node elt env bound lexrefs w/rails)))
	((vector? expr)
	 (forseq (elt expr)
	   (tighten-quasiquote-node elt env bound lexrefs w/rails)))
	((slotmap? expr)
	 (let ((copy (frame-create #f))
	       (slots (getkeys expr)))
	   (do-choices (slot slots)
	     (store! copy (tighten-quasiquote-node slot env bound lexrefs w/rails)
		     (try (tighten-quasiquote-node (get expr slot) env bound lexrefs w/rails)
			  (get expr slot))))
	   copy))
	(else expr)))

(define (tighten-logmsg handler expr env bound lexrefs w/rails)
  (if (or (symbol? (cadr expr)) (number? (cadr expr)))
      (if (or (symbol? (caddr expr)) (number? (caddr expr)))
	  `(,handler ,(cadr expr)
		     ,(caddr expr)
		     ,@(map (lambda (elt)
			      (dotighten elt env bound lexrefs w/rails))
			    (cdddr expr)))
	  `(,handler ,(cadr expr)
		     ,@(map (lambda (elt)
			      (dotighten elt env bound lexrefs w/rails))
			    (cddr expr))))
      `(,handler ,@(map (lambda (elt)
			  (dotighten elt env bound lexrefs w/rails))
			(cdr expr)))))

(define (tighten-logif handler expr env bound lexrefs w/rails)
  (if (or (symbol? (caddr expr))  (number? (caddr expr)))
      `(,handler ,(dotighten (cadr expr) env bound lexrefs w/rails)
		 ,(caddr expr)
		 ,@(map (lambda (elt)
			  (dotighten elt env bound lexrefs w/rails))
			(cdddr expr)))
      `(,handler ,(dotighten (cadr expr) env bound lexrefs w/rails)
		 ,@(map (lambda (elt)
			  (dotighten elt env bound lexrefs w/rails))
			(cddr expr)))))

(define (tighten-logif+ handler expr env bound lexrefs w/rails)
  (if (or (symbol? (caddr expr))  (number? (caddr expr)))
      (if (or (symbol? (cadr (cddr expr))) (number? (cadr (cddr expr))))
	  `(,handler ,(dotighten (cadr expr) env bound lexrefs w/rails)
		     ,(caddr expr)
		     ,(cadr (cddr expr))
		     ,@(map (lambda (elt)
			      (dotighten elt env bound lexrefs w/rails))
			    (cdr (cdddr expr))))
	  `(,handler ,(dotighten (cadr expr) env bound lexrefs w/rails)
		     ,(caddr expr)
		     ,@(map (lambda (elt)
			      (dotighten elt env bound lexrefs w/rails))
			    (cdddr expr))))
      `(,handler ,(dotighten (cadr expr) env bound lexrefs w/rails)
		 ,@(map (lambda (elt)
			  (dotighten elt env bound lexrefs w/rails))
			(cddr expr)))))

;;; Tightening XHTML expressions

;; This doesn't handle mixed alist ((x y)) and plist (x y) attribute lists
;;  because they shouldn't work anyway
(define (tighten-attribs attribs env bound lexrefs w/rails)
  (cond ((not (pair? attribs)) attribs)
	((pair? (cdr attribs))
	 `(,(if (and (pair? (car attribs))
		     (not (eq? (car (car attribs)) 'quote)))
		`(,(car (car attribs))
		  ,(dotighten (cadr (car attribs)) env bound lexrefs w/rails))
		(car attribs))
	   ,(if (and (pair? (car attribs))
		     (not (eq? (car (car attribs)) 'quote)))
		(if (pair? (cadr attribs))
		    `(,(car (cadr attribs))
		      ,(dotighten (cadr (cadr attribs)) env bound lexrefs w/rails))
		    (dotighten (cadr attribs) env bound lexrefs w/rails))
		(dotighten (cadr attribs) env bound lexrefs w/rails))
	   ,@(tighten-attribs (cddr attribs) env bound lexrefs w/rails)))
	((pair? (car attribs))
	 `((,(car (car attribs))
	    ,(dotighten (cadr (car attribs)) env bound lexrefs w/rails))))
	(else attribs)))

(define (tighten-markup handler expr env bound lexrefs w/rails)
  `(,(car expr)
    ,@(map (lambda (x) (dotighten x env bound lexrefs w/rails))
	   (cdr expr))))
(define (tighten-markup* handler expr env bound lexrefs w/rails)
  `(,(car expr) ,(tighten-attribs (second expr) env bound lexrefs w/rails)
    ,@(map (lambda (x) (dotighten x env bound lexrefs w/rails))
	   (cddr expr))))

(define (tighten-emptymarkup fcn expr env bound lexrefs w/rails)
  `(,(car expr) ,@(tighten-attribs (cdr expr) env bound lexrefs w/rails)))

(define (tighten-anchor* fcn expr env bound lexrefs w/rails)
  `(,(car expr) ,(dotighten (cadr expr) env bound lexrefs w/rails)
    ,(tighten-attribs (third expr) env bound lexrefs w/rails)
    ,@(map (lambda (elt) (dotighten elt env bound lexrefs w/rails))
	   (cdr (cddr expr)))))

(define (tighten-xmlblock fcn expr env bound lexrefs w/rails)
  `(,(car expr) ,(cadr expr)
    ,(tighten-attribs (third expr) env bound lexrefs w/rails)
    ,@(map (lambda (elt) (dotighten elt env bound lexrefs w/rails))
	   (cdr (cddr expr)))))

;;; Declare them

(add! special-form-tighteners let tighten-let)
(add! special-form-tighteners let* tighten-let*)
(when (bound? letq)
  (add! special-form-tighteners letq tighten-let)
  (add! special-form-tighteners letq* tighten-let*))
(add! special-form-tighteners (choice lambda ambda slambda)
      tighten-lambda)
(add! special-form-tighteners (choice set! set+! default! define)
      tighten-set-form)

(add! special-form-tighteners
      (choice dolist dotimes doseq forseq)
      tighten-doexpression)
(add! special-form-tighteners
      (choice do-choices for-choices filter-choices try-choices)
      tighten-do2expression)

(add! special-form-tighteners
      (choice begin prog1 try tryif when if unless and or until while)
      tighten-block->rail)
(when (bound? ipeval)
  (add! special-form-tighteners
	(choice ipeval tipeval)
	tighten-block))
(add! special-form-tighteners case tighten-case)
(add! special-form-tighteners cond tighten-cond)
(add! special-form-tighteners do-subsets tighten-dosubsets)
(when (bound? parallel)
  (add! special-form-tighteners
	(choice parallel spawn)
	tighten-block))

(add! special-form-tighteners
      (choice printout lineout stringout message notify)
      tighten-block)

(add! special-form-tighteners unwind-protect tighten-unwind-protect)

(add! special-form-tighteners
      {"ONERROR" "UNWIND-PROTECT" "DYNAMIC-WIND"}
      tighten-block)
(add! special-form-tighteners {"FILEOUT" "SYSTEM"} tighten-block)

(add! special-form-tighteners 
      ({procedure-name (lambda (x) x) 
	(lambda (x) (string->symbol (procedure-name x)))}
       quasiquote)
      tighten-quasiquote)

(add! special-form-tighteners logmsg tighten-logmsg)
(add! special-form-tighteners logif tighten-logif)
(add! special-form-tighteners logif+ tighten-logif+)

;; Don't optimize these because they look at the symbol that is the head
;; of the expression to get their tag name.
(add! special-form-tighteners {"markupblock" "ANCHOR"} tighten-markup)
(add! special-form-tighteners {"markup*block" "markup*"} tighten-markup*)
(add! special-form-tighteners "emptymarkup" tighten-emptymarkup)
(add! special-form-tighteners "ANCHOR*" tighten-anchor*)
(add! special-form-tighteners "XMLBLOCK" tighten-xmlblock)
(add! special-form-tighteners "WITH/REQUEST" tighten-block)
(add! special-form-tighteners "WITH/REQUEST/OUT" tighten-block)
(add! special-form-tighteners "XMLOUT" tighten-block)
(add! special-form-tighteners "XHTML" tighten-block)
(add! special-form-tighteners "XMLEVAL" tighten-block)
(add! special-form-tighteners "GETOPT" tighten-block)
(add! special-form-tighteners "TESTOPT" tighten-block)

(when (bound? fileout)
  (add! special-form-tighteners
	(choice fileout system)
	tighten-block))
