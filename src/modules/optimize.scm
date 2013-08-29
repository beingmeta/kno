;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

;;; Optimizing code structures for the interpreter, including
;;;  use of constant OPCODEs and relative lexical references
(in-module 'optimize)

(define-init standard-modules
  (choice (get (get-module 'reflection) 'getmodules)
	  'scheme 'xscheme 'fileio 'filedb 'history 'logger))

;; This module optimizes an expression or procedure by replacing
;; certain variable references with their values directly, which
;; avoids many environment lookups.  The trick is to not replace
;; anything which will change and so produce an equivalent expression
;; or function which just runs faster.

(use-module 'reflection)
(use-module 'logger)

(define %loglevel %warning%)
(define useopcodes #t)
(define optdowarn #t)

(defslambda (codewarning warning)
  (threadset! 'codewarnings (choice warning (threadget 'codewarnings))))

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

;;; Utility functions

(define special-form-tighteners (make-hashtable))

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

  ;; (def-opcode GET        0xA0 2)
  ;; (def-opcode TEST       0xA1 2)
  ;; (def-opcode TEST       0xA1 3)
  )

;;; The core loop

(defambda (dotighten expr env bound dolex dorail)
  (cond ((ambiguous? expr)
	 (for-choices (each expr) (dotighten each env bound dolex dorail)))
	((fail? expr) expr)
	((symbol? expr)
	 (let ((lexref (get-lexref expr bound 0)))
	   (if lexref (if dolex lexref expr)
	       (let ((module (wherefrom expr env)))
		 (when (and module (table? env))
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
			   (else `(,%get ,module ',expr)))
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
	 (tighten-call expr env bound dolex dorail))
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
	(else (let* ((head (car expr))
		     (n-exprs (length (cdr expr)))
		     (value (if (symbol? head)
				(or (get-lexref head bound 0) (get env head))
				head))
		     (from (and (symbol? head)
				(not (get-lexref head bound 0))
				(wherefrom head env))))
		(when (and from (table? env))
		  (add! env '%free_vars expr)
		  (when (and from (table? from))
		    (add! env '%used_modules
			  (pick (get from '%moduleid) symbol?))))
		(cond ((if from (test from '%unoptimized head)
			   (and env (test env '%unoptimized head)))
		       expr)
		      ((and from (%test from '%rewrite)
			    (%test (get from '%rewrite) head))
		       (dotighten
			((%get (get from '%rewrite) head) expr)
			env bound dolex dorail))
		      ((or (applicable? value) (opcode? value)
			   (and (ambiguous? value)
				(singleton? (applicable? value))
				(applicable? value)))
		       (when (and optdowarn (procedure? value))
			 (when (and (procedure-min-arity value)
				    (< n-exprs (procedure-min-arity value)))
			   (codewarning (list 'TOOFEWARGS expr value))
			   (warning "The call to " expr
				    " provides too few arguments for " value))
			 (when (and (procedure-arity value)
				    (> n-exprs (procedure-arity value)))
			   (codewarning (list 'TOOMANYARGS expr value))
			   (warning "The call to " expr
				    " provides too many arguments for " value)))
		       (callcons (qc (map-opcode
				      (cond ((not from) value)
					    ((test from '%nosubst head) head)
					    ((test from '%volatile head)
					     `(,%get ,from ',head))
					    (else value))
				      n-exprs))
				 (tighten-call (cdr expr) env bound dolex dorail)
				 dorail))
		      ((and (ambiguous? value)
			    (exists applicable? value)
			    (not (singleton? (applicable? value))))
		       ;; Theoretically, we could possibly apply the
		       ;; special form optimizers, but it's not worth
		       ;; it.
		       (warning
			"Inconsistent ND call (mixed special/applicable)")
		       (cons (qc value) (cdr expr)))
		      ((ambiguous? value)
		       ;; This should optimize the arguments in most
		       ;; cases.  It doesn't right now.
		       (cons (qc value) (cdr expr)))
		      ((special-form? value)
		       (let ((tightener
			      (try (get special-form-tighteners value)
				   (get special-form-tighteners
					(procedure-name value)))))
			 (if (exists? tightener)
			     (tightener value expr env bound dolex dorail)
			     expr)))
		      ((macro? value)
		       (dotighten (macroexpand value expr) env
				  bound dolex dorail))
		      (else
		       (when optdowarn
			 (codewarning (cons* 'NOTFCN expr value))
			 (warning "The value of " head  " for "
				  expr ", " value ","
				  " doesn't appear to be a applicable given "
				  (apply append bound)))
		       expr))))))

(define (callcons head tail dorail)
  (if dorail
      (->rail (cons head (->list tail)))
      (cons head tail)))

(define (ident x) x)
(define (->rail x) (apply make-rail x))

(defambda (tighten-call expr env bound dolex dorail)
  (if (pair? expr)
      ((if dorail ->rail ident)
       (if (or (symbol? (car expr)) (pair? (car expr))
	       (ambiguous? (car expr)))
	   `(,(dotighten (car expr) env bound dolex dorail)
	     . ,(if (pair? (cdr expr))
		    (tighten-args (cdr expr) env bound dolex dorail)
		    (cdr expr)))
	   (tighten-args expr env bound dolex dorail)))
      expr))
(defambda (tighten-args expr env bound dolex dorail)
  (if (pair? expr)
      (forseq (arg expr)
	(if (qchoice? arg) arg
	    (dotighten arg env bound dolex dorail)))
      expr))

(define (optimize-procedure! proc (dolex #t) (dorail #f))
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
	      ,@(map (lambda (b)
		       (dotighten b env bound dolex dorail))
		     body)))
      (when (exists? (threadget 'codewarnings))
	(warning "Errors optimizing " proc ": "
		 (do-choices (warning (threadget 'codewarnings))
		   (printout "\n\t" warning)))
	(threadset! 'codewarnings #{})))))

(define (optimize-module! module)
  (logdebug "Optimizing module " module)
  (let ((bindings (module-bindings module))
	(count 0))
    (do-choices (var bindings)
      (logdebug "Optimizing binding " var)
      (let ((value (get module var)))
	(when (and (exists? value) (compound-procedure? value))
	  (set! count (1+ count))
	  (optimize! value))))
    (when (exists symbol? (get module '%moduleid))
      (let* ((referenced-modules (get module '%used_modules))
	     (used-modules (eval `(within-module ',(pick (get module '%moduleid) symbol?)
						 (,getmodules))))
	     (unused (difference used-modules referenced-modules standard-modules
				 (get module '%moduleid))))
	(when (exists? unused)
	  (logwarn "Module " (try (pick (get module '%moduleid) symbol?)
				  (get module '%moduleid))
		   " declares " (choice-size unused) " possibly unused modules: "
		   (do-choices (um unused i) (printout (if (> i 0) ", ") um))))))
    count))

(define (optimize-bindings! bindings)
  (logdebug "Optimizing bindings " bindings)
  (let ((count 0))
    (do-choices (var (getkeys bindings))
      (logdebug "Optimizing binding " var)
      (let ((value (get bindings var)))
	(if (bound? value)
	    (when (compound-procedure? value)
	      (set! count (1+ count)) (optimize! value))
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
			 `(get-module ,x)
			 x))
		   (cdr expr))))))


;;;; Special form handlers

(define (tighten-block handler expr env bound dolex dorail)
  (cons (map-opcode handler (length (cdr expr)))
	(map (lambda (x) (dotighten x env bound dolex dorail))
	     (cdr expr))))

(define (tighten-let handler expr env bound dolex dorail)
  (let ((bindexprs (cadr expr)) (body (cddr expr)))
    `(,handler ,(map (lambda (x)
		       `(,(car x) ,(dotighten (cadr x) env bound dolex dorail)))
		     bindexprs)
	       ,@(let ((bound (cons (map car bindexprs) bound)))
		   (map (lambda (b) (dotighten b env bound dolex dorail))
			body)))))
(define (tighten-doexpression handler expr env bound dolex dorail)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler (,(car bindspec)
		,(dotighten (cadr bindspec) env bound dolex dorail)
		,@(cddr bindspec))
	       ,@(let ((bound
			(if (= (length bindspec) 3)
			    (cons (list (first bindspec) (third bindspec))
				  bound)
			    (cons (list (first bindspec)) bound))))
		   (map (lambda (b) (dotighten b env bound dolex dorail))
			body)))))
(define (tighten-do2expression handler expr env bound dolex dorail)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler ,(cond ((pair? bindspec)
		       `(,(car bindspec)
			 ,(dotighten (cadr bindspec) env bound dolex dorail)
			 ,@(cddr bindspec)))
		      ((symbol? bindspec)
		       `(,bindspec
			 ,(dotighten bindspec env bound dolex dorail)))
		      (else (error 'syntax "Bad do-* expression")))
	       ,@(let ((bound
			(if (symbol? bindspec)
			    (cons (list bindspec) bound)
			    (if (= (length bindspec) 3)
				(cons (list (first bindspec)
					    (third bindspec)) bound)
				(cons (list (first bindspec)) bound)))))
		   (map (lambda (b) (dotighten b env bound dolex dorail))
			body)))))

(define (tighten-dosubsets handler expr env bound dolex dorail)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler (,(car bindspec)
		,(dotighten (cadr bindspec) env bound dolex dorail)
		,@(cddr bindspec))
	       ,@(let ((bound (if (= (length bindspec) 4)
				  (cons (list (first bindspec) (fourth bindspec)) bound)
				  (cons (list (first bindspec)) bound))))
		   (map (lambda (b) (dotighten b env bound dolex dorail))
			body)))))

(define (tighten-let*-bindings bindings env bound dolex dorail)
  (if (null? bindings) '()
      `((,(car (car bindings))
	 ,(dotighten (cadr (car bindings)) env bound dolex dorail))
	,@(tighten-let*-bindings
	   (cdr bindings) env
	   (cons (append (car bound) (list (car (car bindings))))
		 (cdr bound))
	   dolex dorail))))

(define (tighten-let* handler expr env bound dolex dorail)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler
      ,(tighten-let*-bindings (cadr expr) env (cons '() bound) dolex dorail)
      ,@(let ((bound (cons (map car bindspec) bound)))
	  (map (lambda (b) (dotighten b env bound dolex dorail))
	       body)))))
(define (tighten-set-form handler expr env bound dolex dorail)
  `(,handler ,(cadr expr) ,(dotighten (third expr) env bound dolex dorail)))

(define (tighten-lambda handler expr env bound dolex dorail)
  `(,handler ,(cadr expr)
	     ,@(let ((bound (cons (arglist->vars (cadr expr)) bound)))
		 (map (lambda (b) (dotighten b env bound dolex dorail))
		      (cddr expr)))))

(define (tighten-cond handler expr env bound dolex dorail)
  (cons handler (map (lambda (clause)
		       (cond ((eq? (car clause) 'else)
			      `(ELSE
				,@(map (lambda (x)
					 (dotighten x env bound dolex dorail))
				       (cdr clause))))
			     ((and (pair? (cdr clause)) (eq? (cadr clause) '=>))
			      `(,(dotighten (car clause) env bound dolex dorail)
				=>
				,@(map (lambda (x)
					 (dotighten x env bound dolex dorail))
				       (cddr clause))))
			     (else (map (lambda (x)
					  (dotighten x env bound dolex dorail))
					clause))))
		     (cdr expr))))

(define (tighten-case handler expr env bound dolex dorail)
  `(,handler ,(dotighten (cadr expr) env bound dolex dorail)
	     ,@(map (lambda (clause)
		      (cons (car clause)
			    (map (lambda (x)
				   (dotighten x env bound dolex dorail))
				 (cdr clause))))
		    (cddr expr))))

(define (tighten-unwind-protect handler expr env bound dolex dorail)
  `(,handler ,(dotighten (cadr expr) env bound dolex dorail)
	     ,@(map (lambda (uwclause)
		      (dotighten uwclause env bound dolex dorail))
		    (cddr expr))))

;;; Tightening XHTML expressions

;; This doesn't handle mixed alist ((x y)) and plist (x y) attribute lists
;;  because they shouldn't work anyway
(define (tighten-attribs attribs env bound dolex dorail)
  (cond ((not (pair? attribs)) attribs)
	((pair? (cdr attribs))
	 `(,(if (and (pair? (car attribs))
		     (not (eq? (car (car attribs)) 'quote)))
		`(,(car (car attribs))
		  ,(dotighten (cadr (car attribs)) env bound dolex dorail))
		(car attribs))
	   ,(if (and (pair? (car attribs))
		     (not (eq? (car (car attribs)) 'quote)))
		(if (pair? (cadr attribs))
		    `(,(car (cadr attribs))
		      ,(dotighten (cadr (cadr attribs)) env bound dolex dorail))
		    (dotighten (cadr attribs) env bound dolex dorail))
		(dotighten (cadr attribs) env bound dolex dorail))
	   ,@(tighten-attribs (cddr attribs) env bound dolex dorail)))
	((pair? (car attribs))
	 `((,(car (car attribs))
	    ,(dotighten (cadr (car attribs)) env bound dolex dorail))))
	(else attribs)))

(define (tighten-markup handler expr env bound dolex dorail)
  `(,(car expr)
    ,@(map (lambda (x) (dotighten x env bound dolex dorail))
	   (cdr expr))))
(define (tighten-markup* handler expr env bound dolex dorail)
  `(,(car expr) ,(tighten-attribs (second expr) env bound dolex dorail)
    ,@(map (lambda (x) (dotighten x env bound dolex dorail))
	   (cddr expr))))

(define (tighten-emptymarkup fcn expr env bound dolex dorail)
  `(,(car expr) ,@(tighten-attribs (cdr expr) env bound dolex dorail)))

(define (tighten-anchor* fcn expr env bound dolex dorail)
  `(,(car expr) ,(dotighten (cadr expr) env bound dolex dorail)
    ,(tighten-attribs (third expr) env bound dolex dorail)
    ,@(map (lambda (elt) (dotighten elt env bound dolex dorail))
	   (cdr (cddr expr)))))

;;; Declare them

(add! special-form-tighteners (choice let letq) tighten-let)
(add! special-form-tighteners (choice let* letq*) tighten-let*)
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
      (choice begin prog1 until while ipeval
	      tipeval try if tryif when unless and or)
      tighten-block)
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
;; Don't optimize these because they look at the symbol that is the head
;; of the expression to get their tag name.
(add! special-form-tighteners {"markupblock" "ANCHOR"} tighten-markup)
(add! special-form-tighteners {"markup*block" "markup*"} tighten-markup*)
(add! special-form-tighteners "emptymarkup" tighten-emptymarkup)
(add! special-form-tighteners "ANCHOR*" tighten-anchor*)

(when (bound? fileout)
  (add! special-form-tighteners
	(choice fileout system)
	tighten-block))
