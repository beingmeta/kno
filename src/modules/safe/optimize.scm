;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'optimize)

;; This module optimizes an expression or procedure by replacing
;; certain variable references with their values directly, which
;; avoids many environment lookups.  The trick is to not replace
;; anything which will change and so produce an equivalent expression
;; or function which just runs faster.

(define version "$Id$")

(use-module 'reflection)

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

  ;;(def-opcode GET 18 2)
  ;;(def-opcode TEST 19)
  )

;;; The core loop

(defambda (dotighten expr env bound dolex)
  (cond ((ambiguous? expr)
	 (for-choices (each expr) (dotighten each env bound dolex)))
	((symbol? expr)
	 (let ((lexref (get-lexref expr bound 0)))
	   (if lexref (if dolex lexref expr)
	       (let ((module (wherefrom expr env)))
		 (if module
		     (if (%test module '%constants expr)
			 (let ((v (%get module expr)))
			   (if (or (pair? v) (symbol? v) (ambiguous? v))
			       (list 'quote (qc v))
			       v))
			 (list %get module (list 'quote expr)))
		     (begin
		       (when optdowarn
			 (codewarning (cons* 'UNBOUND expr bound))
			 (warning "The symbol " expr " appears to be unbound given bindings "
				  (apply append bound)))
		       expr))))))
	((not (pair? expr)) expr)
	((pair? (car expr))
	 (map (lambda (x) (dotighten x env bound dolex)) expr))
	((or (and (symbol? (car expr)) (not (symbol-bound? (car expr) env))))
	 expr)
	(else (let* ((head (car expr))
		     (n-exprs (length (cdr expr)))
		     (value (if (symbol? head)
				(or (get-lexref head bound 0) (get env head))
				head))
		     (from (and (symbol? head) (wherefrom head env))))
		(cond ((if from (test from '%unoptimized head)
			   (and env (test env '%unoptimized head)))
		       expr)
		      ((applicable? value)
		       (when (and optdowarn (fcn? value))
			 (when (and (fcn-min-arity value) (< n-exprs (fcn-min-arity value)))
			   (codewarning (list 'TOOFEWARGS expr value))
			   (warning "The call to " expr " provides too few arguments for " value))
			 (when (and (fcn-arity value) (> n-exprs (fcn-arity value)))
			   (codewarning (list 'TOOMANYARGS expr value))
			   (warning "The call to " expr " provides too many arguments for " value)))
		       (cons (map-opcode
			      (cond ((not from) value)
				    ((test from '%nosubst head) head)
				    ((test from '%volatile head) `(,%get ,from ',head))
				    (else value))
			      n-exprs)
			     (map (lambda (x)
				    (if (empty? x) x
					(dotighten (qc x) env bound dolex)))
				  (cdr expr))))
		      ((special-form? value)
		       (let ((tightener (get special-form-tighteners value)))
			 (if (exists? tightener) (tightener value expr env bound dolex)
			     expr)))
		      ((macro? value)
		       (dotighten (macroexpand value expr) env bound dolex))
		      (else expr))))))

(define (optimize-procedure! proc (dolex #t))
  (let* ((env (procedure-env proc))
	 (arglist (procedure-args proc))
	 (body (procedure-body proc))
	 (bound (list (arglist->vars arglist))))
    (threadset! 'codewarnings #{})
    (set-procedure-body!
     proc (map (lambda (b) (dotighten b env bound dolex))
	       body))
    (when (exists? (threadget 'codewarnings))
      (warning "Errors optimizing " proc)
      (threadset! 'codewarnings #{}))))

(define (optimize-module! module)
  (let ((bindings (module-bindings module))
	(count 0))
    (do-choices (var bindings)
      (let ((value (get module var)))
	(when (compound-procedure? value)
	  (set! count (1+ count)) (optimize! value))))
    count))

(defambda (module-arg? arg)
  (or (fail? arg) (string? arg)
      (and (pair? arg) (eq? (car arg) 'quote))
      (and (ambiguous? arg) (fail? (reject arg module-arg?)))))

(define (optimize*! . args)
  (dolist (arg args)
    (cond ((compound-procedure? arg) (optimize-procedure! arg))
	  ((table? arg) (optimize-module! arg))
	  (else (error '|TypeError| 'optimize* "Invalid tighten argument: " arg)))))

(define optimize!
  (macro expr
    (cons optimize*!
	  (map (lambda (x)
		 (if (module-arg? x)
		     `(get-module ,x)
		     x))
	       (cdr expr)))))


;;;; Special form handlers

(define (tighten-block handler expr env bound dolex)
  (cons (map-opcode handler (length (cdr expr)))
	(map (lambda (x) (dotighten x env bound dolex))
	     (cdr expr))))

(define (tighten-let handler expr env bound dolex)
  (let ((bindexprs (cadr expr)) (body (cddr expr)))
    `(,handler ,(map (lambda (x) `(,(car x) ,(dotighten (cadr x) env bound dolex)))
		     bindexprs)
	       ,@(let ((bound (cons (map car bindexprs) bound)))
		   (map (lambda (b) (dotighten b env bound dolex))
			body)))))
(define (tighten-doexpression handler expr env bound dolex)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler (,(car bindspec) ,(dotighten (cadr bindspec) env bound dolex)
		,@(cddr bindspec))
	       ,@(let ((bound (if (= (length bindspec) 3)
				  (cons (list (first bindspec) (third bindspec)) bound)
				  (cons (list (first bindspec)) bound))))
		   (map (lambda (b) (dotighten b env bound dolex))
			body)))))
(define (tighten-do2expression handler expr env bound dolex)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler ,(cond ((pair? bindspec)
		       `(,(car bindspec) ,(dotighten (cadr bindspec) env bound dolex)
			 ,@(cddr bindspec)))
		      ((symbol? bindspec)
		       `(,bindspec ,(dotighten bindspec env bound dolex)))
		      (else (error 'syntax "Bad do-* expression")))
	       ,@(let ((bound (if (symbol? bindspec)
				  (cons (list bindspec) bound)
				  (if (= (length bindspec) 3)
				      (cons (list (first bindspec) (third bindspec)) bound)
				      (cons (list (first bindspec)) bound)))))
		   (map (lambda (b) (dotighten b env bound dolex))
			body)))))

(define (tighten-dosubsets handler expr env bound dolex)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler (,(car bindspec) ,(dotighten (cadr bindspec) env bound dolex)
		,@(cddr bindspec))
	       ,@(let ((bound (if (= (length bindspec) 4)
				  (cons (list (first bindspec) (fourth bindspec)) bound)
				  (cons (list (first bindspec)) bound))))
		   (map (lambda (b) (dotighten b env bound dolex))
			body)))))

(define (tighten-let*-bindings bindings env bound dolex)
  (if (null? bindings) '()
      `((,(car (car bindings))
	 ,(dotighten (cadr (car bindings)) env bound dolex))
	,@(tighten-let*-bindings
	   (cdr bindings) env
	   (cons (append (car bound) (list (car (car bindings))))
		 (cdr bound))
	   dolex))))

(define (tighten-let* handler expr env bound dolex)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler
      ,(tighten-let*-bindings (cadr expr) env (cons '() bound) dolex)
      ,@(let ((bound (cons (map car bindspec) bound)))
	  (map (lambda (b) (dotighten b env bound dolex))
	       body)))))
(define (tighten-set-form handler expr env bound dolex)
  `(,handler ,(cadr expr) ,(dotighten (third expr) env bound dolex)))

(define (tighten-lambda handler expr env bound dolex)
  `(,handler ,(cadr expr)
	     ,@(let ((bound (cons (arglist->vars (cadr expr)) bound)))
		 (map (lambda (b) (dotighten b env bound dolex))
		      (cddr expr)))))

(define (tighten-cond handler expr env bound dolex)
  (cons handler (map (lambda (clause)
		       (cond ((eq? (car clause) 'else)
			      `(ELSE ,@(map (lambda (x) (dotighten x env bound dolex))
					    (cdr clause))))
			     ((and (pair? (cdr clause)) (eq? (cadr clause) '=>))
			      `(,(dotighten (car clause) env bound dolex)
				=>
				,@(map (lambda (x) (dotighten x env bound dolex))
				       (cddr clause))))
			     (else (map (lambda (x) (dotighten x env bound dolex))
					clause))))
		     (cdr expr))))

(define (tighten-case handler expr env bound dolex)
  `(,handler ,(dotighten (cadr expr) env bound dolex)
	     ,@(map (lambda (clause)
		      (cons (car clause)
			    (map (lambda (x) (dotighten x env bound dolex))
				 (cdr clause))))
		    (cddr expr))))

(define (tighten-unwind-protect handler expr env bound dolex)
  `(,handler ,(dotighten (cadr expr) env bound dolex)
	     ,@(map (lambda (uwclause) (dotighten uwclause env bound dolex))
		    (cddr expr))))

;;; Declare them

(add! special-form-tighteners (choice let letq) tighten-let)
(add! special-form-tighteners (choice let* letq*) tighten-let*)
(add! special-form-tighteners (choice lambda ambda slambda)
      tighten-lambda)
(add! special-form-tighteners (choice set! set+!)
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

(when (bound? fileout)
  (add! special-form-tighteners
	(choice fileout system)
	tighten-block))
