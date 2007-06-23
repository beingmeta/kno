(in-module 'tighten)

(use-module 'reflection)

(define useopcodes #t)

(define opcodeopt-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) useopcodes)
	  (val (set! useopcodes #t))
	  (val (set! useopcodes #f)))))
(config-def! 'opcodeopt opcodeopt-config)

(module-export! '{tighten! tighten-procedure! tighten-module!})

;; This module optimizes an expression or procedure by replacing
;; certain variable references with their values directly, which
;; avoids many environment lookups.  The trick is to not replace
;; anything which will change and so produce an equivalent expression
;; or function which just runs faster.

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
  (if use-opcodes
      (try (get opcode-map (cons value length))
	   (get opcode-map value)
	   value)
      value))

(define (def-opcode prim code (n-args #f))
  (if n-args
      (add! opcode-map (cons prim n-args) (make-opcode code))
      (add! opcode-map prim (make-opcode code))))

(when (bound? make-opcode)
  (def-opcode 'QUOTE 0)
  (def-opcode AMBIGUOUS? 1 1)
  (def-opcode SINGLETON? 2 1)
  (def-opcode FAIL? 3 1)
  (def-opcode EMPTY? 3 1)
  (def-opcode EXISTS? 4 1)
  (def-opcode 1+ 5 1)
  (def-opcode 1- 6 1)
  (def-opcode -1+ 6 1)
  (def-opcode ZERO? 7 1)
  (def-opcode NULL? 8 1)
  (def-opcode NUMBER? 9 1)
  (def-opcode VECTOR? 10 1)
  (def-opcode PAIR? 11 1)
  (def-opcode CAR 12 1)
  (def-opcode CDR 13 1)
  (def-opcode SINGLETON 14 1)
  (def-opcode EQ? 15 2)
  (def-opcode EQV? 16 2)
  (def-opcode EQUAL? 17 2)
  (def-opcode GET 18 2)
  (def-opcode TEST 19)
  (def-opcode ELT 20 2)
  (def-opcode > 21 2)
  (def-opcode >= 22 2)
  (def-opcode < 23 2)
  (def-opcode <= 24 2)
  (def-opcode = 25 2)
  (def-opcode IF 26 2)
  (def-opcode IF 26 3)
  (def-opcode BEGIN 27)
  (def-opcode WHEN 28)
  (def-opcode UNLESS 29))

;;; The core loop

(define dotighten
  (ambda (expr env bound dolex)
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
		       expr)))))
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
		  (cond ((applicable? value)
			 (cons (map-opcode
				(cond ((not from) value)
				      ((test from '%notighten head) head)
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
			(else expr)))))))

(define (tighten-procedure! proc (dolex #t))
  (let* ((env (procedure-env proc))
	 (arglist (procedure-args proc))
	 (body (procedure-body proc))
	 (bound (list (arglist->vars arglist))))
    (set-procedure-body!
     proc (map (lambda (b) (dotighten b env bound dolex))
	       body))))

(define (tighten-module! module)
  (let ((bindings (module-bindings module))
	(count 0))
    (do-choices (var bindings)
      (let ((value (get module var)))
	(when (compound-procedure? value)
	  (set! count (1+ count)) (tighten! value))))
    count))

(define (tighten*! . args)
  (dolist (arg args)
    (cond ((compound-procedure? arg) (tighten-procedure! arg))
	  ((table? arg) (tighten-module! arg))
	  (else (error "Invalid tighten argument" arg)))))

(define tighten!
  (macro expr
    (cons tighten*!
	  (map (lambda (x)
		 (if (and (singleton? x) (pair? x) (eq? (car x) 'quote))
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

;;; Declare them

(add! special-form-tighteners (choice let letq) tighten-let)
(add! special-form-tighteners (choice let* letq*) tighten-let*)
(add! special-form-tighteners (choice lambda ambda slambda)
      tighten-lambda)
(add! special-form-tighteners (choice set! set+!)
      tighten-set-form)

(add! special-form-tighteners
      (choice do-choices for-choices filter-choices dolist dotimes doseq)
      tighten-doexpression)

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

(when (bound? fileout)
  (add! special-form-tighteners
	(choice fileout system)
	tighten-block))
