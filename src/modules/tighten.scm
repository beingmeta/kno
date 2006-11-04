(in-module 'tighten)

(use-module 'reflection)

(module-export! '{tighten! tighten-module!})

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

(define dotighten
  (ambda (expr env bound dolex)
    (cond ((ambiguous? expr)
	   (for-choices (each expr) (dotighten each env bound dolex)))
	  ((symbol? expr)
	   (let ((lexref (get-lexref expr bound 0)))
	     (if lexref (if dolex lexref expr)
		 (let ((module (wherefrom expr env)))
		   (if module (list %get module (list 'quote expr))
		       expr)))))
	  ((not (pair? expr)) expr)
	  ((pair? (car expr))
	   (map (lambda (x) (dotighten x env bound dolex)) expr))
	  ((or (and (symbol? (car expr)) (not (symbol-bound? (car expr) env))))
	   expr)
	  (else (let* ((head (car expr))
		       (value (if (symbol? head)
				  (or (get-lexref head bound 0) (get env head))
				  head)))
		  (cond ((applicable? value)
			 (cons value (map (lambda (x)
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

(define (tighten! proc (dolex #t))
  (let* ((env (procedure-env proc))
	 (arglist (procedure-args proc))
	 (body (procedure-body proc))
	 (bound (cons (arglist->vars arglist)
		      (if (symbol-bound? '%notighten env)
			  (choice->list (get env '%notighten))
			  '()))))
    (set-procedure-body!
     proc (map (lambda (b) (dotighten b env bound dolex))
	       body))))

(define (tighten-module! module)
  (let ((bindings (difference (module-bindings module)
			      (get module '%notighten)))
	(count 0))
    (do-choices (var bindings)
      (let ((value (get module var)))
	(when (compound-procedure? value)
	  (set! count (1+ count)) (tighten! value))))
    count))


;;;; Special form handlers

(define (tighten-block handler expr env bound dolex)
  (cons handler (map (lambda (x) (dotighten x env bound dolex)) (cdr expr))))

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
