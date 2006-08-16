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

(define dotighten
  (ambda (expr env bound)
    (cond ((ambiguous? expr)
	   (for-choices (each expr) (dotighten each env bound)))
	  ((symbol? expr)
	   (if (member expr bound) expr
	       (if (bound? expr env)
		   (let ((v (get env expr))) 
		     (if (or (pair? v) (symbol? v))
			 `',v
			 v))
		   expr)))
	  ((not (pair? expr)) expr)
	  ((pair? (car expr))
	   (map (lambda (x) (dotighten x env bound)) expr))
	  ((or (not (symbol? (car expr)))
	       (member (car expr) bound)
	       (not (symbol-bound? (car expr) env)))
	   expr)
	  (else (let ((value (get env (car expr))))
		  (cond ((applicable? value)
			 (cons value (map (lambda (x)
					    (if (empty? x) x
						(dotighten (qc x) env bound)))
					  (cdr expr))))
			((special-form? value)
			 (let ((tightener (get special-form-tighteners value)))
			   (if (exists? tightener) (tightener value expr env bound)
			       expr)))
			((macro? value) (macroexpand value expr))
			(else expr)))))))

(define (tighten! proc)
  (let* ((env (procedure-env proc))
	 (arglist (procedure-args proc))
	 (body (procedure-body proc))
	 (bound (append (if (symbol-bound? '%notighten env)
			    (choice->list (get env '%notighten))
			    '())
			(arglist->vars arglist))))
    (set-procedure-body!
     proc (map (lambda (b) (dotighten b env bound))
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

(define (tighten-block handler expr env bound)
  (cons handler (map (lambda (x) (dotighten x env bound)) (cdr expr))))

(define (tighten-let handler expr env bound)
  (let ((bindexprs (cadr expr)) (body (cddr expr)))
    `(,handler ,(map (lambda (x) `(,(car x) ,(dotighten (cadr x) env bound)))
		     bindexprs)
	       ,@(let ((bound (append (map car bindexprs) bound)))
		   (map (lambda (b) (dotighten b env bound))
			body)))))
(define (tighten-doexpression handler expr env bound)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler (,(car bindspec) ,(dotighten (cadr bindspec) env bound)
		,@(cddr bindspec))
	       ,@(let ((bound (if (= (length bindspec) 3)
				  (cons* (first bindspec) (third bindspec) bound)
				  (cons (first bindspec) bound))))
		   (map (lambda (b) (dotighten b env bound))
			body)))))

(define (tighten-let*-bindings bindings env bound)
  (if (null? bindings) '()
      `((,(car (car bindings))
	 ,(dotighten (cadr (car bindings)) env bound))
	,@(tighten-let*-bindings
	   (cdr bindings) env (cons (car (car bindings)) bound)))))

(define (tighten-let* handler expr env bound)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler
      ,(tighten-let*-bindings (cadr expr) env bound)
      ,@(let ((bound (append (map car bindspec) bound)))
	  (map (lambda (b) (dotighten b env bound))
	       body)))))
(define (tighten-set-form handler expr env bound)
  `(,handler ,(cadr expr) ,(dotighten (third expr) env bound)))

(define (tighten-lambda handler expr env bound)
  `(,handler ,(cadr expr)
	     ,@(let ((bound (append (arglist->vars (cadr expr)) bound)))
		 (map (lambda (b) (dotighten b env bound))
		      (cddr expr)))))

(define (tighten-cond handler expr env bound)
  (cons handler (map (lambda (clause)
		       (cond ((eq? (car clause) 'else)
			      `(ELSE ,@(map (lambda (x) (dotighten x env bound))
					    (cdr clause))))
			     ((and (pair? (cdr clause)) (eq? (cadr clause) '=>))
			      `(,(dotighten (car clause) env bound)
				=>
				,@(map (lambda (x) (dotighten x env bound))
				       (cddr clause))))
			     (else (map (lambda (x) (dotighten x env bound))
					clause))))
		     (cdr expr))))

(define (tighten-case handler expr env bound)
  `(,handler ,(dotighten (cadr expr) env bound)
	     ,@(map (lambda (clause)
		      (cons (car clause)
			    (map (lambda (x) (dotighten x env bound))
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
