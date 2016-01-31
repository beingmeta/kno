;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'codewalker)

;;; Walks code for a variety of purposes

(use-module 'reflection)

(define codewalkers (make-hashtable))

(module-export! 'codewalker)

;;; Main function

(define (arg-symbol x)
  (if (pair? x) (car x) x))

(define (codewalker fcn arg (bound '()) (env `#[]))
  (cond ((fail? arg))
	((compound-procedure? arg)
	 (let* ((args (procedure-args arg))
		(body (procedure-body arg))
		(procenv (procedure-env arg))
		(procbound (append (map arg-symbol args) bound)))
	   (dolist (arg args)
	     (when (and (pair? arg) (pair? (cdr arg)))
	       (codewalker fcn (cadr arg) bound procenv)))
	   (dolist (elt body)
	     (codewalk-expr fcn elt procbound procenv))))
	((environment? arg)
	 (let ((bindings (module-bindings arg)))
	   (do-choices (symbol (getkeys bindings))
	     (when (test bindings symbol)
	       (codewalker fcn (get bindings symbol) bound env)))))
	(else (codewalk-expr fcn arg bound env))))

(define (special-call? head bindings env)
  (cond ((special-form? head) #t)
	((pair? head) #f)
	((and (symbol? head) (position head bindings)) #f)
	((and (symbol? head) (special-form? (get env head))) #t)
	(else #f)))

(define (codewalk-expr fcn expr (bound '()) (env `#[]))
  (cond ((oid? expr) (fcn #f expr bound env))
	((and (symbol? expr) (not (position expr bound)))
	 (fcn #f expr bound env))
	((pair? expr)
	 (let ((head-expr (car expr)))
	   (codewalker fcn head-expr bound env)
	   (if (special-call? head-expr bound env)
	       (let ((handler (try (get codewalkers (get env head-expr))
				   (get codewalkers
					(procedure-name (get env head-expr)))
				   (get codewalkers head-expr)
				   )))
		 (if (exists? handler)
		     (handler fcn expr bound env)
		     (begin (warning "No special form handler for " head-expr)
			    (dolist (elt (cdr expr))
			      (codewalker fcn elt bound env)))))
	       (if (and (not (position head-expr bound))
			(macro? (get env head-expr)))
		   (codewalk-expr fcn (macroexpand (get env head-expr) expr)
				  bound env)
		   (dolist (elt (cdr expr)) (codewalker fcn elt bound env))))))
	(else)))

;;; Expr handlers

(define (codewalk-ignore fcn expr bound env))

(define (codewalk-block fcn expr bound env)
  (dolist (elt expr) (codewalker fcn elt bound env)))

(define (codewalk-doexpression fcn expr bound env)
  (when (pair? (cadr expr))
    (codewalker fcn (cadr (cadr expr)) bound env))
  (let ((bound (if (pair? (cadr expr))
		   (cons (car (cadr expr)) bound)
		   (cons (cadr expr) bound))))
    (dolist (elt (cddr expr))
      (codewalker fcn elt bound env))))

(define (codewalk-do2expression fcn expr bound env)
  (when (pair? (cadr expr))
    (codewalker fcn (cadr (cadr expr)) bound env))
  (let* ((bind-expr (cadr expr))
	 (bound (if (pair? bind-expr)
		    (if (> (length bind-expr) 2)
			(cons* (car bind-expr) (third bind-expr) bound)
			(cons (car bind-expr) bound))
		    (cons bind-expr bound))))
    (dolist (elt (cddr expr))
      (codewalker fcn elt bound env))))

(define (codewalk-doplus fcn expr bound env)
  (let* ((bind-expr (cadr expr))
	 (newbound (cons (car bind-expr) bound)))
    (dolist (elt (cdr bind-expr)) (codewalker fcn elt bound env))
    (dolist (elt (cddr expr))
      (codewalker fcn elt newbound env))))

(define (codewalk-do fcn expr bound env)
  (do ((bindings (cadr expr) (cdr bindings))
       (newbound bound (cons (car (car bindings)) newbound)))
      ((null? bindings)
       (dolist (elt (third expr)) (codewalker fcn elt newbound env))
       (dolist (elt (cdr (cdr (cdr expr))))
	 (codewalker fcn elt newbound env)))
    (codewalker fcn (cadr (car bindings)) bound env)))

(define (codewalk-let fcn expr bound env)
  (do ((bindings (cadr expr) (cdr bindings))
       (newbound bound (cons (car (car bindings)) newbound)))
      ((null? bindings)
       (dolist (elt (cdr (cdr (cdr expr))))
	 (codewalker fcn elt newbound env)))
    (codewalker fcn (cadr (car bindings)) bound env)))

(define (codewalk-let* fcn expr bound env)
  (do ((bindings (cadr expr) (cdr bindings))
       (newbound bound (cons (car (car bindings)) newbound)))
      ((null? bindings)
       (dolist (elt (cddr expr))
	 (codewalker fcn elt newbound env)))
    (codewalker fcn (cadr (car bindings)) newbound env)))

(define (codewalk-unwind-protect fcn expr bound env)
  (codewalker fcn (cadr expr) bound env)
  (dolist (elt (caddr expr)) (codewalker fcn elt bound env)))

(define (codewalk-cond fcn expr bound env)
  (dolist (clause (cdr expr))
    (dolist (call clause)
      (codewalker fcn call bound env))))

(define (codewalk-case fcn expr bound env)
  (codewalker fcn (cadr expr) bound env)
  (dolist (clause (cddr expr))
    (dolist (call (cdr clause))
      (codewalker fcn call bound env))))

(define (codewalk-lambda fcn expr bound env)
  (let* ((args (second expr))
	 (body (cddr expr))
	 (newbound (append (map arg-symbol args) bound)))
    (dolist (arg args)
      (when (and (pair? arg) (pair? (cdr arg)))
	(codewalker fcn (cadr arg) bound env)))
    (dolist (elt body)
      (codewalker fcn elt bound env))))

(define (codewalk-set-form fcn expr bound env)
  (fcn 'set (second expr) bound env)
  (codewalker fcn (third expr) bound env))

;;; Quasiquote

(defambda (codewalk-quasiquote fcn expr bound env)
  (cond ((ambiguous? expr)
	 (do-choices (elt expr) (codewalk-quasiquote fcn elt bound env)))
	((pair? expr)
	 (if (or (eq? (car expr) 'unquote) (eq? (car expr) 'unquote*))
	     (codewalker fcn (cadr expr) bound env)
	     (dolist (elt expr)
	       (codewalk-quasiquote fcn elt bound env))))
	((vector? expr)
	 (doseq (elt expr)
	   (codewalk-quasiquote fcn elt bound env)))
	((slotmap? expr)
	 (do-choices (key (getkeys expr))
	   (do-choices (v (get expr key))
	     (codewalk-quasiquote fcn v bound env))))))

;;; Walking XHTML markup

;; This doesn't handle mixed alist ((x y)) and plist (x y) attribute lists
;;  because they shouldn't work anyway
(define (codewalk-attribs fcn attribs bound env)
  (do ((attribs attribs
		(if (pair? (cdr attribs))
		    (cddr attribs)
		    (cdr attribs))))
      ((null? attribs))
    (cond ((pair? (car attribs))
	   (codewalker fcn (cadr (car attribs)) bound env)
	   (if (and (pair? (cdr attribs)) (pair? (cadr attribs)))
	       (codewalker fcn (cadr (cadr attribs)) bound env)))
	  ((pair? (cdr attribs))
	   (codewalker fcn (cadr attribs) bound env))
	  (else (codewalker fcn (car attribs) bound env)))))

(define (codewalk-markup fcn expr bound env)
  (codewalk-attribs fcn (cadr expr) bound env)
  (dolist (elt (cddr expr)) (codewalker fcn elt bound env)))

(define (codewalk-xmlblock fcn expr bound env)
  (codewalk-attribs fcn (third expr) bound env)
  (dolist (elt (cdr (cddr expr))) (codewalker fcn elt bound env)))

(define (codewalk-emptymarkup fcn expr bound env)
  (codewalk-attribs fcn (cdr expr) bound env))

(define (codewalk-anchor* fcn expr bound env)
  (codewalker fcn (second expr) bound env)
  (codewalk-attribs fcn (third expr) bound env)
  (dolist (elt (cdr (cddr expr))) (codewalker fcn elt bound env)))

;;; Setting up the table

(add! codewalkers (choice "QUOTE" quote comment 'quote 'comment) codewalk-ignore)

(add! codewalkers quasiquote codewalk-quasiquote)

(add! codewalkers (choice let) codewalk-let)
(add! codewalkers (choice let*) codewalk-let*)
(add! codewalkers (choice lambda ambda slambda)
      codewalk-lambda)
(add! codewalkers (choice set! set+! default!)
      codewalk-set-form)

(add! codewalkers
      (choice dolist dotimes)
      codewalk-doexpression)
(add! codewalkers
      (choice do-choices for-choices filter-choices try-choices doseq forseq)
      codewalk-do2expression)

(add! codewalkers do codewalk-do)

(add! codewalkers
      (choice begin prog1 until while ;; ipeval tipeval
	      try if tryif when unless and or
	      default bound? ifexists)
      codewalk-block)
(add! codewalkers case codewalk-case)
(add! codewalkers cond codewalk-cond)
(add! codewalkers do-subsets codewalk-doplus)
(when (bound? parallel)
  (add! codewalkers
	(choice parallel spawn)
	codewalk-block))

(add! codewalkers
      (choice printout lineout stringout message notify)
      codewalk-block)

(add! codewalkers unwind-protect codewalk-unwind-protect)

(add! codewalkers {"FILEOUT" "SYSTEM"} codewalk-block)

;;; XHTML markup

(add! codewalkers {"markup" "markupblock" "ANCHOR" "XMLOUT" "XMLEVAL"} codewalk-block)
(add! codewalkers {"markup*block" "markup*"} codewalk-markup)
(add! codewalkers "XMLBLOCK" codewalk-xmlblock)
(add! codewalkers {"emptymarkup" "XMLELT"} codewalk-emptymarkup)
(add! codewalkers "ANCHOR*" codewalk-anchor*)
