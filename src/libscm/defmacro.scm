;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'defmacro)

(module-export! 'defmacro)

(define optsym '|OPT|)

(define (match-expr pattern expr table)
  (cond ((ambiguous? pattern) table)
	((symbol? pattern)
	 (if (eq? pattern expr)
	     table
	     (if (test table pattern)
		 (and (test table pattern expr) table)
		 (begin (store! table pattern expr) table))))
	((equal? pattern expr) table)
	((and (pair? pattern) (eq? (car pattern) optsym))
	 (let ((name (get-arg pattern 1 #f)))
	   (if (test table name)
	       (and (test table name expr) table)
	       (begin (store! table name expr) table))))
	((and (null? expr) (pair? pattern))
	 (bind-opts pattern table))
	((pair? pattern)
	 (and (match-expr (car pattern) (car expr) table)
	      (match-expr (cdr pattern) (cdr expr) table)))
	(else #f)))

(define (bind-opts pattern table (scan) (matched #[]))
  (set! scan pattern)
  (while (and matched (pair? scan))
    (let ((elt (car scan)))
      (if (and (pair? elt) (eq? (car elt) optsym))
	  (let ((name (get-arg elt 1 #f))
		(dflt (get-arg elt 2 #f)))
	    (if (test table name)
		(unless (test table name dflt) (set! matched #f))
		(store! matched name dflt)))
	  (set! matched #f))
      (set! scan (cdr scan))))
  (and matched
       (let ((matched-syms (getkeys matched)))
	 (when (symbol? scan)
	   (store! table scan '()))
	 (do-choices (key matched-syms)
	   (store! table key (get matched key)))
	 table)))
  

(define (get-matches pattern expr)
  (let ((matches {})
	(table (frame-create #f)))
    (if (match-expr pattern expr table) table
	(irritant expr |SyntaxError|))))

(defambda (match-subst expr matches)
  (for-choices expr
    (cond ((and (symbol? expr) (pick matches expr))
	   (get matches expr))
	  ((pair? expr)
	   (cons (qc (match-subst (car expr) matches))
		 (qc (match-subst (cdr expr) matches))))
	  ((vector? expr)
	   (forseq (elt expr)
	     (match-subst (car expr) matches)))
	  (else expr))))

(define defmacro
  (macro expr
    (if (and (pair? expr) (>= (length expr) 3))
	(let* ((template (cadr expr))
	       (name (if (and (pair? template) (symbol? (car template)))
			 (car template)
			 (irritant template |BadMacroTemplate|)))
	       (body (cddr expr)))
	  `(define ,name
	     (macro call-expr
	       (with-bindings (,get-matches ',template call-expr)
			      ,@body))))
	(irritant expr |SyntaxError|))))

