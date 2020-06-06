;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'defmacro)

(use-module '{varconfig logger})

(module-export! 'defmacro)

(define (match-expr pattern expr table)
  (cond ((ambiguous? pattern) table)
	((symbol? pattern)
	 (if (eq? pattern expr)
	     table
	     (if (test table pattern)
		 (and (test table pattern expr) table)
		 (begin (store! table pattern expr) table))))
	((equal? pattern expr) table)
	((pair? pattern)
	 (and (match-expr (car pattern) (car expr) table)
	      (match-expr (cdr pattern) (cdr expr) table)))
	(else #f)))

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

