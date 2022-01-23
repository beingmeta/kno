;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu).

(in-module 'defstruct)

;;; This provides a dead simple RECORDS implementation 
;;;  building on Kno's built-in compounds.

(define (make-xref-generator off tag-expr)
  (lambda (expr) `(,compound-ref ,(cadr expr) ,off ',tag-expr)))
(define (make-predicate-generator off tag-expr)
  (lambda (expr) `(,compound? ,(cadr expr) ',tag-expr)))

(define (make-accessor-def name field tag-expr prefix fields)
  (let* ((field-name (if (pair? field) (car field) field))
	 (get-method-name (string->symbol (stringout prefix "-" field-name))))
    `(define (,get-method-name ,name)
       (,compound-ref ,name ,(position field fields) ,tag-expr))))
(define (make-setter-def name field tag-expr prefix fields allmutable)
  (and (or allmutable (and (pair? field) (pair? (cdr field)) (getopt (cddr field) 'mutable)))
       (let* ((field-name (if (pair? field) (car field) field))
	      (set-method-name
	       (string->symbol (stringout "set-" prefix "-" field-name "!"))))
	 `(defambda (,set-method-name ,name _value)
	    (,compound-set! ,name ,(position field fields) _value ,tag-expr)))))
(define (make-modifier-def name field tag-expr prefix fields allmutable)
  (and (or allmutable (and (pair? field) (pair? (cdr field)) (getopt (cddr field) 'mutable)))
       (let* ((field-name (if (pair? field) (car field) field))
	      (modify-method-name
	       (string->symbol (stringout "modify-" prefix "-" field-name "!"))))
	 `(defambda (,modify-method-name ,name _modifier _value)
	    (,compound-modify! ,name ,tag-expr ,(position field fields) 
			       _modifier _value)))))
(define (make-accessor-subst name field tag-expr prefix fields)
  (let* ((field-name (if (pair? field) (car field) field))
	 (get-method-name (string->symbol (stringout prefix "-" field-name))))
    `(set+! %rewrite
       (cons ',get-method-name
	     (,make-xref-generator 
	      ,(position field fields)
	      ,tag-expr)))))
(define (make-predicate-subst name field tag-expr prefix fields)
  (let* ((predicate-name (string->symbol (stringout prefix "?"))))
    `(set+! %rewrite
       (cons ',predicate-name (,make-predicate-generator ,tag-expr)))))

(define (fieldname x)
  (if (pair? x) (car x) x))

;;(defstruct typespec . fieldspecs...)
;; typespec = typetag |
;;            (typetag . struct-opts...)
;; typetag = symbol or OID
;; struct-opt = schemap | slotmap | symbol
;; struct-opt = schemap | slotmap | symbol

(defambda (getopt-static-inner opts prop)
  (cond ((pair? opts)
	 (try (getopt-static-inner (car opts) prop)
	      (getopt-static-inner (cdr opts) prop)))
	((slotmap? opts) (getopt opts prop {}))
	;; Schemaps need to be evaluated, so we don't use them
	((schemap? opts) (fail))
	((symbol? opts) (tryif (overlaps? prop opts) opts))
	(else (fail))))

(defambda (getopt-static opts prop (default #f))
  (try (getopt-static-inner opts prop) default))

(defambda (get-eval-opt opts property)
  "Returns a value for *property* from *opts* for evaluation. "
  "Opts comes from a `defstruct` body, so may contain schemaps "
  "or slotmaps and values from slotmaps are quoted."
  (try-choices (opt opts)
    (cond ((pair? opt)
	   (try (get-eval-opt (car opt) property)
		(get-eval-opt (cdr opt) property)))
	  ((slotmap? opt)
	   (let ((v (getopt opt property {})))
	     (for-choices v
	       (if (or (symbol? v) (pair? v))
		   (list 'quote v)
		   v))))
	  ((schemap? opt) (getopt opt property {}))
	  ((symbol? opt)
	   (tryif (overlaps? opt property)
	     (list 'quote opt)))
	  ((or (not opt) (empty-list? opt)) (fail))
	  (else opt))))

(defambda (get-static-opts opts)
  (for-choices (opt opts)
    (cond ((pair? opt)
	   (let ((head (get-static-opts (car opt)))
		 (tail (get-static-opts (cdr opt))))
	     (cond  ((and (fail? head) (fail? tail)) (fail))
		    ((fail? head) tail)
		    ((fail? tail) head)
		    (else (cons (qc head) (qc tail))))))
	  ((schemap? opt) (fail))
	  ((symbol? opt) opt)
	  (else opt))))

(define (pick-mutable fields)
  (for-choices (field (elts fields))
    (tryif (and (pair? field) (pair? (cdr field)))
      (tryif (getopt (cddr field) 'mutable)
	(car field)))))

(define defstruct
  (macro defstruct
    (let* ((defspec (cadr defstruct))
	   (defopts (and (pair? defspec) (cdr defspec)))
	   (static-opts (and defopts (get-static-opts defopts)))
	   (name (if (symbol? defspec) defspec
		     (if (and (pair? defspec) (symbol? (car defspec)))
			 (car defspec)
			 (or (getopt static-opts 'name)
			     (irritant defspec |NoName|)))))
	   (tag-expr (getopt defspec 'tag `',name))
	   (prefix (getopt defspec 'prefix name))
	   (ismutable (getopt static-opts 'mutable))
	   (readonly (getopt static-opts 'readonly))
	   (isopaque (getopt static-opts 'opaque))
	   (isseq (getopt static-opts 'sequence))
	   (istable (or (getopt static-opts 'table) (getopt static-opts 'annotated)))
	   (consfn (try (get-eval-opt defopts 'consfn) #f))
	   (stringfn (try (get-eval-opt defopts 'stringfn) #f))
	   (fields (cddr defstruct))
	   (field-names (map fieldname fields))
	   (cons-method-name (string->symbol (stringout "cons-" name)))
	   (predicate-method-name 
	    (getopt defspec 'predicate (string->symbol (stringout name "?")))))
      `(begin (default! %rewrite {})
	 (defambda (,cons-method-name ,@fields)
	   (,make-xcompound ,tag-expr ,ismutable ,isopaque
			    ,(cond ((not isseq) #f)
				   ((eq? isseq 'tail) (length file-names))
				   (else #t)) 
			    ,istable
			    ,@field-names))
	 (define (,predicate-method-name ,name)
	   (,compound-type? ,name ,tag-expr))
	 ,@(forseq (field fields)
	     (make-accessor-def name field tag-expr prefix fields))
	 ,@(forseq (field fields)
	     (make-accessor-subst name field tag-expr prefix fields))
	 ,@(if readonly '()
	       (forseq (field fields)
		 (make-setter-def name field tag-expr prefix fields ismutable)))
	 ,@(if readonly '()
	       (forseq (field fields)
		 (make-modifier-def name field tag-expr prefix fields ismutable)))
	 ,@(if consfn `((type-set-consfn! ,tag-expr ,consfn)) '())
	 ,@(if stringfn `((type-set-stringfn! ,tag-expr ,stringfn)) '())))))

(module-export! '{defstruct})

