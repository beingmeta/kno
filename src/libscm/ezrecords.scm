;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'ezrecords)

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
(define (make-setter-def name field tag-expr prefix fields)
  (let* ((field-name (if (pair? field) (car field) field))
	 (set-method-name
	  (string->symbol (stringout "set-" prefix "-" field-name "!"))))
    `(defambda (,set-method-name ,name _value)
       (,compound-set! ,name ,(position field fields) _value ,tag-expr))))
(define (make-modifier-def name field tag-expr prefix fields)
  (let* ((field-name (if (pair? field) (car field) field))
	 (modify-method-name
	  (string->symbol (stringout "modify-" prefix "-" field-name "!"))))
    `(defambda (,modify-method-name ,name _modifier _value)
       (,compound-modify! ,name ,tag-expr ,(position field fields) 
			  _modifier _value))))
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

;(defrecord tag field1 (field2 opt) field3)
(define defrecord
  (macro defrecord
    (let* ((defspec (cadr defrecord))
	   (name (if (symbol? defspec) defspec
		     (if (and (pair? defspec) (symbol? (car defspec)))
			 (car defspec)
			 (getopt defspec 'name
				 (irritant defspec |NoName|)))))
	   (tag-expr (getopt defspec 'tag `',name))
	   (prefix (getopt defspec 'prefix name))
	   (ismutable (or (and (pair? defspec) (position 'mutable defspec))
			  (testopt defspec 'mutable)))
	   (isopaque (or (and (pair? defspec) (position 'opaque defspec))
			 (testopt defspec 'opaque)))
	   (isseq (or (and (pair? defspec) (position 'sequence defspec))
		      (getopt defspec 'sequence #f)))
	   (istable (or (and (pair? defspec) (position 'table defspec))
			(testopt defspec 'table)))
	   (consfn (getopt defspec 'consfn))
	   (stringfn (getopt defspec 'stringfn))
	   (fields (cddr defrecord))
	   (field-names (map fieldname fields))
	   (cons-method-name (string->symbol (stringout "cons-" name)))
	   (predicate-method-name 
	    (getopt defspec 'predicate (string->symbol (stringout name "?")))))
      `(begin (default! %rewrite {})
	 (defambda (,cons-method-name ,@fields)
	   (,make-xcompound ,tag-expr ,ismutable ,isopaque ,istable
			    ,(cond ((not isseq) #f)
				   ((eq? isseq 'tail) (length field-names))
				   (else #t))
			    ,@field-names))
	 (define (,predicate-method-name ,name)
	   (,compound-type? ,name ,tag-expr))
	 ,@(forseq (field fields)
	     (make-accessor-def name field tag-expr prefix fields))
	 ,@(forseq (field fields)
	     (make-accessor-subst name field tag-expr prefix fields))
	 ,@(if ismutable
	       (forseq (field fields)
		 (make-setter-def name field tag-expr prefix fields))
	       '())
	 ,@(if ismutable
	       (forseq (field fields)
		 (make-modifier-def name field tag-expr prefix fields))
	       '())
	 ,@(if consfn `((type-set-consfn! ,tag-expr ,consfn)) '())
	 ,@(if stringfn `((type-set-stringfn! ,tag-expr ,stringfn)) '())))))

(module-export! '{defrecord})

