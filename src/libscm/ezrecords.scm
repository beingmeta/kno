;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

(in-module 'ezrecords)

;;; This provides a dead simple RECORDS implementation 
;;;  building on FramerD's built-in compounds.

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
	  (string->symbol (stringout "SET-" prefix "-" field-name "!"))))
    `(defambda (,set-method-name ,name _value)
       (,compound-set! ,name ,(position field fields) _value ,tag-expr))))
(define (make-modifier-def name field tag-expr prefix fields)
  (let* ((field-name (if (pair? field) (car field) field))
	 (modify-method-name
	  (string->symbol (stringout "MODIFY-" prefix "-" field-name "!"))))
    `(defambda (,modify-method-name ,name _modifier _value)
       (,compound-modify! ,name ,(position field fields) _modifier _value ,tag-expr))))
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
	   (corelen (getopt defspec 'corelen))
	   (consfn (getopt defspec 'consfn))
	   (stringfn (getopt defspec 'stringfn))
	   (fields (cddr defrecord))
	   (field-names (map fieldname fields))
	   (cons-method-name (string->symbol (stringout "CONS-" name)))
	   (predicate-method-name (getopt defspec 'predicate (string->symbol (stringout name "?")))))
      `(begin (bind-default! %rewrite {})
	 (defambda (,cons-method-name ,@fields)
	   (,(if ismutable
		 (if isopaque make-opaque-mutable-compound make-mutable-compound)
		 (if isopaque make-opaque-compound make-compound))
	    ,tag-expr ,@field-names))
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
	 ,@(if corelen `((compound-set-corelen! ,tag-expr ,corelen)) '())
	 ,@(if consfn `((compound-set-consfn! ,tag-expr ,consfn)) '())
	 ,@(if stringfn `((compound-set-stringfn! ,tag-expr ,stringfn)) '())))))

(module-export! '{defrecord})

