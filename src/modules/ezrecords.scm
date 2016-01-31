;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'ezrecords)

;;; This provides a dead simple RECORDS implementation 
;;;  building on FramerD's built-in compounds.

(define xref-opcode (make-opcode 0xA2))

(define (make-xref-generator off tag)
  (lambda (expr) `(,xref-opcode ,(cadr expr) ,off ',tag)))

(define (make-accessor-def field tag prefix fields)
  (let* ((field-name (if (pair? field) (car field) field))
	 (get-method-name (string->symbol (stringout prefix "-" field-name))))
    `(define (,get-method-name ,tag)
       (,xref-opcode ,tag ,(position field fields) ',tag))))
(define (make-modifier-def field tag prefix fields)
  (let* ((field-name (if (pair? field) (car field) field))
	 (set-method-name
	  (string->symbol (stringout "SET-" prefix "-" field-name "!"))))
    `(defambda (,set-method-name ,tag _value)
       (,compound-set! ,tag ,(position field fields) _value ',tag))))
(define (make-accessor-subst field tag prefix fields)
  (let* ((field-name (if (pair? field) (car field) field))
	 (get-method-name (string->symbol (stringout prefix "-" field-name))))
    `(set+! %rewrite
	    (cons ',get-method-name
		  (,make-xref-generator ,(position field fields) ',tag)))))

(define (fieldname x)
  (if (pair? x) (car x) x))

;(defrecord tag field1 (field2 opt) field3)
(define defrecord
  (macro expr
    (let* ((defspec (cadr expr))
	   (tag (if (symbol? defspec) defspec
		    (if (pair? defspec) (car defspec)
			(get defspec 'tag))))
	   (prefix (or (getopt defspec 'prefix) tag))
	   (ismutable (or (and (pair? defspec) (position 'mutable defspec))
			  (testopt defspec 'mutable)))
	   (isopaque (or (and (pair? defspec) (position 'opaque defspec))
			 (testopt defspec 'opaque)))
	   (corelen (getopt defspec 'corelen))
	   (consfn (getopt defspec 'consfn))
	   (stringfn (getopt defspec 'stringfn))
	   (fields (cddr expr))
	   (field-names (map fieldname fields))
	   (cons-method-name (string->symbol (stringout "CONS-" tag)))
	   (predicate-method-name (string->symbol (stringout tag "?"))))
      `(begin (bind-default! %rewrite {})
	 (defambda (,cons-method-name ,@fields)
	   (,(if ismutable
		 (if isopaque make-opaque-mutable-compound make-mutable-compound)
		 (if isopaque make-opaque-compound make-compound))
	    ',tag ,@field-names))
	 (define (,predicate-method-name ,tag)
	   (,compound-type? ,tag ',tag))
	 ,@(map (lambda (field) (make-accessor-def field tag prefix fields))
		fields)
	 ,@(map (lambda (field) (make-accessor-subst field tag prefix fields))
		fields)
	 ,@(if ismutable
	       (map (lambda (field) (make-modifier-def field tag prefix fields))
		    fields)
	       '())
	 ,@(if corelen `((compound-set-corelen! ',tag ,corelen)) '())
	 ,@(if consfn `((compound-set-consfn! ',tag ,consfn)) '())
	 ,@(if stringfn `((compound-set-stringfn! ',tag ,stringfn)) '())))))

(module-export! '{defrecord})

