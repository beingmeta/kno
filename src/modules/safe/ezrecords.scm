(in-module 'ezrecords)

;;; This provides a dead simple RECORDS implementation 
;;;  building on FramerD's built-in compounds.
(define version "$Id$")


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


;(defrecord tag field1 (field2 opt) field3)
(define defrecord
  (macro expr
    (let* ((defspec (cadr expr))
	   (tag (if (symbol? defspec) defspec
		    (if (pair? defspec) (car defspec)
			(get defspec 'tag))))
	   (prefix (or (getopt defspec 'prefix) tag))
	   (ismutable (or (and (pair? defspec) (position 'mutable defspec))
			  (and (table? defspec) (test defspec 'mutable))))
	   (fields (cddr expr))
	   (field-names (map (lambda (x) (if (pair? x) (car x) x)) fields))
	   (cons-method-name (string->symbol (stringout "CONS-" tag)))
	   (predicate-method-name (string->symbol (stringout tag "?"))))
      `(begin (bind-default! %rewrite {})
	      (defambda (,cons-method-name ,@fields)
		(,(if ismutable make-mutable-compound make-compound) ',tag ,@field-names))
	      (define (,predicate-method-name ,tag)
		(,compound-type? ,tag ',tag))
	      ,@(map (lambda (field) (make-accessor-def field tag prefix fields))
		     fields)
	      ,@(map (lambda (field) (make-accessor-subst field tag prefix fields))
		     fields)
	      ,@(if ismutable
		    (map (lambda (field) (make-modifier-def field tag prefix fields))
			 fields)
		    '())))))

(module-export! 'defrecord)
