(in-module 'ezrecords)

(define xref-opcode (make-opcode 0xA2))

(define (make-xref-generator off tag)
  (lambda (expr) `(,xref-opcode ,(cadr expr) ,off ',tag)))

;(defrecord tag field1 (field2 opt) field3)
(define defrecord
  (macro expr
    (let* ((tag (cadr expr))
	   (fields (cddr expr))
	   (field-names (map (lambda (x) (if (pair? x) (car x) x)) fields))
	   (make-method-name (string->symbol (stringout "MAKE-" tag)))
	   (predicate-method-name (string->symbol (stringout tag "?"))))
      `(begin (define (,make-method-name ,@fields)
		(,make-compound ',tag ,@field-names))
	      (define (,predicate-method-name ,tag) (,compound-type? ,tag ',tag))
	      ,@(map (lambda (field)
		       (let* ((field-name (if (pair? field) (car field) field))
			      (get-method-name (string->symbol (stringout tag "-" field-name))))
			 `(define (,get-method-name ,tag)
			    (,xref-opcode ,tag ,(position field fields) ,tag))))
		     fields)
	      ,@(map (lambda (field)
		       (let* ((field-name (if (pair? field) (car field) field))
			      (get-method-name (string->symbol (stringout tag "-" field-name))))
			 `(set+! %subst
				 (cons ',get-method-name (,make-xref-generator ,(position field fields) ',tag)))))
		     fields)))))

(module-export! 'defrecord)
