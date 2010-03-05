;;; -*- Mode: Scheme; -*-

(in-module 'openlibrary)

;;; Provides access to the Open Library API

(use-module '{fdweb texttools ezrecords})

(module-export!
 '{olib
   olib/ref olib/import olib/fetch olib/parse
   olib/query olib/bibref
   olib/get})

(defrecord olib key)

(define olib-refs (make-hashtable))

(define newolibref
  (slambda (key)
    (try (get olib-refs key)
	 (let ((new (cons-olib key)))
	   (store! olib-refs key new)
	   new))))

(define (olib/ref key)
  (try (get olib-refs key) (newolibref key)))

(define (olib/import data)
  (cond ((string? data) data)
	((vector? data) (map olib/import data))
	((and (table? data)
	      (= (table-size data) 1)
	      (test data "key"))
	 (olib/ref (get data "key")))
	((and (table? data) (test data "type" "/type/datetime"))
	 (parsetime (get data "value")))
	((and (table? data) (test data "type" "/type/text"))
	 (get data "value"))
	((table? data)
	 (let ((new (frame-create #f)))
	   (do-choices (key (getkeys data))
	     (let ((slotid (intern (upcase key)))
		   (value (get data key)))
	       (cond ((equal? key "type") (store! new 'type (olib/ref value)))
		     ((string? value) (store! new slotid value))
		     ((vector? value)
		      (let ((converted (map olib/import value)))
			(store! new (intern (stringout "%" slotid))
				converted)
			(add! new slotid (elts converted))))
		     ((and (table? value) (test value "key"))
		      (olib/ref key))
		     (else (store! new slotid value)))))
	   new))
	(else data)))

(define (olib/fetch ref)
  (if (string? ref)
      (let ((r (urlget (stringout "http://openlibrary.org/"
				  (if (has-prefix ref "/")
				      (subseq ref 1) ref)
				  ".json"))))
	(and (test r 'response 200)
	     (olib/import (jsonparse (get r '%content)))))
      (if (olib? ref)
	  (olib/fetch (olib-key ref))
	  (error "Invalid OLIB (Open Library) value"))))

(define (olib/parse string)
  (if (search "callback(" string)
      (olib/import (jsonparse (subseq string (search "callback(" string))))
      (olib/import (jsonparse string))))

(define (olib/get ref slotid) (get (get olib ref) slotid))

;;;; The query interfaces

(define (olib/query . args)
  (let* ((args (map (lambda (x) (if (olib? x) (olib-key x) x))
		    args))
	 (uri (apply scripturl
		     "http://openlibrary.org/query.json"
		     args))
	 (req (urlget uri)))
    (tryif (test req 'response 200)
      (let ((result (olib/parse (get req '%content))))
	(if (vector? result) (elts result) result)))))

(define (olib/bibref type (val #f))
  (let* ((bibkey (if val (stringout type ":" val) type))
	 (uri (stringout "http://openlibrary.org/api/books?bibkeys="
			 bibkey "&details=true&callback=callback"))
	 (req (urlget uri)))
    (tryif (test req 'response 200)
      (let* ((content (get req '%content))
	     (cbstart (search "callback(" content))
	     (parsed (and cbstart (jsonparse (subseq content (+ cbstart 9))))))
	(tryif parsed
	  (olib/ref (get (get (get parsed bibkey) "details") "key")))))))

;;; The olib itself

(define olib (make-extindex "openlibrary" olib/fetch))
