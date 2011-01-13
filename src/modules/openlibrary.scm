;;; -*- Mode: Scheme; -*-

(in-module 'openlibrary)

;;; Provides access to the Open Library API

(use-module '{fdweb texttools ezrecords parsetime logger})

(module-export!
 '{olib
   olib/ref olib/import olib/fetch olib/parse olib-key
   olib/query olib/bibref olib/id olib/image
   olib/get})

(define %loglevel %notify!)
;;(define %loglevel %debug!)

(defrecord olib key)

(define olib-refs (make-hashtable))

(define newolibref
  (slambda (key)
    (try (get olib-refs key)
	 (let ((new (cons-olib key)))
	   (store! olib-refs key new)
	   new))))

(define (olib/ref key)
  (if (olib? key) key
      (if (string? key)
	  (try (get olib-refs key) (newolibref key))
	  (if (and (table? key) (test key 'key))
	      (olib/ref (get key 'key))
	      (fail)))))

(define (olib/id ref)
  (subseq (olib-key ref) (1+ (position #\/  (olib-key ref) 1))))

(define text-type (choice "/type/text" (olib/ref "/type/text")))
(define author-role (choice "/type/author_role" (olib/ref "/type/author_role")))

(define (olib/import data (cache #f))
  (cond ((string? data) data)
	((vector? data) (map olib/import data))
	((and (table? data)
	      (= (table-size data) 1)
	      (test data 'key))
	 (olib/ref (get data 'key)))
	((and (table? data) (test data 'type "/type/datetime"))
	 (parsetime (get data 'value)))
	((and (table? data) (test data 'type text-type)) (get data 'value))
	((table? data)
	 (let ((new (frame-create #f)))
	   (do-choices (key (getkeys data))
	     (let ((slotid (if (string? key) (intern (upcase key)) key))
		   (value (get data key)))
	       (cond ((equal? key 'type) (store! new 'type (olib/ref (olib/import value))))
		     ((string? value) (store! new slotid value))
		     ((vector? value)
		      (let ((converted (map olib/import value)))
			(store! new (intern (stringout "%" slotid))
				converted)
			(add! new slotid (elts converted))))
		     (else
		      (store! new key (olib/import value))))
	       (when (equal? key 'authors)
		 (store! new 'author
			 (choice (get (get new 'authors) 'author)
				 (reject (get new 'authors) 'author))))))
	   (when cache (extindex-cacheadd! olib (get new 'key) new))
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

(define (olib-query args (cache #f))
  (let* ((query (frame-create #f))
	 (results {}))
    (do ((scan args (cddr scan)))
	((null? scan))
      (set! query (convert-query (car scan) (cadr scan) query)))
    (do-choices query
      (let* ((url (if cache
		      (scripturl "http://openlibrary.org/query.json"
			"query" (->json query) "*" "")
		      (scripturl "http://openlibrary.org/query.json"
			"query" (->json query))))
	     (req (debug%watch (urlget url) query)))
	(if (test req 'response 200)
	    (set+! results
		   (let ((result (olib/parse (get req '%content))))
		     (if (vector? result)
			 (olib/import (elts result) cache)
			 (olib/import result cache)))))))
    results))

(define (convert-query slot value query)
  (if (eq? slot 'authors)
      (store! query 'authors `#[author #[key ,(olib-key value)]])
      (if (olib? value)
	  (store! query slot (olib-key value))
	  (store! query slot value)))
  query)

(define (olib/query . args) (olib-query args))
(define (olib/query+ . args) (olib-query args #t))
      
;;; Getting REFS from bibliographic information

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


;;; Getting covers

(define (olib/image ref (size "S"))
  (string-append "http://covers.openlibrary.org/"
		 (if (has-prefix (olib-key ref) "/authors/") "a" "b")
		 "/olid/" (olib/id ref) "-" size ".jpg"))


;;; The olib itself

(define olib (make-extindex "openlibrary" olib/fetch))
