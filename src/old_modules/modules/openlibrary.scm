;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'openlibrary)

;;; Provides access to the Open Library API

(use-module '{fdweb texttools ezrecords parsetime logger})

(module-export!
 '{olib
   olib/ref olib/import olib/fetch olib/parse olib? olib-key
   olib/query olib/q olib/q+ olib/bibref olib/id olib/key
   olib/image olib/imageurl olib/refurl
   olib/getauthor
   olib/get olib/string})

(define-init %loglevel %notice%)

(define (olib/string x)
  (if (string? x) x
      (if (and (slotmap? x) (test x 'type "/type/text")) (get x 'value)
	  x)))

(defrecord olib key)

(define olib-refs (make-hashtable))

(define newolibref
  (slambda (key)
    (try (get olib-refs key)
	 (let ((new (cons-olib key)))
	   (store! olib-refs key new)
	   new))))

(define olib-ref-pattern
  #("/" {"works/" "authors/" "books/"} "OL" (isdigit+) {"W" "A" "M"}))

(define (olib/ref key)
  (if (olib? key) key
      (if (string? key)
	  (try (get olib-refs key)
	       (if (has-prefix key "http://openlibrary.org/")
		   (olib/ref (try (gather olib-ref-pattern key)
				  (subseq key 22)))
		   (if (has-prefix key "DUP") (fail)
		       (cond ((has-prefix key "/") (newolibref key))
			     ((has-suffix key "W")
			      (newolibref (stringout "/works/" key)))
			     ((has-suffix key "A")
			      (newolibref (stringout "/authors/" key)))
			     ((has-suffix key "M")
			      (newolibref (stringout "/books/" key)))
			     (else (newolibref
				    (try (gather olib-ref-pattern key)
					 key)))))))
	  (if (and (table? key) (test key 'key))
	      (olib/ref (get key 'key))
	      (fail)))))

(define (olib/id ref)
  (subseq (olib-key ref) (1+ (position #\/  (olib-key ref) 1))))
(define (olib/key ref)
  (if (olib? ref) (olib-key ref)
      (if (string? ref) ref
	  (error "Not an Open Library reference"))))

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
			 (choice (get (pick (get new 'authors) table?) 'author)
				 (reject (pick (get new 'authors) table?) 'author)
				 (reject (get new 'authors) table?))))))
	   (when cache (extindex-cacheadd! olib (get new 'key) new))
	   new))
	(else data)))

(define redirect #%(OLIB "/type/redirect"))

(define (olib/fetch ref)
  (if (string? ref)
      (let* ((url (stringout
		    "http://openlibrary.org/"
		    (if (has-prefix ref "/") (subseq ref 1)
			(if (not (position #\/ ref))
			    (stringout
			      (if (has-suffix ref "M") "books/"
				  (if (has-suffix ref "A")
				      "authors/"
				      (if (has-suffix ref "W")
					  "works/"
					  "")))
			      ref)))
		    ".json"))
	     (r (urlget url)))
	(if (test r 'response 200)
	    (let ((parsed (jsonparse (get r '%content))))
	      (if (equal? (get (get parsed 'type) 'key) "/type/redirect")
		  (olib/fetch (get parsed 'location))
		  (olib/import parsed)))
	    (error "Bad server response to " (write url) "\n\t" r)))
      (if (olib? ref)
	  (olib/fetch (olib-key ref))
	  (error "Invalid OLIB (Open Library) value"))))

(define (olib/parse string)
  (if (search "callback(" string)
      (olib/import (jsonparse (subseq string (search "callback(" string) -1)))
      (olib/import (jsonparse string))))

(define (olib/get ref slotid) (get (or (get olib ref) {}) slotid))

;;;; The query interfaces

(define (nodups v)
  (if (or (and (string? v) (has-prefix v "DUP/"))
	  (and (table? v) (test v 'key)
	       (has-prefix (get v 'key) "DUP/")))
      (fail)
      v))

(define (olib-query args (cache #f) (opts #[]))
  (let* ((query (frame-create #f))
	 (results {}))
    (do ((scan args (cddr scan)))
	((null? scan))
      (set! query (convert-query (car scan) (cadr scan) query)))
    (do-choices query
      (let* ((url (scripturl "http://openlibrary.org/query.json"
		      "query" (->json query)
		      "offset" (getopt opts 'offset 0)
		      "limit" (getopt opts 'limit 1000)))
	     (req (debug%watch (urlget url) query)))
	(if (test req 'response 200)
	    (set+! results
		   (let ((result (olib/parse (get req '%content))))
		     (if (vector? result)
			 (olib/import (nodups (elts result)) cache)
			 (olib/import (nodups result) cache))))
	    (error "Bad server response to " (write url) ":\n\t" req))))
    results))
(define (olib-query args (cache #f) (opts #[]))
  (let* ((query (frame-create #f))
	 (results {}))
    (do ((scan args (cddr scan)))
	((null? scan))
      (set! query (convert-query (car scan) (cadr scan) query)))
    (store! query 'offset (getopt opts 'offset 0))
    (store! query 'limit (getopt opts 'limit 1000))
    (do-choices query
      (let* ((url (scripturl "http://openlibrary.org/query.json"
		      ;; These need to be passed in the query object
		      ;; "offset" (getopt opts 'offset 0)
		      ;; "limit" (getopt opts 'limit 1000)
		      "query" (->json query)))
	     (req (debug%watch (urlget url) query)))
	(if (test req 'response 200)
	    (set+! results
		   (let ((result (olib/parse (get req '%content))))
		     (if (vector? result)
			 (olib/import (nodups (elts result)) cache)
			 (olib/import (nodups result) cache))))
	    (error "Bad server response to " (write url) ":\n\t" req))))
    results))

(define (convert-query slot value query)
  (if (eq? slot 'authors)
      (store! query 'authors `#[author #[key ,(olib-key value)]])
      (if (olib? value)
	  (store! query slot (olib-key value))
	  (store! query slot value)))
  query)

(define (olib/query . args)
  (if (even? (length args)) (olib-query args)
      (olib-query (cons 'type args))))

(define olib-cache (make-hashtable))

(define (olib/q . args)
  (if (even? (length args))
      (cachecall olib-cache olib-query args #t)
      (cachecall olib-cache olib-query (cons 'type args) #t)))

(define (olib/q+ opts . args)
  (if (even? (length args))
      (cachecall olib-cache olib-query args #t opts)
      (cachecall olib-cache olib-query (cons 'type args) #t opts)))
      
;;; Getting REFS from bibliographic information

(define (olib/bibref type (val #f))
  (let* ((bibkey (if val (stringout type ":" val) type))
	 (uri (stringout "http://openlibrary.org/api/books?bibkeys="
			 bibkey "&details=true&callback=callback"))
	 (req (urlget uri)))
    (if (test req 'response 200)
	(let* ((content (get req '%content))
	       (cbstart (search "callback(" content))
	       (parsed (and cbstart (jsonparse (subseq content (+ cbstart 9))))))
	  (tryif parsed
	    (olib/ref (get (get (get parsed bibkey) "details") "key"))))
	(error "Bad server response to " (write uri) ":\n\t" req))))


;;; Getting covers

(define (olib/image ref (size "S"))
  (if (string? ref) (set! ref (olib/ref ref)))
  (for-choices (id  (olib/get ref '{covers photos}))
    (stringout "http://covers.openlibrary.org/"
	       (if (has-prefix (olib-key ref) "/authors/") "a" "b")
	       "/id/" id "-" size ".jpg")))

(define (olib/imageurl kind id size)
  (stringout "http://covers.openlibrary.org/"
    kind "/id/" id "-" size ".jpg"))

(define (olib/refurl ref)
  (if (string? ref) (set! ref (olib/ref ref)))
  (stringout "http://openlibrary.org" (olib-key ref)))


;;; Convenience functions

(define (olib/getauthor name)
  (olib/q "/type/author" '{name alternate_names} name))


;;; The olib itself

(define olib (make-extindex "openlibrary" olib/fetch))
