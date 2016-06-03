;;; -*- Mode: Scheme; Character-encoding: utf-8;  -*-

(in-module 'rdf)

(use-module '{fdweb xhtml xhtml/brico texttools reflection domutils})
(use-module '{brico brico/lookup brico/dterms})

(module-export! '->rdf)

(define-init prefixes (make-hashtable))
(define-init urls (make-hashtable))
(store! urls brico-pool "http://bricobase.net/rdf/%%%addr%%%")
(define default-uri "http://bricobase.net/rdf/%%%addr%%%")

(define (label->prefix s)
  (and s
       (if (position #\. s)
	   (slice s 0 (position #\. s))
	   s)))

(define (get-rdf-id g)
  (let* ((pool (getpool g))
	 (prefix (try (get prefixes pool)
		      (label->prefix (pool-label (getpool g)))
		      #f)))
    (if prefix (glom prefix "_" (number->string (oid-offset g) 16))
	(glom "REF_" (number->string (oid-hi g) 16) "_"
	  (number->string (oid-lo g) 16)))))

(define (get-rdf-ref g (local #f))
  (if (and local (hashset-get local g))
      (glom "#" (get-rdf-id g))
      (let* ((pool (getpool g))
	     (uri (try (get urls pool) default-uri)))
	(cond ((procedure? uri) (uri g))
	      ((not (string? uri)) (error "BAD URI spec" uri))
	      ((search "%%%" uri)
	       (string-subst* uri
		 "%%%hi%%%" (number->string (oid-hi g) 16)
		 "%%%lo%%%" (number->string (oid-lo g) 16)
		 "%%%addr%%%"
		 (glom (number->string (oid-hi g) 16) "/"
		   (number->string (oid-lo g) 16))))
	      (else (glom uri
		      (and (not (has-suffix uri "/")) "/")
		      (number->string (oid-hi g) 16) "/"
		      (number->string (oid-lo g) 16)))))))

(define (concept->rdf concept
		      (local (req/get 'localrefs #f))
		      (slots (req/get 'rdfslots #[]))
		      (languages (get-languages)))
  (xmlblock "Description" ((id (get-rdf-id concept)))
    "\n\t" (xmlelt 'type 'resource "rdfs:Class")
    (do-choices (g (get concept @?genls))
      (xmlout "\n\t")
      (xmlelt "rdfs:subclassOf"
	'resource (get-rdf-ref g local)))
    (do-choices (lang languages)
      (do-choices (word (get concept lang))
	(xmlout "\n\t")
	(xmlblock rdfs:label ((lang (get lang 'iso639/1)))
	  word))
      (do-choices (gloss (get-gloss concept lang))
	(xmlout "\n\t")
	(xmlblock rdfs:comment ((lang (get lang 'iso639/1)))
	  gloss)))
    "\n"
    (do-choices (slot (getkeys slots))
      (let* ((value (if (procedure? slot)
			(slot concept)
			(get concept slot)))
	     (label (and (exists? value) (get slots slot)))
	     (fakenode `#[%xmltag ,label %content ()]))
	(when (exists? value)
	  (if (ambiguous? value)
	      (xmlout "\n\t" (xmlopen fakenode)
		"\n\t\t"
		(xmlblock "Set" ()
		  (do-choices (v value)
		    (xmlout "\n\t\t  ")
		    (if (oid? v)
			(xmlelt "li" 'resource (get-rdf-ref v local))
			(xmlblock "li" () v)))
		  (xmlout "\n"))
		"\n\t" (void (xmlclose fakenode)))
	      (xmlout
		(if (oid? value)
		    (xmlelt label 'resource (get-rdf-ref value local))
		    (begin
		      (xmlopen fakenode) (xmlout value)
		      (void (xmlclose fakenode))))
		"\n\t")))))
    "\n"))

(define (individual->rdf concept
			 (local (req/get 'localrefs #f))
			 (slots (req/get 'rdfslots #[]))
			 (languages (get-languages)))
  (xmlblock "Description" ((id (get-rdf-id concept)))
    (do-choices (g (get concept @?implies))
      (xmlout "\n\t")
      (xmlelt "type" 'resource (get-rdf-ref g)))
    (do-choices (lang languages)
      (do-choices (word (get concept lang))
	(xmlout "\n\t")
	(xmlblock rdfs:label ((lang (get lang 'iso639/1)))
	  word)))
    "\n"
    (do-choices (slot (getkeys slots))
      (let* ((value (if (procedure? slot)
			(slot concept)
			(get concept slot)))
	     (label (and (exists? value) (get slots slot)))
	     (fakenode `#[%xmltag ,label %content ()]))
	(when (exists? value)
	  (if (ambiguous? value)
	      (xmlout "\n\t" (xmlopen fakenode)
		"\n\t\t"
		(xmlblock "Set" ()
		  (do-choices (v value)
		    (xmlout "\n\t\t  ")
		    (if (oid? v)
			(xmlelt "li" 'resource (get-rdf-ref v local))
			(xmlblock "li" () v)))
		  (xmlout "\n"))
		"\n\t" (void (xmlclose fakenode)))
	      (xmlout
		(if (oid? value)
		    (xmlelt label 'resource (get-rdf-ref value local))
		    (begin
		      (xmlopen fakenode) (xmlout value)
		      (void (xmlclose fakenode))))
		"\n\t")))))
    "\n"))

(defambda (->rdf f (opts #[]))
  (let* ((local (choice->hashset f))
	 (slots (getopt opts 'slots #[]))
	 (namespaces (getopt opts 'namespaces {}))
	 (fakenode #[%xmltag "RDF" %content ()])
	 (languages (getopt opts 'languages (get-languages))))
    (dom/set! fakenode "xmlns"  "http://www.w3.org/TR/REC-rdf-syntax#")
    (dom/set! fakenode "xmlns:rdfs"  "http://www.w3.org/TR/REC-rdf-syntax#")
    (do-choices (prefix (getkeys namespaces))
      (dom/set! fakenode
		(glom "xmlns:" prefix)
		(get namespaces prefix)))
    (xmlout
      (xmlopen fakenode)
      (do-choices (concept f)
	(unless (fail? (oid-value concept))
	  (xmlout"\n"
	    (cond ((test concept 'type 'individual)
		   (individual->rdf concept local slots languages))
		  (else (concept->rdf concept local slots languages)))
	    "\n")))
      (void (xmlclose fakenode)))))


