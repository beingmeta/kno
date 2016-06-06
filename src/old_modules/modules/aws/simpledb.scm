;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module '{aws/simpledb})

;;; Accessing Amazon Simple DB

(module-export! '{sdb/signature sdb/uri sdb/op sdb/opxml})
(module-export! '{lisp->sdb sdb->lisp})
(module-export! '{sdb/put sdb/fetch
		  sdb/addvalues sdb/dropvalues sdb/setvalues})
(module-export! '{sdb/cached? sdb/cached sdb/exists?})
(module-export! '{sdb/create sdb/probe sdb/defined?
		  sdb/get sdb/add! sdb/drop! sdb/set!})
(module-export! '{sdb/domains sdb/domains/new
		  sdb/domain/info sdb/domain/drop!})

(use-module '{aws fdweb texttools logger fdweb varconfig jsonout rulesets})

(define-init %loglevel %notice%)

(define-init default-domain #f)
(varconfig! simpledb:domain default-domain)

(define-init sdb:key #f)
(define-init sdb:secret #f)
(varconfig! simpledb:key sdb:key)
(varconfig! simpledb:secret sdb:secret)

(define-init use-json #t)
(varconfig! sdb:json use-json)

(define-init simpledb-domains (make-hashtable))

(define (get-ptable params (integrate #f))
  "This returns a parameter table from a list of arguments."
  (do ((p params (cddr p)))
      ((or (not (pair? p)) (null? (cdr p)))
       (if (not (pair? p)) (frame-create #f)
	   (if (table? (car p)) (car p)
	       (frame-create #f))))))

(define (just-result xml)
  (if (and (pair? xml) (pair? (cdr xml))
	   (string-starts-with? (car xml) #((spaces*) "<?"))
	   (null? (cdr (cdr xml))))
      (cadr xml)
      xml))

;;; Simple DB stuff

(define simpledb-base-uri "https://sdb.amazonaws.com/?")

(define sdb-common-params
  '("Version" "2007-11-07" "SignatureVersion" "1"))

(define (sdb/signature ptable)
  (let ((desc (stringout
		(doseq (key (lexsorted (getkeys ptable) downcase))
		  (printout key
			    (do-choices (v (get ptable key)) (printout v)))))))
    (hmac-sha1 (or sdb:secret aws:secret) desc)))
(define (sdb/signature0 action timestamp)
  (let ((desc (stringout action (get timestamp 'iso))))
    (hmac-sha1 (or sdb:secret aws:secret) desc)))

(define (sdb/uri . params)
  (let ((timestamp (gmtimestamp 'seconds))
	;; We can specify parameters as the last (odd) argument
	(ptable (get-ptable params)))
    (do ((p params (cddr p)))
	((or (null? p)  (null? (cdr p))))
      (add! ptable (car p) (cadr p)))
    (do ((p sdb-common-params (cddr p)))
	((null? p))
      (unless (test ptable (car p))
	(store! ptable (car p) (cadr p))))
    (store! ptable "Timestamp" (get timestamp 'iso))
    (store! ptable "AWSAccessKeyId" (or sdb:key aws:key))
    (stringout simpledb-base-uri
      (do-choices (key (getkeys ptable) i)
	(printout (if (> i 0) "&")
	  key "=" (uriencode (stringout (get ptable key))))) 
      "&Signature="
      (uriencode (packet->base64 (sdb/signature ptable))))))

(define (sdb/op action . params)
  (let ((uri (apply sdb/uri "Action" action params)))
    (%debug "Fetching " uri)
    (urlget uri)))
(define (sdb/opxml action . params)
  (let ((uri (apply sdb/uri "Action" action params)))
    (%debug "Fetching XML for " uri)
    (let* ((response (urlget uri))
	   (content (get response '%content)))
      (%debug "Fetched XML for " uri " was " response)
      (if (test response 'content-type)
	  (xmlparse content 'data)
	  (if (packet? content)
	      (error "SimpleDB error was " (packet->string content))
	      (error "SimpleDB error was " content))))))

(comment (sdb/op "CreateDomain" "DomainName" "bricotags"))

;;; Converting values

(define tolisp-conversions '())
(define fromlisp-conversions '())

(ruleconfig! sdb:tolisp tolisp-conversions)
(ruleconfig! sdb:fromlisp fromlisp-conversions)

(define (sdb->lisp string (attrib #f))
  (if (has-prefix string "!")
      (let ((tcode (and (> (length string) 1) (elt string 1))))
	(cond ((has-prefix string "!@") (parse-arg (slice string 1)))
	      ((has-prefix string "!!") (slice string 1))
	      ((has-prefix string "!d") (timestamp (slice string 2)))
	      ((has-prefix string "!U") (getuuid (slice string 2)))
	      ((has-prefix string "!B") #t)
	      ((has-prefix string "!b") #f)
	      ((has-prefix string "!E") (fail))
	      ((has-prefix string "!i") (string->number (slice string 2)))
	      ((has-prefix string "!I") (- (string->number (slice string 2))))
	      ((has-prefix string "!P") (base16->packet (slice string 2)))
	      ((has-prefix string "!:") (string->lisp (slice string 2)))
	      ((has-prefix string "!#") (elts (jsonparse (slice string 2))))
	      ((has-prefix string "!C") (elts (jsonparse (slice string 2))))
	      ((has-prefix string "!$")
	       (let ((v (jsonparse (slice string 2))))
		 (if (vector? v) (map sdb->lisp v)
		     (if (slotmap? v) (sdb->lisp/table v)
			 v))))
	      (else (try (tryseq (method tolisp-conversions)
			   ((cdr method) string attrib))
			 (string->lisp (slice string 1))))))
      string))
(define sdb/tolisp sdb->lisp)

(defambda (lisp->sdb object (json use-json) (padlen 0))
  (cond ((fail? object) "!E")
	((ambiguous? object)
	 (if json
	     (stringout "!C" (jsonout (choice->vector object)))
	     (stringout "!:" (write object))))
	((oid? object)
	 (glom "!" (oid->string object)))
	((string? object)
	 (if (has-prefix object "!")
	     (glom "!" object)
	     object))
	((timestamp? object)
	 (stringout "!d" (get (get object 'gmt) 'iso)))
	((uuid? object) (glom "!U" (uuid->string object)))
	((packet? object) (glom "!P" (packet->base16 object)))
	((integer? object)
	 (let ((irep (number->string (abs object))))
	   (stringout "!" (if (>= object 0) "i" "I")
		      (dotimes (i (- padlen (length irep))) (printout "0"))
		      irep)))
	((eq? object #t) "!B")
	((not object) "!b")
	((symbol? object) (stringout "!:" (symbol->string object)))
	(else (try (tryseq (method fromlisp-conversions)
		     ((cdr method) object))
		   (if (or (vector? object) (slotmap? object))
		       (if json
			   (stringout "!$" (jsonout (lisp->sdb/table object)))
			   (stringout "!:" (write object)))
		       (if json
			   (stringout "!$" (jsonout object))
			   (stringout "!:" (write object))))))))
(define sdb/fromlisp lisp->sdb)

;;;; Converting tables

(define (sdb->lisp/table v)
  (let ((result #[]) (keyval #f))
    (do-choices (key (getkeys v))
      (set! keyval (get v key))
      (if (vector? keyval)
	  (add! result (sdb->lisp key) (sdb->lisp (elts keyval)))
	  (add! result (sdb->lisp key) (sdb->lisp keyval))))
    result))

(define (lisp->sdb/table v)
  (let ((result #[]))
    (do-choices (key (getkeys v))
      (store! result (sdb->lisp key) (sdb->lisp (get v key))))
    result))

;;;; Low level PUT/FETCH/ETC

(define (sdb/put domain item . keyvals)
  (let ((ptable (get-ptable keyvals)))
    (do ((kv keyvals (cddr kv))
	 (i 0 (1+ i)))
	((or (not (pair? kv)) (not (pair? (cdr kv))))
	 (sdb/op "PutAttributes"
		 "DomainName" domain
		 "ItemName" (lisp->sdb item)
		 ptable))
      (store! ptable (stringout "Attribute." i ".Name")
	      (unparse-arg (car kv)))
      (store! ptable (stringout "Attribute." i ".Value")
	      (lisp->sdb (cadr kv))))))

(define (sdb/fetch domain item (key #f) (raw #f))
  (let* ((key (and key (lisp->sdb key)))
	 (xml (if key
		  (sdb/opxml "GetAttributes"
			     "DomainName" domain
			     "ItemName" (unparse-arg item)
			     "AttributeName" key)
		  (sdb/opxml "GetAttributes"
			     "DomainName" domain
			     "ItemName" (unparse-arg item))))
	 (result (get (get (xmlget xml 'getattributesresponse)
			   'getattributesresult)
		      'attribute)))
    (debug%watch "SDB/FETCH" result xml)
    (if key
	(if raw
	    (get (pick result 'name key) 'value)
	    (sdb->lisp (get (pick result 'name key) 'value)))
	(let ((table (frame-create #f)))
	  (do-choices (r result)
	    (if raw
		(add! table (get r 'name) (get r 'value))
		(add! table
		      (sdb->lisp (get r 'name))
		      (sdb->lisp (get r 'value) (get r 'name)))))
	  table))))

(defambda (sdb/addvalues domain item slotids values)
  (when (and (exists? slotids) (exists? values))
    (do-choices item
      (let ((ptable `#["DomainName" ,domain
		       "ItemName" ,(unparse-arg item)])
	    (count 0))
	(do-choices (key (lisp->sdb slotids))
	  (do-choices (value (lisp->sdb values))
	    (store! ptable (stringout "Attribute." count ".Name")
		    key)
	    (store! ptable (stringout "Attribute." count ".Value")
		    value)
	    (set! count (1+ count))))
	(%debug "Doing PutAttributes on " ptable)
	(sdb/op "PutAttributes" ptable)))))

(defambda (sdb/setvalues domain item slotids values)
  (when (and (exists? slotids) (exists? values))
    (do-choices item
      (let ((ptable `#["DomainName" ,domain
		       "ItemName" ,(unparse-arg item)])
	    (count 0))
	(do-choices (key (lisp->sdb slotids))
	  (do-choices (value (lisp->sdb values))
	    (store! ptable (stringout "Attribute." count ".Name")
		    key)
	    (store! ptable (stringout "Attribute." count ".Value")
		    value)
	    (store! ptable (stringout "Attribute." count ".Replace")
		    "true")
	    (set! count (1+ count))))
	(%debug "Doing PutAttributes/replace on " ptable)
	(sdb/op "PutAttributes" ptable)))))

(defambda (sdb/dropvalues domain item slotids values)
  (when (and (exists? slotids) (exists? values))
    (do-choices item
      (let ((ptable `#["DomainName" ,domain
		       "ItemName" ,(lisp->sdb item)])
	    (count 0))
	(do-choices (key (lisp->sdb slotids))
	  (do-choices (value (lisp->sdb values))
	    (store! ptable (stringout "Attribute." count ".Name")
		    key)
	    (store! ptable (stringout "Attribute." count ".Value")
		    value)
	    (set! count (1+ count))))
	(%debug "Doing DeleteAttributes on " ptable)
	(sdb/op "DeleteAttributes" ptable)))))

;;;; Domain Metadata

(define (sdb/domain/info name)
  (let* ((result (just-result (sdb/opxml "DomainMetadata" "DomainName" name)))
	 (core (tryif (and (slotmap? result)
			   (test result 'domainmetadataresult))
		 (get result 'domainmetadataresult))))
    (tryif (exists? core)
      `#[domain ,name
	 timestamp ,(gmtimestamp (string->lisp (get core 'timestamp)))
	 itemcount ,(string->lisp (get core 'itemcount))
	 itemnamesize ,(string->lisp (get core 'ITEMNAMESSIZEBYTES))
	 attribcount ,(string->lisp (get core 'ATTRIBUTENAMECOUNT))
	 attribsize  ,(string->lisp (get core 'ATTRIBUTENAMESSIZEBYTES))
	 valuecount ,(string->lisp (get core 'ATTRIBUTEVALUECOUNT))
	 valuesize ,(string->lisp (get core 'ATTRIBUTEVALUESSIZEBYTES))])))

;;; Using an item cache

(define-init sdb-cache (make-hashtable))
(define default-domain "default")

(define (sdb/cached? item (domain (req/get '_simpledb default-domain)))
  (test sdb-cache (vector item domain)))

(define (sdb/cached item (domain (req/get '_simpledb default-domain)))
  (try (get sdb-cache (vector item domain))
       (onerror
	(let ((fetched (sdb/fetch domain item)))
	  (store! sdb-cache (vector item domain) fetched)
	  fetched)
	(lambda (ex)
	  (warning "Error accessing SIMPLEDB " domain
		   " for " item ": " ex)
	  (fail)))))

(define (sdb/exists? item (domain (req/get '_simpledb default-domain)))
  (if (pair? item)
      (begin (set! domain (cdr item))
	(set! item (car item)))
      (if (and (table? item) (test item 'id) (test item 'domain))
	  (begin (set! domain (get item 'domain))
	    (set! item (get item 'id)))))
  (exists? (getkeys (sdb/fetch domain item))))

;;; CREATE

(define (sdb/create item (domain (req/get '_simpledb default-domain)))
  (if (pair? item)
      (begin (set! domain (cdr item))
	(set! item (car item)))
      (if (and (table? item) (test item 'id) (test item 'domain))
	  (begin (set! domain (get item 'domain))
	    (set! item (get item 'id)))))
  (try (get sdb-cache (vector item domain))
       (sdb/cache-create item domain)))

(define sdb/cache-create
  (slambda (item domain)
    (try (get sdb-cache (vector item domain))
	 (let ((fetched (sdb/fetch domain item)))
	   (if (and (exists? fetched)
		    (test fetched 'id item)
		    (test fetched 'domain domain))
	       fetched
	       (let ((fresh (frame-create #f 'id item 'domain domain)))
		 (sdb/setvalues domain item 'id item)
		 (sdb/setvalues domain item 'domain domain)
		 (store! sdb-cache (vector item domain) fresh)
		 fresh))))))

(define (sdb/probe item (domain (req/get '_simpledb default-domain)))
  (if (pair? item)
      (begin (set! domain (cdr item))
	(set! item (car item)))
      (if (and (table? item) (test item 'id) (test item 'domain))
	  (begin (set! domain (get item 'domain))
	    (set! item (get item 'id)))))
  (let ((fetched (try (get sdb-cache (vector item domain))
		      (sdb/fetch domain item))))
    (if (and (exists? fetched)
	     (test fetched 'id item)
	     (test fetched 'domain domain))
	fetched
	(fail))))

(define (sdb/defined? item (domain (req/get '_simpledb default-domain)))
  (if (pair? item)
      (begin (set! domain (cdr item))
	(set! item (car item)))
      (if (and (table? item) (test item 'id) (test item 'domain))
	  (begin (set! domain (get item 'domain))
	    (set! item (get item 'id)))))
  (or (and (test sdb-cache (vector item domain))
	   (test (get sdb-cache (vector item domain)) 'id item)
	   (test (get sdb-cache (vector item domain)) 'domain domain))
      (and (exists? (sdb/fetch domain item 'id))
	   (exists? (sdb/fetch domain item 'domain)))))

;;; GET/ADD/DROP/etc

(defambda (sdb/get item (slotid #f)
		   (domain (req/get '_simpledb default-domain)))
  "Gets values from an attribute of an item, using/updating a local cache"
  (if (pair? item)
      (begin (set! domain (cdr item))
	(set! item (car item)))
      (if (and (table? item) (test item 'id) (test item 'domain))
	  (begin (set! domain (get item 'domain))
	    (set! item (get item 'id)))))
  (if slotid
      (get (sdb/cached item domain) slotid)
      (sdb/cached item domain)))

(defambda (sdb/add! item slotid values
		    (dom (req/get '_simpledb default-domain)))
  "Adds values to an attribute of an item, updating a local cache"
  (do-choices item
    (let ((domain dom) (id item))
      (if (pair? item)
	  (begin (set! domain (cdr item))
	    (set! id (car item)))
	  (if (and (table? item) (test item 'id) (test item 'domain))
	      (begin (set! domain (get item 'domain))
		(set! id (get item 'id)))))
      (if (sdb/cached? id domain)
	  (let ((cache (try (get sdb-cache (vector id domain))
			    (sdb/cached id domain))))
	    (do-choices slotid
	      (let ((toadd (difference values (get cache slotid))))
		(loginfo "Adding " toadd " to " (write slotid) " of " item)
		(add! cache slotid toadd)
		(when (slotmap? item) (add! item slotid toadd))
		(sdb/addvalues domain id slotid toadd))))
	  (begin (sdb/addvalues domain id slotid values)
	    (when (slotmap? item) (store! item slotid values)))))))

(defambda (sdb/drop! item slotid (values #f)
		     (dom (req/get '_simpledb default-domain)))
  "Removes values from an attribute of an item, updating a local cache"
  (do-choices item
    (let ((domain dom) (id item))
      (if (pair? item)
	  (begin (set! domain (cdr item))
	    (set! id (car item)))
	  (if (and (table? item) (test item 'id) (test item 'domain))
	      (begin (set! domain (get item 'domain))
		(set! id (get item 'id)))))
      (if (sdb/cached? id domain)
	  (let ((cache (try (get sdb-cache (vector id domain))
			    (sdb/cached id domain))))
	    (do-choices slotid
	      (let ((todrop (if (and (bound? values)
				     (or (fail? values) values))
				(intersection (get cache slotid) values)
				(get cache slotid))))
		(drop! cache slotid todrop)
		(when (slotmap? item) (drop! item slotid todrop))
		(sdb/dropvalues domain id slotid todrop))))
	  (begin (sdb/dropvalues domain id slotid values)
	    (when (slotmap? item) (drop! item slotid values)))))))

(defambda (sdb/set! item slotid values
		    (dom (req/get '_simpledb default-domain)))
  "Adds values to an attribute of an item, updating a local cache"
  (do-choices item
    (let ((domain dom) (id item))
      (if (pair? item)
	  (begin (set! domain (cdr item))
	    (set! id (car item)))
	  (if (and (table? item) (test item 'id) (test item 'domain))
	      (begin (set! domain (get item 'domain))
		(set! id (get item 'id)))))
      (if (sdb/cached? id domain)
	  (let ((cached (try (get sdb-cache (vector id domain))
			     (sdb/cached id domain))))
	    (do-choices slotid
	      (store! cached slotid values)
	      (when (slotmap? item) (add! item slotid values))
	      (sdb/setvalues domain id slotid values)))
	  (begin (sdb/setvalues domain id slotid values)
	    (when (slotmap? item) (store! item slotid values)))))))
(define sdb/store! sdb/set!)

;;;; Operations on domains

(define (sdb/domains)
  (get (get (xmlget (sdb/opxml "ListDomains") 'listdomainsresponse)
	    'listdomainsresult)
       'domainname))
(define (sdb/domains/new name)
  (just-result (sdb/opxml "CreateDomain" "DomainName" name)))
(define (sdb/domains/drop! name)
  (just-result (sdb/opxml "DeleteDomain" "DomainName" name)))

;;;; CONFIG stuff

(config-def! 'sdb:domain
	     (lambda (var (val))
	       (if (bound? val)
		   (set! default-domain val)
		   default-domain)))
(config-def! 'sdb:baseuri
	     (lambda (var (val))
	       (if (bound? val)
		   (set! simpledb-base-uri val)
		   simpledb-base-uri)))
(config-def! 'sdb:cache
	     (lambda (var (val))
	       (if (bound? val)
		   (set! sdb-cache
			 (if (table? val) val
			     (if val (make-hashtable) {})))
		   sdb-cache)))

