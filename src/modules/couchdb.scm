;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2010 beingmeta, inc. All rights reserved

(in-module 'couchdb)

(define version "$Id: glosses.scm 5530 2010-09-13 16:53:19Z haase $")
(define revision "$Revision: 5530 $")

(use-module '{fdweb ezrecords extoids})
(use-module '{texttools logger})

(module-export! '{checkgloss newgloss getgloss glossdb/moveid})

(define-init %loglevel %notice!)
;;(define %loglevel %debug!)

(define-init couchdbs (make-hashtable))

(defrecord couchdb url (map #[]) (curlopts #[]) (conns {}) (views (make-hashtable)))
(defrecord couchview db design view path (options {}))

(define (couchdb url)
  (try (get couchdbs url) (new-couchdb url)))
(define (couchdef! def db)
  (store! couchdbs def
	  (if (string? db) (couchdb db)
	      (if (couchdb? db) db
		  (error "Not a valid DB" db)))))
(module-export! '{couchdb couchdb? couchdb-url couchdb-map couchdb-views couchdef!})
(module-export! '{couchview-db couchview-options})

(defslambda (new-couchdb url)
  (try (get couchdbs url)
       (let ((new (cons-couchdb url #[])))
	 (store! couchdbs url new)
	 new)))

(define (couchdb/add-map! db . args)
  (let* ((db (if (string? db) (couchdb db) db))
	 (map (couchdb-map db)))
    (when (odd? (length args))
      (do-choices (key (getkeys (car args)))
	(store! map key (get (car args) key)))
      (set! args (cdr args)))
    (do ((scan args (cddr scan)))
	((null? scan))
      (store! map (car scan) (cadr scan)))))
(module-export! 'couchdb/add-map!)

(define (couchdb/set-curlopts! db . args)
  (let* ((db (if (string? db) (couchdb db) db))
	 (map (couchdb-curlopts db)))
    (when (odd? (length args))
      (do-choices (key (getkeys (car args)))
	(store! map key (get (car args) key)))
      (set! args (cdr args)))
    (do ((scan args (cddr scan)))
	((null? scan))
      (store! map (car scan) (cadr scan)))))
(module-export! 'couchdb/set-curlopts!)

(define (couchdb/req db path (options #f) . args)
  (let* ((path (mkpath (couchdb-url db) path))
	 (call (if (null? args) path (apply scripturl path args)))
	 (resp (urlget call (or options (couchdb-curlopts db)))))
    (debug%watch "COUCHDB/REQ" path call options)
    (debug%watch "COUCHDB/REQ" resp)
    (tryif (and (test resp 'response) (>= (get resp 'response) 200)
		(< (get resp 'response) 300))
      (jsonparse (get resp '%content) 56 (couchdb-map db)))))

(define (couchdb/get db id (opts #f))
  (if (couchdb? db)
      (couchdb/req db (if (oid? id)
			  (stringout "@" (number->string (oid-addr id) 16))
			  (if (uuid? id) (uuid->string id)
			      (if (string? id) (uriencode id)
				  (stringout
				    ":" (uriencode (lisp->string id))))))
		   opts)
      (if (couchview? db)
	  (let* ((options (or opts {}))
		 (viewopts (couchview-options db))
		 (include_docs
		  (if (test options 'include_docs) (get options 'include_docs)
		      (and (test viewopts 'include_docs) (get viewopts 'include_docs))))
		 (response
		  (couchdb/req
		   (couchview-db db)
		   (scripturl (couchview-path db)
		     "key" (->json id)
		     "group"
		     (if (test options 'group)
			 (tryif (get options 'group) "true")
			 (tryif (and (test viewopts 'group) (get viewopts 'group)) "true"))
		     "group_level"
		     (try (get options 'group_level) (get viewopts 'group_level))
		     "include_docs" (tryif include_docs "true")
		     "descending"
		     (if (test options 'descending)
			 (tryif (get options 'descending) "true")
			 (tryif (and (test viewopts 'descending)
				     (get viewopts 'descending))
			   "true"))
		     "skip" (get options 'skip) "limit" (get options 'limit))
		   opts))
		 (results {}))
	    (doseq (row (get response 'rows))
	      (set+! results (if include_docs (get row 'doc) (get row 'value))))
	    results)
	  (error "Invalid arg" db))))

(define (convert-field field)
  (if (symbol? field) (downcase (symbol->string field))
      (if (string? field) field
	  (unparse-arg field))))

(define (convert-value value)
  (if (timestamp? value) (get value 'tick)
      (if (oid? value) (oid->string value)
	  (if (string? value) value
	      (unparse-arg value)))))

(define (->idstring id)
  (if (string? id) (uriencode id)
      (if (oid? id) (stringout "@" (number->string (oid-addr id) 16))
	  (if (uuid? id) (uuid->string id)
	      (error "Bad id value")))))

(define (couchdb/save! db value (id #f) (flags 56) (options #f))
  (let*  ((idstring (if (not id)
			(->idstring (try (get value '_id) (getuuid)))
			(if (oid? id)
			    (stringout "@" (number->string (oid-addr id) 16))
			    (if (uuid? id) (uuid->string id)
				(if (string? id) (uriencode id)
				    (stringout
				      ":" (uriencode (lisp->string id))))))))
	  (json (->json value flags))
	  (r (urlput (mkpath (couchdb-url db) idstring) json
		     (or options (couchdb-curlopts db))))
	  (httpcode (get r 'response)))
    (debug%watch "COUCHDB/SAVE!" r value)
    (or (and (exists? httpcode) (>= httpcode 200) (< httpcode 300))
	(begin (notice%watch "COUCHDB/SAVE! failed" r db value (->json value))
	       #f))))

(define (couchdb/mutate! db id mutate)
  (let ((success #f) (cur (couchdb/get db id)))
    (until success
      (mutate cur)
      (set! success (couchdb/save! db cur))
      (unless success (set! cur (couchdb/get db id))))))
  
(define (couchdb/delete! db id (rev #f))
  (let ((rev (or rev (get (couchdb/get db id) '_rev))))
    (couchdb/req db (if (oid? id)
			(stringout "@" (number->string (oid-addr id) 16))
			(if (uuid? id) (uuid->string id)
			    (if (string? id) (uriencode id)
				(stringout
				  ":" (uriencode (lisp->string id))))))
		 #[METHOD DELETE]
		 "rev" rev)))

(define (couchdb/store! db id slotid value)
  (let ((success #f)
	(id (if (slotmap? id) (get id '_id) id))
	(cur (if (slotmap? id) id (couchdb/get db id))))
    (until success
      (if (identical? (get cur slotid) value)
	  (set! success #t)
	  (begin (store! cur slotid value)
		 (set! success (couchdb/save! db cur))
		 (unless success (set! cur (couchdb/get db id))))))))
(define (couchdb/push! db id slotid value)
  (let ((success #f)
	(id (if (slotmap? id) (get id '_id) id))
	(cur (if (slotmap? id) id (couchdb/get db id))))
    (until success
      (if (and (test cur slotid) (vector? (get cur slotid))
	       (position value (get cur slotid)))
	  (set! success #t)
	  (begin (store! cur slotid
			 (if (test cur slotid)
			     (if (vector? (get cur slotid))
				 (append (vector value) (get cur slotid))
				 (vector value (get cur slotid)))
			     (vector value)))
		 (set! success (couchdb/save! db cur))
		 (unless success (set! cur (couchdb/get db id))))))))
(define (couchdb/add! db id slotid value)
  (let ((success #f)
	(id (if (slotmap? id) (get id '_id) id))
	(cur (if (slotmap? id) id (couchdb/get db id))))
    (until success
      (if (test cur slotid value)
	  (set! success #t)
	  (begin (add! cur slotid value)
		 (set! success (couchdb/save! db cur))
		 (unless success (set! cur (couchdb/get db id))))))))
(define (couchdb/drop! db id slotid value)
  (let ((success #f)
	(id (if (slotmap? id) (get id '_id) id))
	(cur (if (slotmap? id) id (couchdb/get db id))))
    (until success
      (if (test cur slotid value)
	  (begin (drop! cur slotid value)
		 (set! success (couchdb/save! db cur))
		 (unless success (set! cur (couchdb/get db id))))
	  (set! success #t)))))

(module-export!
 '{couchdb/req
   couchdb/get couchdb/save! couchdb/mutate! couchdb/delete!
   couchdb/store! couchdb/add! couchdb/drop! couchdb/push!})

;;; CDB (storing OIDs in COUCHDB)

(define (cdb/get id (options #f) (db) (pool))
  (default! pool (getpool id))
  (default! db (get couchdbs pool))
  (let ((value (couchdb/req db
			    (if (oid? id)
				(stringout "@" (number->string (oid-addr id) 16))
				(if (uuid? id) (uuid->string id)
				    (if (string? id) (uriencode id)
					(stringout
					  ":" (uriencode (lisp->string id))))))
			    options)))
    (debug%watch "CDB/GET" id value)
    (extpool-cache! pool id value)
    value))

(define (cdb/mutate! id mutate (db))
  (default! db (get couchdbs (getpool id)))
  (let ((success #f) (cur (try (couchdb/get db id) (frame-create #f))))
    (until success
      (mutate cur)
      (set! success (couchdb/save! db cur))
      (unless success (set! cur (couchdb/get db id)))))
  (swapout id))

(define (cdb/store! id slotid value (db))
  (default! db (get couchdbs (getpool id)))
  (set! db (get couchdbs (getpool id)))
  (let ((noedit #f)
	(success #f)
	(id (if (slotmap? id) (get id '_id) id))
	(cur (if (slotmap? id) id (couchdb/get db id))))
    (until success
      (if (identical? (get cur slotid) value)
	  (begin (set! success #t) (set! noedit #t))
	  (begin (store! cur slotid value)
		 (set! success (couchdb/save! db cur))
		 (unless success (set! cur (couchdb/get db id))))))
    (unless noedit (swapout id))))

(define (cdb/add! id slotid value (db))
  (default! db (get couchdbs (getpool id)))
  (let ((noedit #f)
	(success #f)
	(id (if (slotmap? id) (get id '_id) id))
	(cur (if (slotmap? id) id (couchdb/get db id))))
    (until success
      (if (test cur slotid value)
	  (begin (set! success #t) (set! noedit #t))
	  (begin (if (vector? (get cur slotid))
		     (store! cur slotid (choice value (elts (get cur slotid))))
		     (add! cur slotid value))
		 (set! success (couchdb/save! db cur))
		 (unless success (set! cur (couchdb/get db id)))))
      (unless noedit (swapout id)))))

(define (cdb/drop! id slotid value (db))
  (default! db (get couchdbs (getpool id)))
  (let ((noedit #f)
	(success #f)
	(id (if (slotmap? id) (get id '_id) id))
	(cur (if (slotmap? id) id (couchdb/get db id))))
    (until success
      (if (test cur slotid value)
	  (begin (drop! cur slotid value)
		 (set! success (couchdb/save! db cur))
		 (unless success (set! cur (couchdb/get db id))))
	  (begin (set! success #t) (set! noedit #t)))
      (unless noedit (swapout id)))))

(module-export! '{cdb/get cdb/mutate! cdb/store! cdb/add! cdb/drop!})

;;; Getting views

(define (couchdb/view db design view (opts #[]))
  (if (string? db)
      (couchdb/view (couchdb db) design view)
      (try (get (couchdb-views db) (cons design view))
	   (new-view db design view opts))))
    
(defslambda (new-view db design view opts)
  (try (get (couchdb-views db) (cons design view))
       (let ((new (cons-couchview
		   db design view
		   (stringout "_design/" design "/_view/" view)
		   opts)))
	 (store! (couchdb-views db) (cons design view) new)
	 new)))
(module-export! 'couchdb/view)

(define (couchdb/table view (opts #f) (table #f))
  (let* ((options (or opts {}))
	 (viewopts (couchview-options view))
	 (include_docs
	  (if (test options 'include_docs) (get options 'include_docs)
	      (and (test viewopts 'include_docs) (get viewopts 'include_docs))))
	 (response
	  (couchdb/req
	   (couchview-db view)
	   (scripturl (couchview-path view)
	     "key" (->json (get options 'key))
	     "startkey" (->json (get options 'start))
	     "endkey" (->json (get options 'end))
	     "group" (if (test options 'group)
			 (tryif (get options 'group) "true")
			 (tryif (and (test viewopts 'group) (get viewopts 'group)) "true"))
	     "group_level" (try (get options 'group_level) (get viewopts 'group_level))
	     "include_docs" (tryif include_docs "true")
	     "descending" (if (test options 'descending)
			      (tryif (get options 'descending) "true")
			      (tryif (and (test viewopts 'descending)
					  (get viewopts 'descending))
				"true"))
	     "skip" (get options 'skip) "limit" (get options 'limit))))
	 (table (or table
		    (and opts (getopt opts 'output))
		    (make-hashtable))))
    (doseq (row (get response 'rows))
      (add! table (get row 'key) (if include_docs (get row 'doc) (get row 'value))))
    table))
(define (couchdb/list view (opts #f))
  (let* ((options (or opts {}))
	 (viewopts (couchview-options view))
	 (include_docs
	  (if (test options 'include_docs) (get options 'include_docs)
	      (and (test viewopts 'include_docs) (get viewopts 'include_docs))))
	 (response
	  (couchdb/req
	   (couchview-db view)
	   (scripturl (couchview-path view)
	     "key" (->json (get options 'key))
	     "startkey" (->json (get options 'start))
	     "endkey" (->json (get options 'end))
	     "group" (if (test options 'group)
			 (tryif (get options 'group) "true")
			 (tryif (and (test viewopts 'group) (get viewopts 'group)) "true"))
	     "group_level" (try (get options 'group_level) (get viewopts 'group_level))
	     "include_docs" (tryif include_docs "true")
	     "descending" (if (test options 'descending)
			      (tryif (get options 'descending) "true")
			      (tryif (and (test viewopts 'descending)
					  (get viewopts 'descending))
				"true"))
	     "skip" (get options 'skip) "limit" (get options 'limit))))
	 (results {}))
    (doseq (row (get response 'rows))
      (set+! results (if include_docs (get row 'doc) (get row 'value))))
    results))
(define (couchdb/range view start end (opts #f))
  (let* ((options (or opts {}))
	 (viewopts (couchview-options view))
	 (include_docs
	  (if (test options 'include_docs) (get options 'include_docs)
	      (and (test viewopts 'include_docs) (get viewopts 'include_docs))))
	 (response
	  (couchdb/req
	   (couchview-db view)
	   (scripturl (couchview-path view)
	     "startkey" (->json start) "endkey" (->json end)
	     "group" (if (test options 'group)
			 (tryif (get options 'group) "true")
			 (tryif (and (test viewopts 'group) (get viewopts 'group)) "true"))
	     "group_level" (try (get options 'group_level) (get viewopts 'group_level))
	     "include_docs" (tryif include_docs "true")
	     "descending" (if (test options 'descending)
			      (tryif (get options 'descending) "true")
			      (tryif (and (test viewopts 'descending)
					  (get viewopts 'descending))
				"true"))
	     "skip" (get options 'skip) "limit" (get options 'limit))))
	 (results {}))
    (doseq (row (get response 'rows))
      (set+! results (if include_docs (get row 'doc) (get row 'value))))
    results))
(module-export! '{couchdb/table couchdb/list couchdb/range})
