;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2016 beingmeta, inc. All rights reserved

(in-module 'couchdb)

(use-module '{fdweb ezrecords extoids jsonout})
(use-module '{texttools logger})

(define-init %loglevel %notice%)

(define-init couchdbs (make-hashtable))

(defrecord couchdb url (map #[]) (cache #f)
  (views (make-hashtable))
  (curlopts #[]) 
  (conns {}))
(defrecord couchview db design view path (options {}))

(define (couchdb url (map #f) (cache #f))
  (if map
      (let ((probe (get couchdbs url)))
	(when (and cache (exists? probe)
		   (not (eq? (couchdb-cache probe) cache)))
	  (logwarn "Too late to define cache for " probe))
	(if  (exists? probe)
	     (if (eq? map (couchdb-map probe)) probe
		 (begin (couchdb/add-map! probe map)
			probe))
	     (new-couchdb url map cache)))
      (try (get couchdbs url) (new-couchdb url #f cache))))
(define (couchdef! def db)
  (store! couchdbs def
	  (if (string? db) (couchdb db)
	      (if (couchdb? db) db
		  (error "Not a valid DB" db)))))
(module-export!
 '{couchdb
   couchdb? couchdb-url couchdb-map couchdb-cache
   couchdb-views couchdef!})
(module-export! '{couchview-db couchview-options})

(defslambda (new-couchdb url (map #f) (cache #f))
  (if (exists? (get couchdbs url)) ;; Cover race condition 
      (if (not map) (get couchdbs url)
	  (let ((probe (get couchdbs url)))
	    (unless (eq? map (couchdb-map probe))
	      (couchdb/add-map! probe map))
	    (when (and cache (not (eq? (couchdb-cache probe) cache)))
	      (logwarn "Too late to define cache for " probe))
	    probe))
       (let ((new (cons-couchdb url (or map #[])
				(and cache
				     (if (hashtable? cache) cache
					 (make-hashtable))))))
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
	 (resp (begin (debug%watch "COUCHDB/REQ" path call options)
		      (urlget call (or options (couchdb-curlopts db))))))
    (debug%watch "COUCHDB/REQ" resp)
    (tryif (and (test resp 'response) (>= (get resp 'response) 200)
		(< (get resp 'response) 300))
      (jsonparse (get resp '%content) 56 (couchdb-map db)))))

(define (docget db id opts)
  (couchdb/req db (if (oid? id)
		      (stringout "@" (number->string (oid-addr id) 16))
		      (if (uuid? id) (uuid->string id)
			  (if (string? id) (uriencode id)
			      (stringout
				":" (uriencode (lisp->string id))))))
	       opts))
(define (viewget db id opts)
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
      (set+! results
	     (couchoid (if include_docs
			   (get row 'doc)
			   (get row 'value)))))
    results))

(define (couchdb/get db id (opts #f) (usecache #t))
  (if (couchdb? db)
      (if (and usecache (couchdb-cache db))
	  (or (try (get (couchdb-cache db) id)
		   (let ((value (docget db id opts)))
		     (store! (couchdb-cache db) id (try value #f))
		     value))
	      {})
	  (docget db id opts))
      (if (couchview? db)
	  (viewget db id opts)
	  (error "Invalid arg" db))))

(defambda (couchdb/getn db ids (opts #f) (tbl #f))
  (if (couchdb? db)
      (docgetn db (qc ids) opts tbl)
      (if (couchview? db)
	  (viewgetn db (qc ids) opts tbl)
	  (error "Not a couch DB" db))))

(define (docgetn db ids opts tbl)
  (let* ((url (scripturl (mkpath (couchdb-url db) "_all_docs")
		"include_docs=true"))
	 (data (stringout "{\"keys\": ["
			  (do-choices (id ids i)
			    (if (> i 0) (printout ", "))
			    (if (oid? id)
				(printout
				  "\"@" (number->string (oid-addr id) 16)
				  "\"")
				(jsonout id)))
			  "]}"))
	 (response (urlpost url data))
	 (body (jsonparse (get response '%content) 56 (couchdb-map db)))
	 (results (if tbl
		      (if (hashtable? tbl) tbl (make-hashtable))
		      {})))
    (if tbl
	(doseq (row (get body 'rows))
	  (store! results (get row 'id) (get row 'doc)))
	(doseq (row (get body 'rows))
	  (set+! results (get row 'doc))))
    results))

(define (viewgetn view ids opts tbl)
  (let* ((db (couchview-db view))
	 (options (or opts {}))
	 (viewopts (couchview-options view))
	 (include_docs
	  (if (test options 'include_docs) (get options 'include_docs)
	      (and (test viewopts 'include_docs) (get viewopts 'include_docs))))
	 (url (scripturl (mkpath (couchdb-url db) (couchview-path view))
		"include_docs" (tryif include_docs "true")
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
		"skip" (get options 'skip) "limit" (get options 'limit)))
	 (data (stringout "{\"keys\": ["
			  (do-choices (id ids i)
			    (if (> i 0) (printout ", "))
			    (if (oid? id)
				(printout
				  "\"@" (number->string (oid-addr id) 16)
				  "\"")
				(jsonout id)))
			  "]}"))
	 (response (urlpost url data))
	 (body (jsonparse (get response '%content) 56 (couchdb-map db)))
	 (results (if tbl
		      (if (hashtable? tbl) tbl (make-hashtable))
		      {})))
    (if tbl
	(doseq (row (get body 'rows))
	  (add! results (get row 'key)
		(if include_docs (get row 'doc)
		    (get row 'value))))
	(doseq (row (get body 'rows))
	  (set+! results
		 (if include_docs (get row 'doc)
		     (get row 'value)))))
    results))

(defambda (couchdb/prefetch! db ids (opts #f))
  (when (couchdb-cache db)
    (couchdb/getn db (reject ids (couchdb-cache db))
		  opts (couchdb-cache db))))

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
  (let ((success #f) (cur (couchdb/get db id #f #f)))
    (until success
      (mutate cur)
      (set! success (couchdb/save! db cur))
      (unless success (set! cur (couchdb/get db id #f #f))))
    (when (couchdb-cache db) (drop! (couchdb-cache db) id))))
  
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
   couchdb/get couchdb/getn couchdb/prefetch!
   couchdb/save! couchdb/mutate! couchdb/delete!
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
  (let ((success #f) (cur (try (couchdb/get db id #f #f) (frame-create #f))))
    (until success
      (mutate cur)
      (set! success (couchdb/save! db cur))
      (unless success (set! cur (couchdb/get db id #f #f)))))
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

(define (couchoid x)
  (if (oid? x) x
      (if (string? x)
	  (if (or (has-prefix x "@") (has-prefix x ":@"))
	      (onerror (parse-arg x) (lambda (ex) x))
	      x)
	  (if (and (table? x) (exists has-prefix (get x '_id) "@")
		   (not (test x '%nocache)))
	      (onerror
	       (let ((oid (parse-arg (get x '_id))))
		 (if (oid? oid)
		     (let ((pool (getpool oid)))
		       (when (exists? pool) (extpool-cache! pool oid x))
		       oid)
		     x))
	       (lambda (ex) x))
	      x))))

(module-export! '{cdb/get cdb/mutate! cdb/store! cdb/add! cdb/drop couchoid})

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
	 (raw (or (test options 'raw) (test viewopts 'raw)))
	 (response
	  (couchdb/req
	   (couchview-db view)
	   (scripturl (couchview-path view)
	     "key" (->json (get options 'key))
	     "startkey" (->json (get options 'start))
	     "endkey" (->json (get options 'end))
	     "group" (if (test options 'group)
			 (tryif (get options 'group) "true")
			 (tryif (and (test viewopts 'group)
				     (get viewopts 'group)) "true"))
	     "group_level" (try (get options 'group_level)
				(get viewopts 'group_level))
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
      (add! table (get row 'key)
	    (if raw
		(if include_docs (get row 'doc) (get row 'value))
		(couchoid (if include_docs (get row 'doc) (get row 'value))))))
    table))
(define (couchdb/list view (opts #f))
  (let* ((options (or opts {}))
	 (viewopts (couchview-options view))
	 (include_docs
	  (if (test options 'include_docs) (get options 'include_docs)
	      (and (test viewopts 'include_docs) (get viewopts 'include_docs))))
	 (raw (or (test options 'raw) (test viewopts 'raw)))
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
      (set+! results (if raw
			 (if include_docs (get row 'doc) (get row 'value))
			 (couchoid (if include_docs (get row 'doc) (get row 'value))))))
    results))
(define (couchdb/range view start end (opts #f))
  (let* ((options (or opts {}))
	 (viewopts (couchview-options view))
	 (include_docs
	  (if (test options 'include_docs) (get options 'include_docs)
	      (and (test viewopts 'include_docs) (get viewopts 'include_docs))))
	 (raw (or (test options 'raw) (test viewopts 'raw)))
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
      (set+! results
	     (if raw
		 (if include_docs (get row 'doc) (get row 'value))
		 (couchoid (if include_docs (get row 'doc) (get row 'value))))))
    results))
(module-export! '{couchdb/table couchdb/list couchdb/range})
